package tree

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

type leaseWire struct {
	V              int       `json:"v"`
	ID             string    `json:"id"`
	NodeID         NodeID    `json:"node_id"`
	Owner          string    `json:"owner"`
	Token          string    `json:"token"`
	Fence          uint64    `json:"fence"`
	TTLNanoseconds int64     `json:"ttl_nanoseconds"`
	ExpiresAt      time.Time `json:"expires_at"`
	Released       bool      `json:"released"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func leaseFromWire(w leaseWire, etag string) Lease {
	return Lease{ID: w.ID, NodeID: w.NodeID, Owner: w.Owner, Token: w.Token, Fence: w.Fence, TTL: time.Duration(w.TTLNanoseconds), ExpiresAt: w.ExpiresAt, ETag: etag}
}
func validLeaseID(v string) bool {
	if len(v) != 32 {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil && strings.ToLower(v) == v
}
func (s *Store) getLeaseWire(ctx context.Context, id string) (leaseWire, string, error) {
	if !validLeaseID(id) {
		return leaseWire{}, "", ErrInvalidLease
	}
	var obj *s3backend.Object
	err := s.runBackend(ctx, "get_lease", func() error { var e error; obj, e = s.backend.Get(ctx, s.leaseKey(id)); return e })
	if errors.Is(err, s3backend.ErrNotFound) {
		return leaseWire{}, "", ErrNotFound
	}
	if err != nil {
		return leaseWire{}, "", err
	}
	var w leaseWire
	if err = decodeCanonical(obj.Body, &w); err != nil {
		return leaseWire{}, "", &CorruptError{Key: s.leaseKey(id), Reason: err.Error()}
	}
	if w.V != 1 || w.ID != id || w.Owner == "" || w.Token == "" || w.Fence == 0 || w.TTLNanoseconds <= 0 || validateNodeID(w.NodeID) != nil || w.CreatedAt.IsZero() || w.UpdatedAt.Before(w.CreatedAt) {
		return leaseWire{}, "", &CorruptError{Key: s.leaseKey(id), Reason: "invalid lease fields"}
	}
	return w, obj.ETag, nil
}

func (s *Store) AcquireLease(ctx context.Context, nodeID NodeID, owner string, ttl time.Duration) (out Lease, err error) {
	if validateName(owner) != nil || ttl <= 0 {
		return Lease{}, ErrInvalidLease
	}
	err = s.withMutationGate(ctx, "acquire_lease", func(g *mutationGate) error {
		if _, e := s.verifyNodeForLiveness(ctx, nodeID); e != nil {
			return e
		}
		for collision := 0; collision < 4; collision++ {
			id, e := randomHex(16)
			if e != nil {
				return e
			}
			token, e := randomHex(32)
			if e != nil {
				return e
			}
			now := s.opts.Now()
			w := leaseWire{V: 1, ID: id, NodeID: nodeID, Owner: owner, Token: token, Fence: 1, TTLNanoseconds: int64(ttl), ExpiresAt: now.Add(ttl), CreatedAt: now, UpdatedAt: now}
			body, _ := json.Marshal(w)
			if e := s.renewMutationGate(ctx, g); e != nil {
				return e
			}
			etag, e := s.writeLeaseConditional(ctx, w, body, &s3backend.Preconditions{IfNoneMatch: true})
			if errors.Is(e, s3backend.ErrPreconditionFailed) {
				continue
			}
			if e != nil {
				return e
			}
			out = leaseFromWire(w, etag)
			return nil
		}
		return ErrConflict
	})
	return out, err
}

// RenewLease advances the fence. With no ttl argument it reuses the lease's
// acquired TTL; an optional positive override becomes the new remembered TTL.
func (s *Store) RenewLease(ctx context.Context, lease Lease, ttlOverride ...time.Duration) (out Lease, err error) {
	ttl := lease.TTL
	if len(ttlOverride) > 1 {
		return Lease{}, ErrInvalidLease
	}
	if len(ttlOverride) == 1 {
		ttl = ttlOverride[0]
	}
	if ttl <= 0 {
		return Lease{}, ErrInvalidLease
	}
	err = s.withMutationGate(ctx, "renew_lease", func(g *mutationGate) error {
		old, etag, e := s.getLeaseWire(ctx, lease.ID)
		if e != nil {
			return e
		}
		now := s.opts.Now()
		if old.Released || old.Token != lease.Token || old.Owner != lease.Owner || old.NodeID != lease.NodeID || old.Fence != lease.Fence || !old.ExpiresAt.After(now) || old.Fence == math.MaxUint64 {
			return ErrLeaseLost
		}
		next := old
		next.Fence++
		next.TTLNanoseconds = int64(ttl)
		next.ExpiresAt = now.Add(ttl)
		next.UpdatedAt = maxTime(old.UpdatedAt, now)
		body, _ := json.Marshal(next)
		if e := s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		newETag, e := s.writeLeaseConditional(ctx, next, body, &s3backend.Preconditions{IfMatchETag: etag})
		if errors.Is(e, s3backend.ErrPreconditionFailed) {
			return ErrLeaseLost
		}
		if e != nil {
			return e
		}
		out = leaseFromWire(next, newETag)
		return nil
	})
	return out, err
}
func (s *Store) ReleaseLease(ctx context.Context, lease Lease) (err error) {
	if !validLeaseID(lease.ID) || lease.Fence == 0 || lease.Token == "" {
		return ErrInvalidLease
	}
	return s.withMutationGate(ctx, "release_lease", func(g *mutationGate) error {
		old, etag, e := s.getLeaseWire(ctx, lease.ID)
		if e != nil {
			return e
		}
		if old.Released {
			if old.Token == lease.Token && old.Fence == lease.Fence+1 {
				return nil
			}
			return ErrLeaseLost
		}
		if old.Token != lease.Token || old.Owner != lease.Owner || old.NodeID != lease.NodeID || old.Fence != lease.Fence || old.Fence == math.MaxUint64 {
			return ErrLeaseLost
		}
		next := old
		next.Released = true
		next.Fence++
		next.ExpiresAt = s.opts.Now()
		next.UpdatedAt = maxTime(old.UpdatedAt, s.opts.Now())
		body, _ := json.Marshal(next)
		if e := s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		_, e = s.writeLeaseConditional(ctx, next, body, &s3backend.Preconditions{IfMatchETag: etag})
		if errors.Is(e, s3backend.ErrPreconditionFailed) {
			return ErrLeaseLost
		}
		return e
	})
}

func (s *Store) writeLeaseConditional(ctx context.Context, w leaseWire, body []byte, pre *s3backend.Preconditions) (string, error) {
	p := s.opts.Retry
	delay := s3collections.BackoffDelays(p, nil)
	ambiguous := false
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		etag, err := s.backend.Put(ctx, s.leaseKey(w.ID), body, pre)
		if err == nil {
			return etag, nil
		}
		if errors.Is(err, s3backend.ErrPreconditionFailed) {
			if ambiguous {
				if got, gotETag, e := s.getLeaseWire(ctx, w.ID); e == nil && equalJSON(got, w) {
					return gotETag, nil
				}
			}
			return "", err
		}
		if !s3backend.IsRetryable(err) {
			return "", err
		}
		// A retryable write may have been applied. Reconcile even when this
		// policy allows only one attempt.
		ambiguous = true
		if got, gotETag, e := s.getLeaseWire(ctx, w.ID); e == nil && equalJSON(got, w) {
			return gotETag, nil
		}
		if attempt == p.MaxAttempts {
			return "", err
		}
		timer := time.NewTimer(delay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", ErrConflict
}
func (s *Store) activeLeases(ctx context.Context) ([]Lease, error) {
	prefix := s.prefix + "lease/"
	token := ""
	now := s.opts.Now()
	var out []Lease
	for {
		page, err := s.backend.List(ctx, prefix, &s3backend.ListOptions{ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, info := range page.Objects {
			base := strings.TrimPrefix(info.Key, prefix)
			if !strings.HasSuffix(base, ".json") {
				continue
			}
			id := strings.TrimSuffix(base, ".json")
			w, etag, e := s.getLeaseWire(ctx, id)
			if e != nil {
				return nil, e
			}
			if !w.Released && w.ExpiresAt.Add(s.opts.ClockSkewHint).After(now) {
				out = append(out, leaseFromWire(w, etag))
			}
		}
		if !page.IsTruncated {
			break
		}
		if page.NextContinuationToken == "" || page.NextContinuationToken == token {
			return nil, &CorruptError{Key: prefix, Reason: "invalid continuation token"}
		}
		token = page.NextContinuationToken
	}
	return out, nil
}
