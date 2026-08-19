package tree

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

type refWire struct {
	V         int       `json:"v"`
	Name      string    `json:"name"`
	NodeID    *NodeID   `json:"node_id"`
	Revision  uint64    `json:"revision"`
	Tombstone bool      `json:"tombstone"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func refFromWire(w refWire, etag string) Ref {
	return Ref{Name: w.Name, NodeID: *w.NodeID, Revision: w.Revision, ETag: etag}
}
func (s *Store) GetRef(ctx context.Context, name string) (Ref, error) {
	if err := validateRefName(name); err != nil {
		return Ref{}, err
	}
	w, etag, err := s.getRefWire(ctx, name)
	if err != nil {
		return Ref{}, err
	}
	if w.Tombstone {
		return Ref{}, ErrNotFound
	}
	return refFromWire(w, etag), nil
}
func (s *Store) getRefWire(ctx context.Context, name string) (refWire, string, error) {
	var obj *s3backend.Object
	err := s.runBackend(ctx, "get_ref", func() error { var e error; obj, e = s.backend.Get(ctx, s.refKey(name)); return e })
	if errors.Is(err, s3backend.ErrNotFound) {
		return refWire{}, "", ErrNotFound
	}
	if err != nil {
		return refWire{}, "", err
	}
	var w refWire
	if err = decodeCanonical(obj.Body, &w); err != nil {
		return refWire{}, "", &CorruptError{Key: s.refKey(name), Reason: err.Error()}
	}
	if w.V != 1 || w.Name != name || w.Revision == 0 || w.CreatedAt.IsZero() || w.UpdatedAt.Before(w.CreatedAt) {
		return refWire{}, "", &CorruptError{Key: s.refKey(name), Reason: "invalid ref fields"}
	}
	if w.Tombstone {
		if w.NodeID != nil {
			return refWire{}, "", &CorruptError{Key: s.refKey(name), Reason: "tombstone has node"}
		}
	} else if w.NodeID == nil || validateNodeID(*w.NodeID) != nil {
		return refWire{}, "", &CorruptError{Key: s.refKey(name), Reason: "invalid node id"}
	}
	return w, obj.ETag, nil
}

func (s *Store) CreateRef(ctx context.Context, name string, nodeID NodeID) (out Ref, err error) {
	if err = validateRefName(name); err != nil {
		return Ref{}, err
	}
	err = s.withMutationGate(ctx, "create_ref", func(g *mutationGate) error {
		if _, e := s.verifyNodeForLiveness(ctx, nodeID); e != nil {
			return e
		}
		now := s.opts.Now()
		id := nodeID
		candidate := refWire{V: 1, Name: name, NodeID: &id, Revision: 1, CreatedAt: now, UpdatedAt: now}
		body, _ := json.Marshal(candidate)
		if e := s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		etag, e := s.writeRefConditional(ctx, name, candidate, body, &s3backend.Preconditions{IfNoneMatch: true})
		if e == nil {
			out = refFromWire(candidate, etag)
			return nil
		}
		if errors.Is(e, s3backend.ErrPreconditionFailed) {
			return ErrAlreadyExists
		}
		return e
	})
	return out, err
}
func (s *Store) CompareAndSwapRef(ctx context.Context, name string, expected uint64, nodeID NodeID) (out Ref, err error) {
	if err = validateRefName(name); err != nil {
		return Ref{}, err
	}
	err = s.withMutationGate(ctx, "cas_ref", func(g *mutationGate) error {
		if _, e := s.verifyNodeForLiveness(ctx, nodeID); e != nil {
			return e
		}
		old, etag, e := s.getRefWire(ctx, name)
		if e != nil {
			return e
		}
		if old.Tombstone || old.Revision != expected || expected == math.MaxUint64 {
			return ErrConflict
		}
		id := nodeID
		next := refWire{V: 1, Name: name, NodeID: &id, Revision: expected + 1, CreatedAt: old.CreatedAt, UpdatedAt: maxTime(old.UpdatedAt, s.opts.Now())}
		body, _ := json.Marshal(next)
		if e := s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		newETag, e := s.writeRefConditional(ctx, name, next, body, &s3backend.Preconditions{IfMatchETag: etag})
		if errors.Is(e, s3backend.ErrPreconditionFailed) {
			return ErrConflict
		}
		if e != nil {
			return e
		}
		out = refFromWire(next, newETag)
		return nil
	})
	return out, err
}
func (s *Store) DeleteRef(ctx context.Context, name string, expected uint64) (err error) {
	if err = validateRefName(name); err != nil {
		return err
	}
	return s.withMutationGate(ctx, "delete_ref", func(g *mutationGate) error {
		old, etag, e := s.getRefWire(ctx, name)
		if e != nil {
			return e
		}
		if old.Tombstone {
			if old.Revision == expected+1 {
				return nil
			}
			return ErrConflict
		}
		if old.Revision != expected || expected == math.MaxUint64 {
			return ErrConflict
		}
		next := refWire{V: 1, Name: name, Revision: expected + 1, Tombstone: true, CreatedAt: old.CreatedAt, UpdatedAt: maxTime(old.UpdatedAt, s.opts.Now())}
		body, _ := json.Marshal(next)
		if e := s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		_, e = s.writeRefConditional(ctx, name, next, body, &s3backend.Preconditions{IfMatchETag: etag})
		if errors.Is(e, s3backend.ErrPreconditionFailed) {
			return ErrConflict
		}
		return e
	})
}

func (s *Store) writeRefConditional(ctx context.Context, name string, w refWire, body []byte, pre *s3backend.Preconditions) (string, error) {
	p := s.opts.Retry
	delay := s3collections.BackoffDelays(p, nil)
	ambiguous := false
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		etag, err := s.backend.Put(ctx, s.refKey(name), body, pre)
		if err == nil {
			return etag, nil
		}
		if errors.Is(err, s3backend.ErrPreconditionFailed) {
			if ambiguous {
				if got, gotETag, e := s.getRefWire(ctx, name); e == nil && equalJSON(got, w) {
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
		if got, gotETag, e := s.getRefWire(ctx, name); e == nil && equalJSON(got, w) {
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
func (s *Store) listRefs(ctx context.Context) ([]Ref, error) {
	prefix := s.prefix + "r/"
	token := ""
	var out []Ref
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
			raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSuffix(base, ".json"))
			if err != nil {
				return nil, &CorruptError{Key: info.Key, Reason: "invalid encoded ref name"}
			}
			name := string(raw)
			w, etag, e := s.getRefWire(ctx, name)
			if e != nil {
				return nil, fmt.Errorf("tree: ref %q: %w", name, e)
			}
			if !w.Tombstone {
				out = append(out, refFromWire(w, etag))
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
