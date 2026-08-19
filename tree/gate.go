package tree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

type gateWire struct {
	V         int       `json:"v"`
	Held      bool      `json:"held"`
	Owner     string    `json:"owner"`
	Token     string    `json:"token"`
	Fence     uint64    `json:"fence"`
	ExpiresAt time.Time `json:"expires_at"`
}
type mutationGate struct {
	wire gateWire
	etag string
}

// acquireMutationGate serializes all liveness publication/removal with a GC
// sweep. A live gate cannot be stolen before its expiry plus skew margin.
func (s *Store) acquireMutationGate(ctx context.Context, owner string) (mutationGate, error) {
	if owner == "" {
		owner = "operation"
	}
	token, err := randomHex(32)
	if err != nil {
		return mutationGate{}, err
	}
	next := s3collections.BackoffDelays(s.opts.Retry, nil)
	for attempts := 0; ; attempts++ {
		if err = ctx.Err(); err != nil {
			return mutationGate{}, err
		}
		obj, gerr := s.backend.Get(ctx, s.gateKey())
		now := s.opts.Now()
		if errors.Is(gerr, s3backend.ErrNotFound) {
			w := gateWire{V: 1, Held: true, Owner: owner, Token: token, Fence: 1, ExpiresAt: now.Add(s.opts.MutationGateTTL)}
			body, _ := json.Marshal(w)
			etag, perr := s.backend.Put(ctx, s.gateKey(), body, &s3backend.Preconditions{IfNoneMatch: true})
			if perr == nil {
				return mutationGate{w, etag}, nil
			}
			if cur, applied, _ := s.reconcileGateWrite(w); applied {
				return mutationGate{w, cur.ETag}, nil
			}
			if !errors.Is(perr, s3backend.ErrPreconditionFailed) && !s3backend.IsRetryable(perr) {
				return mutationGate{}, perr
			}
		} else if gerr != nil {
			if !s3backend.IsRetryable(gerr) {
				return mutationGate{}, gerr
			}
		} else {
			var old gateWire
			if e := decodeCanonical(obj.Body, &old); e != nil || old.V != 1 || old.Fence == 0 {
				return mutationGate{}, &CorruptError{Key: s.gateKey(), Reason: "invalid coordination gate"}
			}
			// Fail closed: a held gate is never stolen automatically, even after its
			// advisory expiry. Otherwise a delayed unconditional Delete or liveness
			// PUT from the prior holder could apply after takeover. Operators may
			// inspect and explicitly clear a gate left by a crashed process.
			available := !old.Held
			if available {
				if old.Fence == math.MaxUint64 {
					return mutationGate{}, ErrConflict
				}
				w := gateWire{V: 1, Held: true, Owner: owner, Token: token, Fence: old.Fence + 1, ExpiresAt: now.Add(s.opts.MutationGateTTL)}
				body, _ := json.Marshal(w)
				etag, perr := s.backend.Put(ctx, s.gateKey(), body, &s3backend.Preconditions{IfMatchETag: obj.ETag})
				if perr == nil {
					return mutationGate{w, etag}, nil
				}
				if cur, applied, _ := s.reconcileGateWrite(w); applied {
					return mutationGate{w, cur.ETag}, nil
				}
				if !errors.Is(perr, s3backend.ErrPreconditionFailed) && !s3backend.IsRetryable(perr) {
					return mutationGate{}, perr
				}
			}
		}
		if attempts+1 >= s.opts.Retry.MaxAttempts {
			return mutationGate{}, fmt.Errorf("%w: coordination gate busy", ErrConflict)
		}
		timer := time.NewTimer(next())
		select {
		case <-ctx.Done():
			timer.Stop()
			return mutationGate{}, ctx.Err()
		case <-timer.C:
		}
	}
}
func (s *Store) reconcileGateWrite(w gateWire) (*s3backend.Object, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	delay := s3collections.BackoffDelays(s.opts.Retry, nil)
	for attempt := 1; attempt <= s.opts.Retry.MaxAttempts; attempt++ {
		obj, err := s.backend.Get(ctx, s.gateKey())
		if err == nil {
			var got gateWire
			if decodeCanonical(obj.Body, &got) != nil {
				return nil, false, ErrCorrupt
			}
			return obj, equalJSON(got, w), nil
		}
		if !s3backend.IsRetryable(err) || attempt == s.opts.Retry.MaxAttempts {
			return nil, false, err
		}
		timer := time.NewTimer(delay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, false, ErrLeaseLost
}
func (s *Store) gateWriteApplied(_ context.Context, w gateWire) bool {
	_, ok, _ := s.reconcileGateWrite(w)
	return ok
}

func (s *Store) renewMutationGate(ctx context.Context, g *mutationGate) error {
	obj, err := s.backend.Get(ctx, s.gateKey())
	if err != nil {
		return ErrLeaseLost
	}
	var cur gateWire
	if decodeCanonical(obj.Body, &cur) != nil {
		return ErrCorrupt
	}
	if !cur.Held || cur.Token != g.wire.Token || cur.Fence != g.wire.Fence || cur.Owner != g.wire.Owner || cur.ExpiresAt.Before(s.opts.Now()) {
		return ErrLeaseLost
	}
	if cur.Fence == math.MaxUint64 {
		return ErrLeaseLost
	}
	next := cur
	next.Fence++
	next.ExpiresAt = s.opts.Now().Add(s.opts.MutationGateTTL)
	body, _ := json.Marshal(next)
	etag, err := s.backend.Put(ctx, s.gateKey(), body, &s3backend.Preconditions{IfMatchETag: obj.ETag})
	if err != nil {
		if appliedObj, applied, _ := s.reconcileGateWrite(next); applied {
			g.wire, g.etag = next, appliedObj.ETag
			return nil
		}
		return ErrLeaseLost
	}
	g.wire, g.etag = next, etag
	return nil
}
func (s *Store) releaseMutationGate(_ context.Context, g mutationGate) error {
	// Cleanup must outlive a caller cancellation after the protected mutation
	// has already committed, otherwise one canceled request could brick the
	// store-wide gate forever.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	delay := s3collections.BackoffDelays(s.opts.Retry, nil)
	for attempt := 1; attempt <= s.opts.Retry.MaxAttempts; attempt++ {
		obj, err := s.backend.Get(ctx, s.gateKey())
		if err == nil {
			var cur gateWire
			if decodeCanonical(obj.Body, &cur) != nil {
				return ErrCorrupt
			}
			// Read-back reconciliation for an ambiguous earlier release.
			if !cur.Held && cur.Token == g.wire.Token && cur.Fence == g.wire.Fence+1 {
				return nil
			}
			if !cur.Held || cur.Token != g.wire.Token || cur.Fence != g.wire.Fence {
				return ErrLeaseLost
			}
			if cur.Fence == math.MaxUint64 {
				return ErrLeaseLost
			}
			next := cur
			next.Held = false
			next.Fence++
			next.ExpiresAt = s.opts.Now()
			body, _ := json.Marshal(next)
			_, err = s.backend.Put(ctx, s.gateKey(), body, &s3backend.Preconditions{IfMatchETag: obj.ETag})
			if err == nil || s.gateWriteApplied(ctx, next) {
				return nil
			}
			if errors.Is(err, s3backend.ErrPreconditionFailed) {
				return ErrLeaseLost
			}
		}
		if err != nil && !s3backend.IsRetryable(err) {
			return err
		}
		if attempt == s.opts.Retry.MaxAttempts {
			return err
		}
		timer := time.NewTimer(delay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrLeaseLost
}

func (s *Store) withMutationGate(ctx context.Context, owner string, fn func(*mutationGate) error) error {
	g, err := s.acquireMutationGate(ctx, owner)
	if err != nil {
		return err
	}
	opErr := fn(&g)
	relErr := s.releaseMutationGate(ctx, g)
	if relErr != nil {
		s.opts.Logger.Warn(relErr, "tree: coordination gate release failed")
		if opErr == nil {
			return relErr
		}
	}
	return opErr
}

// MutationGateInfo is operational state for explicit crash recovery.
type MutationGateInfo struct {
	Held      bool
	Owner     string
	Fence     uint64
	ExpiresAt time.Time
}

func (s *Store) MutationGate(ctx context.Context) (MutationGateInfo, error) {
	o, err := s.backend.Get(ctx, s.gateKey())
	if errors.Is(err, s3backend.ErrNotFound) {
		return MutationGateInfo{}, nil
	}
	if err != nil {
		return MutationGateInfo{}, err
	}
	var w gateWire
	if decodeCanonical(o.Body, &w) != nil || w.V != 1 || w.Fence == 0 {
		return MutationGateInfo{}, ErrCorrupt
	}
	return MutationGateInfo{Held: w.Held, Owner: w.Owner, Fence: w.Fence, ExpiresAt: w.ExpiresAt}, nil
}

// RecoverMutationGate releases a gate left by a process known to be dead.
// The expected fence prevents recovery against a changed holder. Callers must
// provide external operator exclusivity; automatic TTL stealing is unsafe
// because a delayed unconditional S3 delete may still complete.
func (s *Store) RecoverMutationGate(ctx context.Context, expectedFence uint64) error {
	o, err := s.backend.Get(ctx, s.gateKey())
	if err != nil {
		return err
	}
	var cur gateWire
	if decodeCanonical(o.Body, &cur) != nil || cur.V != 1 || cur.Fence == 0 {
		return ErrCorrupt
	}
	if !cur.Held {
		return nil
	}
	if cur.Fence != expectedFence || cur.Fence == math.MaxUint64 {
		return ErrConflict
	}
	next := cur
	next.Held = false
	next.Fence++
	next.ExpiresAt = s.opts.Now()
	body, _ := json.Marshal(next)
	_, err = s.backend.Put(ctx, s.gateKey(), body, &s3backend.Preconditions{IfMatchETag: o.ETag})
	if errors.Is(err, s3backend.ErrPreconditionFailed) {
		return ErrConflict
	}
	if err != nil && s.gateWriteApplied(ctx, next) {
		return nil
	}
	return err
}
