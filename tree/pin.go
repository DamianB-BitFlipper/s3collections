package tree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/damianb/s3collections/s3backend"
)

type recentPinWire struct {
	V         int       `json:"v"`
	NodeID    NodeID    `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Store) publishRecentPin(ctx context.Context, id NodeID) error {
	w := recentPinWire{V: 1, NodeID: id, ExpiresAt: s.opts.Now().Add(s.opts.GCGrace)}
	body, _ := json.Marshal(w)
	err := s.runBackend(ctx, "pin_commit", func() error { _, e := s.backend.Put(ctx, s.recentPinKey(id), body, nil); return e })
	if err == nil {
		return nil
	}
	obj, e := s.backend.Get(ctx, s.recentPinKey(id))
	if e == nil && string(obj.Body) == string(body) {
		return nil
	}
	return err
}
func (s *Store) recentRoots(ctx context.Context) ([]NodeID, error) {
	prefix := s.prefix + "gc/recent/"
	now := s.opts.Now()
	var out []NodeID
	err := s.eachObject(ctx, prefix, func(info s3backend.ObjectInfo) error {
		base := strings.TrimPrefix(info.Key, prefix)
		if !strings.HasSuffix(base, ".json") {
			return nil
		}
		id := NodeID(strings.TrimSuffix(base, ".json"))
		if validateNodeID(id) != nil {
			return &CorruptError{Key: info.Key, Reason: "invalid recent-pin id"}
		}
		obj, e := s.backend.Get(ctx, info.Key)
		if errors.Is(e, s3backend.ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		var w recentPinWire
		if e = decodeCanonical(obj.Body, &w); e != nil || w.V != 1 || w.NodeID != id {
			return &CorruptError{Key: info.Key, Reason: "invalid recent pin"}
		}
		if w.ExpiresAt.Add(s.opts.ClockSkewHint).After(now) {
			out = append(out, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return validatedUniqueIDs(out)
}

type recentBlobPinWire struct {
	V         int       `json:"v"`
	BlobID    BlobID    `json:"blob_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Store) publishRecentBlobPin(ctx context.Context, id BlobID) error {
	w := recentBlobPinWire{V: 1, BlobID: id, ExpiresAt: s.opts.Now().Add(s.opts.GCGrace)}
	body, _ := json.Marshal(w)
	return s.runBackend(ctx, "pin_blob", func() error { _, e := s.backend.Put(ctx, s.recentBlobPinKey(id), body, nil); return e })
}
func (s *Store) recentBlobs(ctx context.Context, protectSince time.Time) ([]BlobID, error) {
	prefix := s.prefix + "gc/recent-blobs/"
	now := s.opts.Now()
	set := map[BlobID]struct{}{}
	err := s.eachObject(ctx, prefix, func(info s3backend.ObjectInfo) error {
		base := strings.TrimPrefix(info.Key, prefix)
		if !strings.HasSuffix(base, ".json") {
			return nil
		}
		id := BlobID(strings.TrimSuffix(base, ".json"))
		if validateBlobID(id) != nil {
			return &CorruptError{Key: info.Key, Reason: "invalid recent blob pin"}
		}
		obj, e := s.backend.Get(ctx, info.Key)
		if errors.Is(e, s3backend.ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		var w recentBlobPinWire
		if decodeCanonical(obj.Body, &w) != nil || w.V != 1 || w.BlobID != id {
			return &CorruptError{Key: info.Key, Reason: "invalid recent blob pin"}
		}
		if w.ExpiresAt.Add(s.opts.ClockSkewHint).After(now) || (!protectSince.IsZero() && !info.ModTime.Add(s.opts.ClockSkewHint).Before(protectSince)) {
			set[id] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]BlobID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out, nil
}
