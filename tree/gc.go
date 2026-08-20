package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/damianb/s3collections/s3backend"
)

func (s *Store) PlanGC(ctx context.Context, cutoff time.Time, options ...GCPlanOption) (GCPlan, error) {
	o := GCPlanOptions{Cutoff: cutoff}
	for _, option := range options {
		if option != nil {
			option(&o)
		}
	}
	return s.PlanGCWithOptions(ctx, o)
}

func (s *Store) PlanGCWithOptions(ctx context.Context, o GCPlanOptions) (GCPlan, error) {
	now := s.opts.Now()
	if o.Cutoff.IsZero() || o.Cutoff.After(now) {
		return GCPlan{}, errors.New("tree: invalid GC cutoff")
	}
	grace := o.Grace
	if grace == 0 {
		grace = s.opts.GCGrace
	}
	if grace < s.opts.GCGrace {
		return GCPlan{}, errors.New("tree: GC grace is below store safety minimum")
	}
	pins := append([]NodeID(nil), o.Roots...)
	for _, r := range o.Refs {
		pins = append(pins, r.NodeID)
	}
	for _, l := range o.ActiveLeases {
		if l.ExpiresAt.Add(s.opts.ClockSkewHint).After(now) {
			pins = append(pins, l.NodeID)
		}
	}
	pins, err := validatedUniqueIDs(pins)
	if err != nil {
		return GCPlan{}, err
	}
	// Nodes newer than the cutoff are temporary roots: although not candidates
	// themselves, their older ancestors and payloads must remain reachable.
	fresh, err := s.freshNodeRoots(ctx, o.Cutoff, s.opts.ClockSkewHint)
	if err != nil {
		return GCPlan{}, err
	}
	pins = append(pins, fresh...)
	pins, err = validatedUniqueIDs(pins)
	if err != nil {
		return GCPlan{}, err
	}
	roots, err := s.currentRoots(ctx, pins)
	if err != nil {
		return GCPlan{}, err
	}
	liveN, liveB, err := s.markReachable(ctx, roots)
	if err != nil {
		return GCPlan{}, err
	}
	recentBlobIDs, err := s.recentBlobs(ctx, time.Time{})
	if err != nil {
		return GCPlan{}, err
	}
	for _, id := range recentBlobIDs {
		liveB[id] = struct{}{}
	}
	nodes, err := s.collectNodeCandidates(ctx, o.Cutoff, liveN)
	if err != nil {
		return GCPlan{}, err
	}
	blobs, err := s.collectBlobCandidates(ctx, o.Cutoff, liveB)
	if err != nil {
		return GCPlan{}, err
	}
	edges, err := s.collectEdgeCandidates(ctx, o.Cutoff, liveN)
	if err != nil {
		return GCPlan{}, err
	}
	id, err := randomHex(16)
	if err != nil {
		return GCPlan{}, err
	}
	plan := GCPlan{ID: id, CreatedAt: now, NotBefore: now.Add(grace), Cutoff: o.Cutoff, ClockSkewHint: s.opts.ClockSkewHint, PinnedRoots: pins, Nodes: nodes, Blobs: blobs, Edges: edges}
	body, err := json.Marshal(plan)
	if err != nil {
		return GCPlan{}, err
	}
	if err = s.putImmutable(ctx, "persist_gc_plan", s.gcPlanKey(id), body); err != nil {
		return GCPlan{}, err
	}
	return plan, nil
}

// SweepGC acquires the store-wide liveness mutation gate. Ref changes and
// lease acquire/renew/release use the same gate, so after fresh reachability
// and object-version validation no liveness publication can race an
// unconditional S3 Delete. The gate is renewed before every candidate.
func (s *Store) SweepGC(ctx context.Context, plan GCPlan) (out SweepResult, err error) {
	if !validLeaseID(plan.ID) || plan.Cutoff.IsZero() || plan.CreatedAt.IsZero() || plan.Cutoff.After(plan.CreatedAt) || plan.NotBefore.Before(plan.CreatedAt) {
		return out, ErrInvalidGCPlan
	}
	var storedPlan *s3backend.Object
	readErr := s.runBackend(ctx, "get_gc_plan", func() error { var e error; storedPlan, e = s.backend.Get(ctx, s.gcPlanKey(plan.ID)); return e })
	if errors.Is(readErr, s3backend.ErrNotFound) {
		return out, ErrInvalidGCPlan
	}
	if readErr != nil {
		return out, readErr
	}
	body, marshalErr := json.Marshal(plan)
	if marshalErr != nil || !bytes.Equal(body, storedPlan.Body) {
		return out, ErrInvalidGCPlan
	}
	if s.opts.Now().Before(plan.NotBefore) {
		return out, ErrPlanNotReady
	}
	pins, e := validatedUniqueIDs(plan.PinnedRoots)
	if e != nil {
		return out, ErrInvalidGCPlan
	}
	if e = s.validatePlanKeys(plan); e != nil {
		return out, e
	}
	gate, e := s.acquireMutationGate(ctx, "gc_sweep")
	if e != nil {
		return out, e
	}
	defer func() {
		if releaseErr := s.releaseMutationGate(ctx, gate); releaseErr != nil {
			s.opts.Logger.Warn(releaseErr, "tree: GC gate release failed")
		}
	}()
	// Leases active when the sweep obtains the gate remain roots throughout
	// this sweep, even if their wall-clock expiry passes while renewals wait.
	active, e := s.activeLeases(ctx)
	if e != nil {
		return out, e
	}
	for _, l := range active {
		pins = append(pins, l.NodeID)
	}
	recent, e := s.recentRoots(ctx)
	if e != nil {
		return out, e
	}
	pins = append(pins, recent...)
	young, e := s.freshNodeRoots(ctx, plan.Cutoff, plan.ClockSkewHint)
	if e != nil {
		return out, e
	}
	pins = append(pins, young...)
	pins, _ = validatedUniqueIDs(pins)
	fresh := func() (map[NodeID]struct{}, map[BlobID]struct{}, error) {
		roots, e := s.currentRoots(ctx, pins)
		if e != nil {
			return nil, nil, e
		}
		ln, lb, e := s.markReachable(ctx, roots)
		if e != nil {
			return nil, nil, e
		}
		recent, e := s.recentBlobs(ctx, plan.CreatedAt)
		if e != nil {
			return nil, nil, e
		}
		for _, id := range recent {
			lb[id] = struct{}{}
		}
		return ln, lb, nil
	}
	liveNodes, liveBlobs, e := fresh()
	if e != nil {
		return out, e
	}
	nodeCandidates := append([]GCNodeCandidate(nil), plan.Nodes...)
	sort.Slice(nodeCandidates, func(i, j int) bool {
		if nodeCandidates[i].Depth == nodeCandidates[j].Depth {
			return nodeCandidates[i].ID < nodeCandidates[j].ID
		}
		return nodeCandidates[i].Depth > nodeCandidates[j].Depth
	})
	for _, c := range nodeCandidates {
		if e = s.renewMutationGate(ctx, &gate); e != nil {
			return out, e
		}
		if _, ok := liveNodes[c.ID]; ok {
			continue
		}
		same, e := s.versionUnchanged(ctx, c.Object, plan.Cutoff, plan.ClockSkewHint)
		if e != nil {
			return out, e
		}
		if !same {
			continue
		}
		if e = s.deleteKey(ctx, c.Object.Key); e != nil {
			return out, e
		}
		out.NodesDeleted++
	}
	for _, c := range plan.Blobs {
		if e = s.renewMutationGate(ctx, &gate); e != nil {
			return out, e
		}
		if _, ok := liveBlobs[c.ID]; ok {
			continue
		}
		exact, e := s.blobCandidateUnchanged(ctx, c, plan.Cutoff, plan.ClockSkewHint)
		if e != nil {
			return out, e
		}
		if !exact {
			continue
		}
		// The manifest is the visibility point. With liveness mutation blocked,
		// removing it then raw data cannot invalidate a newly published ref.
		if c.Manifest != nil {
			if e = s.deleteKey(ctx, c.Manifest.Key); e != nil {
				return out, e
			}
		}
		if c.Data != nil {
			if e = s.renewMutationGate(ctx, &gate); e != nil {
				return out, e
			}
			if e = s.deleteKey(ctx, c.Data.Key); e != nil {
				return out, e
			}
		}
		out.BlobsDeleted++
	}
	for _, c := range plan.Edges {
		if e = s.renewMutationGate(ctx, &gate); e != nil {
			return out, e
		}
		ln, _, e := fresh()
		if e != nil {
			return out, e
		}
		if _, ok := ln[c.Child]; ok {
			continue
		}
		same, e := s.versionUnchanged(ctx, c.Object, plan.Cutoff, plan.ClockSkewHint)
		if e != nil {
			return out, e
		}
		if !same {
			continue
		}
		if e = s.deleteKey(ctx, c.Object.Key); e != nil {
			return out, e
		}
		out.EdgesDeleted++
	}
	if deleteErr := s.deleteKey(ctx, s.gcPlanKey(plan.ID)); deleteErr != nil {
		s.opts.Logger.Warn(deleteErr, "tree: completed GC plan cleanup failed")
	}
	return out, nil
}

func validatedUniqueIDs(in []NodeID) ([]NodeID, error) {
	set := map[NodeID]struct{}{}
	for _, id := range in {
		if err := validateNodeID(id); err != nil {
			return nil, err
		}
		set[id] = struct{}{}
	}
	out := make([]NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}
func (s *Store) currentRoots(ctx context.Context, pinned []NodeID) ([]NodeID, error) {
	refs, err := s.listRefs(ctx)
	if err != nil {
		return nil, err
	}
	leases, err := s.activeLeases(ctx)
	if err != nil {
		return nil, err
	}
	recent, err := s.recentRoots(ctx)
	if err != nil {
		return nil, err
	}
	ids := append([]NodeID(nil), pinned...)
	ids = append(ids, recent...)
	for _, r := range refs {
		ids = append(ids, r.NodeID)
	}
	for _, l := range leases {
		ids = append(ids, l.NodeID)
	}
	return validatedUniqueIDs(ids)
}
func (s *Store) markReachable(ctx context.Context, roots []NodeID) (map[NodeID]struct{}, map[BlobID]struct{}, error) {
	nodes := map[NodeID]struct{}{}
	blobs := map[BlobID]struct{}{}
	for _, root := range roots {
		line, err := s.ResolveLineage(ctx, root, nil)
		if err != nil {
			return nil, nil, err
		}
		for _, n := range line {
			nodes[n.ID] = struct{}{}
			for _, ref := range n.Payloads {
				stored, e := s.StatBlob(ctx, ref)
				if e != nil {
					return nil, nil, e
				}
				if !equalJSON(stored, normalizeRef(ref)) {
					return nil, nil, ErrCorrupt
				}
				blobs[ref.Hash] = struct{}{}
			}
		}
	}
	return nodes, blobs, nil
}
func oldEnough(info s3backend.ObjectInfo, cutoff time.Time, skew time.Duration) bool {
	return !info.ModTime.IsZero() && !info.ModTime.Add(skew).After(cutoff)
}
func versionOf(i s3backend.ObjectInfo) GCObjectVersion {
	return GCObjectVersion{Key: i.Key, ETag: i.ETag, Size: i.Size, ModTime: i.ModTime}
}
func (s *Store) freshNodeRoots(ctx context.Context, cutoff time.Time, skew time.Duration) ([]NodeID, error) {
	prefix := s.prefix + "n/"
	var out []NodeID
	err := s.eachObject(ctx, prefix, func(i s3backend.ObjectInfo) error {
		if i.ModTime.IsZero() || i.ModTime.Add(skew).After(cutoff) {
			if id, ok := s.nodeIDFromKey(i.Key); ok {
				out = append(out, id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return validatedUniqueIDs(out)
}
func (s *Store) collectNodeCandidates(ctx context.Context, cutoff time.Time, live map[NodeID]struct{}) ([]GCNodeCandidate, error) {
	prefix := s.prefix + "n/"
	var out []GCNodeCandidate
	err := s.eachObject(ctx, prefix, func(i s3backend.ObjectInfo) error {
		id, ok := s.nodeIDFromKey(i.Key)
		if !ok {
			return nil
		}
		if _, yes := live[id]; yes || !oldEnough(i, cutoff, s.opts.ClockSkewHint) {
			return nil
		}
		n, err := s.GetNode(ctx, id)
		if err != nil {
			return err
		}
		out = append(out, GCNodeCandidate{ID: id, Depth: n.Depth, Object: versionOf(i)})
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Depth == out[j].Depth {
			return out[i].ID < out[j].ID
		}
		return out[i].Depth > out[j].Depth
	})
	return out, err
}
func (s *Store) collectBlobCandidates(ctx context.Context, cutoff time.Time, live map[BlobID]struct{}) ([]GCBlobCandidate, error) {
	prefix := s.prefix + "b/"
	groups := map[BlobID]*GCBlobCandidate{}
	err := s.eachObject(ctx, prefix, func(i s3backend.ObjectInfo) error {
		id, kind, ok := s.blobIDFromKey(i.Key)
		if !ok {
			return nil
		}
		g := groups[id]
		if g == nil {
			g = &GCBlobCandidate{ID: id}
			groups[id] = g
		}
		v := versionOf(i)
		if kind == "data" {
			g.Data = &v
		} else {
			g.Manifest = &v
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var out []GCBlobCandidate
	for id, g := range groups {
		if _, yes := live[id]; yes {
			continue
		}
		if g.Data != nil && !oldVersionEnough(*g.Data, cutoff, s.opts.ClockSkewHint) {
			continue
		}
		if g.Manifest != nil && !oldVersionEnough(*g.Manifest, cutoff, s.opts.ClockSkewHint) {
			continue
		}
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func oldVersionEnough(v GCObjectVersion, cutoff time.Time, skew time.Duration) bool {
	return !v.ModTime.IsZero() && !v.ModTime.Add(skew).After(cutoff)
}
func (s *Store) collectEdgeCandidates(ctx context.Context, cutoff time.Time, live map[NodeID]struct{}) ([]GCEdgeCandidate, error) {
	prefix := s.prefix + "edges/"
	var out []GCEdgeCandidate
	err := s.eachObject(ctx, prefix, func(i s3backend.ObjectInfo) error {
		p, c, ok := s.edgeIDsFromKey(i.Key)
		if !ok {
			return nil
		}
		if _, yes := live[c]; yes || !oldEnough(i, cutoff, s.opts.ClockSkewHint) {
			return nil
		}
		out = append(out, GCEdgeCandidate{Parent: p, Child: c, Object: versionOf(i)})
		return nil
	})
	return out, err
}
func (s *Store) eachObject(ctx context.Context, prefix string, fn func(s3backend.ObjectInfo) error) error {
	token := ""
	for {
		var page *s3backend.ListPage
		err := s.runBackend(ctx, "list_gc", func() error {
			var e error
			page, e = s.backend.List(ctx, prefix, &s3backend.ListOptions{ContinuationToken: token})
			return e
		})
		if err != nil {
			return err
		}
		for _, info := range page.Objects {
			if err = fn(info); err != nil {
				return err
			}
		}
		if !page.IsTruncated {
			return nil
		}
		if page.NextContinuationToken == "" || page.NextContinuationToken == token {
			return &CorruptError{Key: prefix, Reason: "invalid continuation token"}
		}
		token = page.NextContinuationToken
	}
}
func (s *Store) nodeIDFromKey(key string) (NodeID, bool) {
	rel := strings.TrimPrefix(key, s.prefix+"n/")
	parts := strings.Split(rel, "/")
	if len(parts) != 2 || len(parts[0]) != 2 || !strings.HasSuffix(parts[1], ".json") {
		return "", false
	}
	id := NodeID(strings.TrimSuffix(parts[1], ".json"))
	return id, validateNodeID(id) == nil && parts[0] == string(id)[:2]
}
func (s *Store) blobIDFromKey(key string) (BlobID, string, bool) {
	rel := strings.TrimPrefix(key, s.prefix+"b/")
	parts := strings.Split(rel, "/")
	if len(parts) != 3 || (parts[2] != "data" && parts[2] != "manifest.json") {
		return "", "", false
	}
	id := BlobID(parts[1])
	return id, parts[2], validateBlobID(id) == nil && parts[0] == string(id)[:2]
}
func (s *Store) edgeIDsFromKey(key string) (NodeID, NodeID, bool) {
	rel := strings.TrimPrefix(key, s.prefix+"edges/")
	parts := strings.Split(rel, "/")
	if len(parts) != 2 {
		return "", "", false
	}
	p, c := NodeID(parts[0]), NodeID(parts[1])
	return p, c, validateNodeID(p) == nil && validateNodeID(c) == nil
}
func (s *Store) validatePlanKeys(plan GCPlan) error {
	eligible := func(v GCObjectVersion) bool { return oldVersionEnough(v, plan.Cutoff, plan.ClockSkewHint) }
	for _, c := range plan.Nodes {
		if validateNodeID(c.ID) != nil || c.Object.Key != s.nodeKey(c.ID) || !eligible(c.Object) {
			return ErrInvalidGCPlan
		}
	}
	for _, c := range plan.Blobs {
		if validateBlobID(c.ID) != nil {
			return ErrInvalidGCPlan
		}
		if c.Data != nil && (c.Data.Key != s.blobDataKey(c.ID) || !eligible(*c.Data)) {
			return ErrInvalidGCPlan
		}
		if c.Manifest != nil && (c.Manifest.Key != s.blobMetaKey(c.ID) || !eligible(*c.Manifest)) {
			return ErrInvalidGCPlan
		}
		if c.Data == nil && c.Manifest == nil {
			return ErrInvalidGCPlan
		}
	}
	for _, c := range plan.Edges {
		if validateNodeID(c.Parent) != nil || validateNodeID(c.Child) != nil || c.Object.Key != s.edgeKey(c.Parent, c.Child) || !eligible(c.Object) {
			return ErrInvalidGCPlan
		}
	}
	return nil
}
func (s *Store) blobCandidateUnchanged(ctx context.Context, c GCBlobCandidate, cutoff time.Time, skew time.Duration) (bool, error) {
	checks := []struct {
		want *GCObjectVersion
		key  string
	}{{c.Data, s.blobDataKey(c.ID)}, {c.Manifest, s.blobMetaKey(c.ID)}}
	for _, x := range checks {
		got, err := s.statObject(ctx, x.key)
		if errors.Is(err, s3backend.ErrNotFound) {
			if x.want != nil {
				return false, nil
			}
			continue
		}
		if err != nil {
			return false, err
		}
		if x.want == nil {
			return false, nil
		}
		if got.ModTime.IsZero() || got.ModTime.Add(skew).After(cutoff) {
			return false, nil
		}
		same := got.Size == x.want.Size
		if x.want.ETag != "" {
			same = same && got.ETag == x.want.ETag
		} else {
			same = same && got.ModTime.Equal(x.want.ModTime)
		}
		if !same {
			return false, nil
		}
	}
	return true, nil
}
func (s *Store) versionUnchanged(ctx context.Context, want GCObjectVersion, cutoff time.Time, skew time.Duration) (bool, error) {
	got, err := s.statObject(ctx, want.Key)
	if errors.Is(err, s3backend.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if got.ModTime.IsZero() || got.ModTime.Add(skew).After(cutoff) {
		return false, nil
	}
	same := got.Size == want.Size
	if want.ETag != "" {
		same = same && got.ETag == want.ETag
	} else {
		same = same && got.ModTime.Equal(want.ModTime)
	}
	return same, nil
}
func (s *Store) deleteKey(ctx context.Context, key string) error {
	return s.runBackend(ctx, "sweep_gc", func() error { return s.backend.Delete(ctx, key) })
}

// GetGCPlan reloads a persisted GC plan by ID. It validates the stored object
// matches the expected plan structure. Returns ErrNotFound if no plan exists.
func (s *Store) GetGCPlan(ctx context.Context, planID string) (GCPlan, error) {
	if !validLeaseID(planID) {
		return GCPlan{}, ErrInvalidGCPlan
	}
	var obj *s3backend.Object
	err := s.runBackend(ctx, "get_gc_plan", func() error {
		var e error
		obj, e = s.backend.Get(ctx, s.gcPlanKey(planID))
		return e
	})
	if errors.Is(err, s3backend.ErrNotFound) {
		return GCPlan{}, ErrNotFound
	}
	if err != nil {
		return GCPlan{}, err
	}
	var plan GCPlan
	if err = json.Unmarshal(obj.Body, &plan); err != nil {
		return GCPlan{}, &CorruptError{Key: s.gcPlanKey(planID), Reason: err.Error()}
	}
	if plan.ID != planID {
		return GCPlan{}, &CorruptError{Key: s.gcPlanKey(planID), Reason: "plan ID mismatch"}
	}
	return plan, nil
}

// ListGCPlans returns all persisted GC plans in the store. Plans that fail
// to decode are skipped. This allows queue maintenance to discover and sweep
// due plans without separate marker objects.
func (s *Store) ListGCPlans(ctx context.Context) ([]GCPlan, error) {
	prefix := s.prefix + "gc/plans/"
	var out []GCPlan
	err := s.eachObject(ctx, prefix, func(info s3backend.ObjectInfo) error {
		base := strings.TrimSuffix(strings.TrimPrefix(info.Key, prefix), ".json")
		if !validLeaseID(base) {
			return nil
		}
		obj, e := s.backend.Get(ctx, info.Key)
		if errors.Is(e, s3backend.ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		var plan GCPlan
		if json.Unmarshal(obj.Body, &plan) != nil || plan.ID != base {
			return nil // skip corrupt
		}
		out = append(out, plan)
		return nil
	})
	return out, err
}
