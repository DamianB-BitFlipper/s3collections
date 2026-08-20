package tree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

func newTestStore(t *testing.T, b s3backend.Backend, now *time.Time, extra ...Option) *Store {
	t.Helper()
	opts := []Option{WithBlobSizeRange(1, 500<<20), WithMultipartThreshold(100 << 20), WithGCGrace(time.Hour), WithClockSkewHint(0), WithMutationGateTTL(time.Hour), WithRetry(s3collections.RetryPolicy{MaxAttempts: 3, Base: time.Microsecond, Max: time.Millisecond, Jitter: -1}), WithClock(func() time.Time { return *now })}
	opts = append(opts, extra...)
	s, err := New(b, "test/tree", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func digest(data []byte) BlobID { h := sha256.Sum256(data); return BlobID(hex.EncodeToString(h[:])) }
func putBlob(t *testing.T, ctx context.Context, s *Store, data []byte, opts ...BlobPutOption) BlobRef {
	t.Helper()
	ref, err := s.PutBlob(ctx, digest(data), bytes.NewReader(data), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestBlobRawSeparateMetadataRangeAndIntegrity(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	data := bytes.Repeat([]byte("raw-data-"), 6000)
	ref := putBlob(t, ctx, s, data, WithExpectedBlobSize(int64(len(data))), WithEncodingDescriptor([]byte{0, 0xff, 1}), WithEncryptionDescriptor([]byte("opaque:kms:v1")), WithBlobMetadata([]byte{3, 2, 1, 0}))
	raw, err := m.Get(ctx, s.blobDataKey(ref.Hash))
	if err != nil || !bytes.Equal(raw.Body, data) {
		t.Fatalf("raw data: %v", err)
	}
	meta, err := m.Get(ctx, s.blobMetaKey(ref.Hash))
	if err != nil || bytes.Equal(meta.Body, data) {
		t.Fatalf("sidecar: %v", err)
	}
	st, err := s.StatBlob(ctx, ref)
	if err != nil || !equalJSON(st, ref) {
		t.Fatalf("stat=%#v err=%v", st, err)
	}
	r, err := s.GetBlob(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get len=%d err=%v", len(got), err)
	}
	r, err = s.GetBlob(ctx, ref, WithRange(7, 19))
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, data[7:26]) {
		t.Fatalf("range=%q err=%v", got, err)
	}
	again, err := s.PutBlob(ctx, ref.Hash, bytes.NewReader(data), WithEncodingDescriptor(ref.Encoding), WithEncryptionDescriptor(ref.Encryption), WithBlobMetadata(ref.Metadata))
	if err != nil || !equalJSON(again, ref) {
		t.Fatalf("idempotent=%#v %v", again, err)
	}
	first, err := s.PutBlob(ctx, ref.Hash, bytes.NewReader(data), WithBlobMetadata([]byte("different")))
	if err != nil || !equalJSON(first, ref) {
		t.Fatalf("first-writer metadata=%#v err=%v", first, err)
	}
	bad := BlobID(strings.Repeat("0", 64))
	if bad == ref.Hash {
		bad = BlobID(strings.Repeat("1", 64))
	}
	before := m.Len()
	if _, err = s.PutBlob(ctx, bad, bytes.NewReader(data)); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("hash mismatch=%v", err)
	}
	if m.Len() != before {
		t.Fatal("hash mismatch published objects")
	}
	if _, err = m.Put(ctx, s.blobDataKey(ref.Hash), []byte("corrupt"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StatBlob(ctx, ref); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt stat=%v", err)
	}
}

type capabilityBackend struct {
	base                                   *s3backend.Memory
	mu                                     sync.Mutex
	stream, multipart, gets, stats, ranges int
}

func (b *capabilityBackend) Get(c context.Context, k string) (*s3backend.Object, error) {
	b.mu.Lock()
	b.gets++
	b.mu.Unlock()
	return b.base.Get(c, k)
}
func (b *capabilityBackend) Put(c context.Context, k string, v []byte, p *s3backend.Preconditions) (string, error) {
	return b.base.Put(c, k, v, p)
}
func (b *capabilityBackend) Delete(c context.Context, k string) error { return b.base.Delete(c, k) }
func (b *capabilityBackend) List(c context.Context, p string, o *s3backend.ListOptions) (*s3backend.ListPage, error) {
	return b.base.List(c, p, o)
}
func (b *capabilityBackend) GetStream(c context.Context, k string) (*s3backend.StreamObject, error) {
	return b.base.GetStream(c, k)
}
func (b *capabilityBackend) PutStream(c context.Context, k string, r io.Reader, n int64, p *s3backend.Preconditions) error {
	b.mu.Lock()
	b.stream++
	b.mu.Unlock()
	return b.base.PutStream(c, k, r, n, p)
}
func (b *capabilityBackend) GetRange(c context.Context, k string, o, n int64) (*s3backend.StreamObject, error) {
	b.mu.Lock()
	b.ranges++
	b.mu.Unlock()
	return b.base.GetRange(c, k, o, n)
}
func (b *capabilityBackend) Stat(c context.Context, k string) (*s3backend.StatObject, error) {
	b.mu.Lock()
	b.stats++
	b.mu.Unlock()
	return b.base.Stat(c, k)
}
func (b *capabilityBackend) PutMultipart(c context.Context, k string, r io.Reader, n int64, p *s3backend.Preconditions) error {
	b.mu.Lock()
	b.multipart++
	b.mu.Unlock()
	if p != nil {
		return errors.New("portable multipart must be unconditional")
	}
	return b.base.PutStream(c, k, r, n, nil)
}

type streamOnlyBackend struct {
	base   *s3backend.Memory
	stream int
}

func (b *streamOnlyBackend) Get(c context.Context, k string) (*s3backend.Object, error) {
	return b.base.Get(c, k)
}
func (b *streamOnlyBackend) Put(c context.Context, k string, v []byte, p *s3backend.Preconditions) (string, error) {
	return b.base.Put(c, k, v, p)
}
func (b *streamOnlyBackend) Delete(c context.Context, k string) error { return b.base.Delete(c, k) }
func (b *streamOnlyBackend) List(c context.Context, p string, o *s3backend.ListOptions) (*s3backend.ListPage, error) {
	return b.base.List(c, p, o)
}
func (b *streamOnlyBackend) GetStream(c context.Context, k string) (*s3backend.StreamObject, error) {
	return b.base.GetStream(c, k)
}
func (b *streamOnlyBackend) PutStream(c context.Context, k string, r io.Reader, n int64, p *s3backend.Preconditions) error {
	b.stream++
	return b.base.PutStream(c, k, r, n, p)
}
func (b *streamOnlyBackend) GetRange(c context.Context, k string, o, n int64) (*s3backend.StreamObject, error) {
	return b.base.GetRange(c, k, o, n)
}
func (b *streamOnlyBackend) Stat(c context.Context, k string) (*s3backend.StatObject, error) {
	return b.base.Stat(c, k)
}

func TestBlobMultipartThresholdAndStreamFallback(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	cap := &capabilityBackend{base: s3backend.NewMemory()}
	s := newTestStore(t, cap, &now, WithMultipartThreshold(16))
	putBlob(t, ctx, s, bytes.Repeat([]byte("x"), 16))
	if cap.multipart != 1 || cap.stream != 0 {
		t.Fatalf("multipart=%d stream=%d", cap.multipart, cap.stream)
	}
	cap.mu.Lock()
	cap.gets, cap.stats, cap.ranges = 0, 0, 0
	cap.mu.Unlock()
	ref, _ := s.StatBlobID(ctx, digest(bytes.Repeat([]byte("x"), 16)))
	cap.mu.Lock()
	cap.gets, cap.stats, cap.ranges = 0, 0, 0
	cap.mu.Unlock()
	r, err := s.GetBlob(ctx, ref, WithRange(0, 4))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(r)
	_ = r.Close()
	if cap.ranges != 1 || cap.gets != 0 || cap.stats != 0 {
		t.Fatalf("hot range calls: range=%d get=%d stat=%d", cap.ranges, cap.gets, cap.stats)
	}
	putBlob(t, ctx, s, bytes.Repeat([]byte("y"), 15))
	if cap.multipart != 1 || cap.stream != 1 {
		t.Fatalf("multipart=%d stream=%d", cap.multipart, cap.stream)
	}
	fallback := &streamOnlyBackend{base: s3backend.NewMemory()}
	s2 := newTestStore(t, fallback, &now, WithMultipartThreshold(8))
	putBlob(t, ctx, s2, bytes.Repeat([]byte("z"), 32))
	if fallback.stream != 1 {
		t.Fatalf("fallback stream=%d", fallback.stream)
	}
}

func TestNodesRefsTopologyAndOpaqueMetadata(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	blob := putBlob(t, ctx, s, []byte("payload"))
	opaque := []byte{0xff, 0, 1, 2, 0x80}
	root, err := s.CommitRoot(ctx, []BlobRef{blob}, opaque)
	if err != nil {
		t.Fatal(err)
	}
	rootAgain, err := s.CommitRoot(ctx, []BlobRef{blob}, opaque)
	if err != nil || rootAgain != root {
		t.Fatalf("determinism %s %v", rootAgain, err)
	}
	obj, _ := m.Get(ctx, s.nodeKey(root))
	if !bytes.Contains(obj.Body, []byte(`"parent":null,"root":null`)) {
		t.Fatalf("root wire=%s", obj.Body)
	}
	left, err := s.CommitChild(ctx, root, []BlobRef{blob}, []byte("left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := s.CommitChild(ctx, root, nil, []byte("right"))
	if err != nil {
		t.Fatal(err)
	}
	grand, err := s.CommitChild(ctx, left, nil, []byte("grand"))
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.GetNode(ctx, root)
	if err != nil || !bytes.Equal(n.OpaqueMetadata, opaque) || n.ParentID != nil || n.RootID != nil {
		t.Fatalf("root=%#v err=%v", n, err)
	}
	rid, err := s.Root(ctx, grand)
	if err != nil || rid != root {
		t.Fatalf("Root=%s %v", rid, err)
	}
	line, err := s.ResolveLineage(ctx, grand, nil)
	if err != nil || len(line) != 3 || line[0].ID != root || line[2].ID != grand {
		t.Fatalf("line=%v err=%v", line, err)
	}
	short, err := s.ResolveLineage(ctx, grand, &LineageOptions{Stop: func(n Node) bool { return bytes.Equal(n.OpaqueMetadata, []byte("left")) }})
	if err != nil || len(short) != 2 || short[0].ID != left {
		t.Fatalf("short=%v %v", short, err)
	}
	children, err := s.ListChildren(ctx, root)
	if err != nil || len(children) != 2 || !((children[0] == left && children[1] == right) || (children[0] == right && children[1] == left)) {
		t.Fatalf("children=%v %v", children, err)
	}
	yes, err := s.IsAncestor(ctx, root, grand)
	if err != nil || !yes {
		t.Fatalf("ancestor=%v %v", yes, err)
	}
	lca, err := s.LCA(ctx, grand, right)
	if err != nil || lca != root {
		t.Fatalf("lca=%s %v", lca, err)
	}
	other, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.LCA(ctx, grand, other); !errors.Is(err, ErrNoCommonAncestor) {
		t.Fatalf("other lca=%v", err)
	}
	ref, err := s.CreateRef(ctx, "branches/main", left)
	if err != nil || ref.Revision != 1 {
		t.Fatalf("create=%#v %v", ref, err)
	}
	ref, err = s.CompareAndSwapRef(ctx, "branches/main", 1, grand)
	if err != nil || ref.Revision != 2 {
		t.Fatalf("cas=%#v %v", ref, err)
	}
	if _, err = s.CompareAndSwapRef(ctx, "branches/main", 1, right); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale=%v", err)
	}
	if err = s.DeleteRef(ctx, "branches/main", 2); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetRef(ctx, "branches/main"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tombstone=%v", err)
	}
	if _, err = s.CreateRef(ctx, "branches/main", right); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("tombstone create=%v", err)
	}
}

func TestLeaseFencesStaleCopies(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	s := newTestStore(t, s3backend.NewMemory(), &now)
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, badErr := s.AcquireLease(ctx, root, string([]byte{0xff}), time.Hour); !errors.Is(badErr, ErrInvalidLease) {
		t.Fatalf("invalid owner=%v", badErr)
	}
	lease, err := s.AcquireLease(ctx, root, "worker", time.Hour)
	if err != nil || lease.Fence != 1 {
		t.Fatalf("acquire=%#v %v", lease, err)
	}
	renewed, err := s.RenewLease(ctx, lease)
	if err != nil || renewed.Fence != 2 {
		t.Fatalf("renew=%#v %v", renewed, err)
	}
	if _, err = s.RenewLease(ctx, lease); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale renew=%v", err)
	}
	if err = s.ReleaseLease(ctx, lease); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale release=%v", err)
	}
	if err = s.ReleaseLease(ctx, renewed); err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseLease(ctx, renewed); err != nil {
		t.Fatalf("idempotent release=%v", err)
	}
}

type edgeFailBackend struct {
	*s3backend.Memory
	fail bool
}

func (b *edgeFailBackend) Put(ctx context.Context, key string, body []byte, pre *s3backend.Preconditions) (string, error) {
	if b.fail && strings.Contains(key, "/edges/") {
		b.fail = false
		return "", &s3backend.Error{Op: "Put", Key: key, StatusCode: 503, Code: "SlowDown", Message: "injected", Retryable: true}
	}
	return b.Memory.Put(ctx, key, body, pre)
}
func TestCommitChildReturnsIDWhenEdgePending(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	b := &edgeFailBackend{Memory: s3backend.NewMemory()}
	s := newTestStore(t, b, &now, WithRetry(s3collections.RetryPolicy{MaxAttempts: 1}))
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.fail = true
	id, err := s.CommitChild(ctx, root, nil, nil)
	if err == nil || id == "" {
		t.Fatalf("id=%s err=%v", id, err)
	}
	repaired, err := s.RepairEdges(ctx)
	if err != nil || repaired != 1 {
		t.Fatalf("repair=%d err=%v", repaired, err)
	}
	kids, err := s.ListChildren(ctx, root)
	if err != nil || len(kids) != 1 || kids[0] != id {
		t.Fatalf("kids=%v %v", kids, err)
	}
}

func TestGCGraceRevalidationAndOrphanData(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	liveBlob := putBlob(t, ctx, s, []byte("live"))
	live, _ := s.CommitRoot(ctx, []BlobRef{liveBlob}, nil)
	if _, err := s.CreateRef(ctx, "main", live); err != nil {
		t.Fatal(err)
	}
	keepBlob := putBlob(t, ctx, s, []byte("keep-later"))
	keep, _ := s.CommitRoot(ctx, []BlobRef{keepBlob}, nil)
	deadBlob := putBlob(t, ctx, s, []byte("delete-me"))
	dead, _ := s.CommitRoot(ctx, []BlobRef{deadBlob}, nil)
	orphanData := []byte("abandoned upload")
	orphanID := digest(orphanData)
	if _, err := m.Put(ctx, s.blobDataKey(orphanID), orphanData, nil); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	plan, err := s.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SweepGC(ctx, plan); !errors.Is(err, ErrPlanNotReady) {
		t.Fatalf("early=%v", err)
	}
	if _, err = s.CreateRef(ctx, "late", keep); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	result, err := s.SweepGC(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.NodesDeleted != 1 || result.BlobsDeleted != 2 {
		t.Fatalf("result=%#v plan=%#v", result, plan)
	}
	if _, err = s.GetNode(ctx, dead); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dead=%v", err)
	}
	if _, err = s.GetNode(ctx, keep); err != nil {
		t.Fatalf("late live=%v", err)
	}
	if _, err = m.Get(ctx, s.blobDataKey(orphanID)); !errors.Is(err, s3backend.ErrNotFound) {
		t.Fatalf("orphan raw=%v", err)
	}
	if _, err = s.StatBlob(ctx, liveBlob); err != nil {
		t.Fatalf("live blob=%v", err)
	}
}

func TestMutationGateBlocksRefPublication(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	s := newTestStore(t, s3backend.NewMemory(), &now, WithRetry(s3collections.RetryPolicy{MaxAttempts: 1}))
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.acquireMutationGate(ctx, "test_sweep")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateRef(ctx, "blocked", root); !errors.Is(err, ErrConflict) {
		t.Fatalf("mutation under sweep gate=%v", err)
	}
	if err = s.releaseMutationGate(ctx, g); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateRef(ctx, "blocked", root); err != nil {
		t.Fatal(err)
	}
}

func TestRefListingUsesOpaquePaginationTokens(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	s := newTestStore(t, m, &now)
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1005; i++ {
		name := strings.Repeat("x", 4) + "/" + hex.EncodeToString([]byte{byte(i >> 8), byte(i)})
		id := root
		w := refWire{V: 1, Name: name, NodeID: &id, Revision: 1, CreatedAt: now, UpdatedAt: now}
		body, _ := json.Marshal(w)
		if _, err = m.Put(ctx, s.refKey(name), body, nil); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := s.listRefs(ctx)
	if err != nil || len(refs) != 1005 {
		t.Fatalf("refs=%d err=%v", len(refs), err)
	}
}

func TestGCYoungDescendantPinsOldAncestors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	blob := putBlob(t, ctx, s, []byte("old payload"))
	root, err := s.CommitRoot(ctx, []BlobRef{blob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	child, err := s.CommitChild(ctx, root, nil, []byte("young orphan"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range plan.Nodes {
		if c.ID == root {
			t.Fatal("old ancestor of young node planned for deletion")
		}
	}
	for _, c := range plan.Blobs {
		if c.ID == blob.Hash {
			t.Fatal("old ancestor payload planned for deletion")
		}
	}
	if _, err = s.GetNode(ctx, child); err != nil {
		t.Fatal(err)
	}
}

func TestGCPostPlanYoungChildProtectsOldParentAndBlob(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	blob := putBlob(t, ctx, s, []byte("old-parent-payload"))
	root, err := s.CommitRoot(ctx, []BlobRef{blob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	cutoff := now.Add(-time.Hour)
	plan, err := s.PlanGC(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes) != 1 || plan.Nodes[0].ID != root {
		t.Fatalf("pre-child plan=%#v", plan.Nodes)
	}
	child, err := s.CommitChild(ctx, root, nil, []byte("created after plan"))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	result, err := s.SweepGC(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.NodesDeleted != 0 || result.BlobsDeleted != 0 {
		t.Fatalf("swept protected closure: %#v", result)
	}
	if _, err = s.GetNode(ctx, root); err != nil {
		t.Fatalf("parent deleted: %v", err)
	}
	if _, err = s.GetNode(ctx, child); err != nil {
		t.Fatalf("child missing: %v", err)
	}
	if _, err = s.StatBlob(ctx, blob); err != nil {
		t.Fatalf("payload deleted: %v", err)
	}
}

func TestGCPlansDescendantsBeforeAncestors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	root, _ := s.CommitRoot(ctx, nil, nil)
	child, _ := s.CommitChild(ctx, root, nil, nil)
	grand, _ := s.CommitChild(ctx, child, nil, nil)
	now = now.Add(2 * time.Hour)
	plan, err := s.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes) != 3 || plan.Nodes[0].ID != grand || plan.Nodes[1].ID != child || plan.Nodes[2].ID != root {
		t.Fatalf("order=%#v", plan.Nodes)
	}
}

func TestGCFabricatedAgeCannotDeleteYoungOrphanData(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(10_000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	data := []byte("young orphan")
	id := digest(data)
	etag, err := m.Put(ctx, s.blobDataKey(id), data, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan := GCPlan{ID: "forged", CreatedAt: now.Add(-2 * time.Hour), NotBefore: now.Add(-time.Hour), Cutoff: now.Add(-3 * time.Hour), Blobs: []GCBlobCandidate{{ID: id, Data: &GCObjectVersion{Key: s.blobDataKey(id), ETag: etag, Size: int64(len(data)), ModTime: now.Add(-4 * time.Hour)}}}}
	result, err := s.SweepGC(ctx, plan)
	if !errors.Is(err, ErrInvalidGCPlan) {
		t.Fatalf("forged plan err=%v", err)
	}
	if result.BlobsDeleted != 0 {
		t.Fatalf("deleted=%#v", result)
	}
	if _, err = m.Get(ctx, s.blobDataKey(id)); err != nil {
		t.Fatalf("young orphan deleted: %v", err)
	}
}

func TestCommitAndRootRejectForgedParentTopology(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	s := newTestStore(t, m, &now)
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	declaredRoot := root
	_, body, err := canonicalNode(&root, &declaredRoot, 7, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	forged := NodeID(hashBytes(body))
	if _, err = m.Put(ctx, s.nodeKey(forged), body, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Root(ctx, forged); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Root forged=%v", err)
	}
	if _, err = s.CommitChild(ctx, forged, nil, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("CommitChild forged=%v", err)
	}
}

func TestRefAndLeaseTolerateClockRollback(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2000, 0).UTC()
	s := newTestStore(t, s3backend.NewMemory(), &now)
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := s.CreateRef(ctx, "clock", root)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireLease(ctx, root, "owner", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(-time.Second)
	ref, err = s.CompareAndSwapRef(ctx, "clock", ref.Revision, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetRef(ctx, "clock"); err != nil {
		t.Fatalf("ref corrupt after rollback: %v", err)
	}
	lease, err = s.RenewLease(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.getLeaseWire(ctx, lease.ID); err != nil {
		t.Fatalf("lease corrupt after rollback: %v", err)
	}
}

type cancelAfterPutBackend struct {
	s3backend.Backend
	cancel context.CancelFunc
	needle string
}

func (b *cancelAfterPutBackend) Put(ctx context.Context, key string, body []byte, pre *s3backend.Preconditions) (string, error) {
	etag, err := b.Backend.Put(ctx, key, body, pre)
	if err == nil && strings.Contains(key, b.needle) {
		b.cancel()
	}
	return etag, err
}
func TestMutationGateReleaseSurvivesCallerCancellation(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	base := s3backend.NewMemory()
	plain := newTestStore(t, base, &now)
	root, err := plain.CommitRoot(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wrapped := &cancelAfterPutBackend{Backend: base, cancel: cancel, needle: "/r/"}
	s := newTestStore(t, wrapped, &now)
	if _, err = s.CreateRef(ctx, "cancel", root); err != nil {
		t.Fatalf("create=%v", err)
	}
	info, err := s.MutationGate(context.Background())
	if err != nil || info.Held {
		t.Fatalf("gate=%#v err=%v", info, err)
	}
	if _, err = plain.CommitRoot(context.Background(), nil, []byte("after")); err != nil {
		t.Fatalf("store bricked: %v", err)
	}
}
func TestRecoverMutationGateRequiresFence(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	s := newTestStore(t, s3backend.NewMemory(), &now)
	g, err := s.acquireMutationGate(ctx, "crashed")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecoverMutationGate(ctx, g.wire.Fence+1); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong fence=%v", err)
	}
	if err = s.RecoverMutationGate(ctx, g.wire.Fence); err != nil {
		t.Fatal(err)
	}
	info, _ := s.MutationGate(ctx)
	if info.Held {
		t.Fatal("gate still held")
	}
}

func TestGCPlanCannotDeleteBlobPublishedAfterPlan(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	data := []byte("planned raw")
	id := digest(data)
	if _, err := m.Put(ctx, s.blobDataKey(id), data, nil); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	plan, err := s.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := s.PutBlob(ctx, id, bytes.NewReader(data), WithExpectedBlobSize(int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if _, err = s.SweepGC(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StatBlob(ctx, ref); err != nil {
		t.Fatalf("published blob deleted: %v", err)
	}
}
func TestIdempotentPutBlobRefreshesRecentPin(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	data := []byte("existing")
	ref := putBlob(t, ctx, s, data)
	now = now.Add(2 * time.Hour)
	plan, err := s.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutBlob(ctx, ref.Hash, bytes.NewReader(data), WithExpectedBlobSize(int64(len(data)))); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if _, err = s.SweepGC(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StatBlob(ctx, ref); err != nil {
		t.Fatalf("refreshed blob deleted: %v", err)
	}
}

func TestGetNodeRejectsNonNormalizedEmptyForms(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	s := newTestStore(t, m, &now)
	forms := [][]byte{[]byte(`{"v":1,"parent":null,"root":null,"depth":0,"payloads":null,"opaque_metadata":null}`), []byte(`{"v":1,"parent":null,"root":null,"depth":0,"payloads":[],"opaque_metadata":""}`)}
	for _, body := range forms {
		id := NodeID(hashBytes(body))
		if _, err := m.Put(ctx, s.nodeKey(id), body, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetNode(ctx, id); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("body=%s err=%v", body, err)
		}
	}
}

func TestConcurrentRefCASHasOneWinner(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	s := newTestStore(t, s3backend.NewMemory(), &now, WithRetry(s3collections.RetryPolicy{MaxAttempts: 20, Base: time.Microsecond, Max: time.Millisecond, Jitter: -1}))
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := s.CreateRef(ctx, "main", root)
	if err != nil {
		t.Fatal(err)
	}
	const n = 12
	nodes := make([]NodeID, n)
	for i := range nodes {
		nodes[i], err = s.CommitChild(ctx, root, nil, []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for _, id := range nodes {
		wg.Add(1)
		go func(id NodeID) {
			defer wg.Done()
			_, e := s.CompareAndSwapRef(ctx, "main", ref.Revision, id)
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				wins++
			} else if !errors.Is(e, ErrConflict) {
				t.Errorf("cas=%v", e)
			}
		}(id)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("wins=%d", wins)
	}
	got, err := s.GetRef(ctx, "main")
	if err != nil || got.Revision != 2 {
		t.Fatalf("ref=%#v %v", got, err)
	}
}

func TestAmbiguousRefAndLeaseWritesReconcile(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	base := s3backend.NewMemory()
	plain := newTestStore(t, base, &now)
	root, err := plain.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	child, err := plain.CommitChild(ctx, root, nil, []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	chaos := s3backend.NewChaos(base, s3backend.ChaosConfig{AmbiguousWriteRate: 1})
	s := newTestStore(t, chaos, &now, WithRetry(s3collections.RetryPolicy{MaxAttempts: 1}))
	ref, err := s.CreateRef(ctx, "ambiguous", root)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = s.CompareAndSwapRef(ctx, ref.Name, ref.Revision, child)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireLease(ctx, child, "owner", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	lease, err = s.RenewLease(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteRef(ctx, ref.Name, ref.Revision); err != nil {
		t.Fatal(err)
	}
}

func TestBlobChecksumSemanticsAfterSameSizeCorruption(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	s := newTestStore(t, m, &now)
	data := []byte("correct bytes")
	ref := putBlob(t, ctx, s, data)
	bad := bytes.Repeat([]byte("x"), len(data))
	if _, err := m.Put(ctx, s.blobDataKey(ref.Hash), bad, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StatBlob(ctx, ref); err != nil {
		t.Fatalf("stat should validate declaration/size: %v", err)
	}
	r, err := s.GetBlob(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	r.Close()
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("full read err=%v", err)
	}
	rr, err := s.GetBlob(ctx, ref, WithRange(0, 3))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rr)
	rr.Close()
	if !bytes.Equal(got, bad[:3]) {
		t.Fatalf("range=%q", got)
	}
}

func TestGCActiveLeaseRetainsUnreferencedLineage(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	root, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	child, err := s.CommitChild(ctx, root, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireLease(ctx, child, "worker", 4*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	plan, err := s.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range plan.Nodes {
		if c.ID == root || c.ID == child {
			t.Fatal("leased lineage planned")
		}
	}
	_ = lease
}
func TestGCPersistedPlanRejectsTampering(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	_, err := s.CommitRoot(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	plan, err := s.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	bad := plan
	bad.Cutoff = bad.Cutoff.Add(-time.Hour)
	if _, err = s.SweepGC(ctx, bad); !errors.Is(err, ErrInvalidGCPlan) {
		t.Fatalf("tamper=%v", err)
	}
}

func TestPostPlanBlobPinProtectsAcrossAllowedClockSkew(t *testing.T) {
	ctx := context.Background()
	clientNow := time.Unix(1000, 0).UTC()
	serverNow := clientNow.Add(-5 * time.Millisecond)
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return serverNow })
	s := newTestStore(t, m, &clientNow, WithGCGrace(20*time.Millisecond), WithClockSkewHint(10*time.Millisecond))
	data := []byte("skew-safe")
	ref := putBlob(t, ctx, s, data)
	clientNow = clientNow.Add(time.Hour)
	serverNow = clientNow.Add(-5 * time.Millisecond)
	plan, err := s.PlanGC(ctx, clientNow.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Blobs) != 1 {
		t.Fatalf("candidates=%#v", plan.Blobs)
	}
	if _, err = s.PutBlob(ctx, ref.Hash, bytes.NewReader(data), WithExpectedBlobSize(int64(len(data)))); err != nil {
		t.Fatal(err)
	}
	clientNow = clientNow.Add(30 * time.Millisecond)
	serverNow = clientNow.Add(-5 * time.Millisecond)
	if _, err = s.SweepGC(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StatBlob(ctx, ref); err != nil {
		t.Fatalf("post-plan publication lost across permitted skew: %v", err)
	}
}

type cancelAppliedGateBackend struct {
	s3backend.Backend
	cancel          context.CancelFunc
	cancelAt, count int
}

func (b *cancelAppliedGateBackend) Put(ctx context.Context, key string, body []byte, pre *s3backend.Preconditions) (string, error) {
	if !strings.Contains(key, "coordination/gate.json") {
		return b.Backend.Put(ctx, key, body, pre)
	}
	b.count++
	etag, err := b.Backend.Put(context.Background(), key, body, pre)
	if err == nil && b.count == b.cancelAt {
		b.cancel()
		return etag, context.Canceled
	}
	return etag, err
}
func TestMutationGateAcquireReconcilesAppliedCancellation(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	base := s3backend.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	wrapped := &cancelAppliedGateBackend{Backend: base, cancel: cancel, cancelAt: 1}
	s := newTestStore(t, wrapped, &now)
	g, err := s.acquireMutationGate(ctx, "cancel-acquire")
	if err != nil {
		t.Fatalf("acquire=%v", err)
	}
	if err = s.releaseMutationGate(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	info, err := s.MutationGate(context.Background())
	if err != nil || info.Held {
		t.Fatalf("gate=%#v err=%v", info, err)
	}
}
func TestMutationGateRenewReconcilesAppliedCancellation(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	base := s3backend.NewMemory()
	plain := newTestStore(t, base, &now)
	root, err := plain.CommitRoot(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wrapped := &cancelAppliedGateBackend{Backend: base, cancel: cancel, cancelAt: 2}
	s := newTestStore(t, wrapped, &now)
	_, _ = s.CreateRef(ctx, "renew-cancel", root)
	info, err := s.MutationGate(context.Background())
	if err != nil || info.Held {
		t.Fatalf("gate stranded %#v err=%v", info, err)
	}
	if _, err = plain.CommitRoot(context.Background(), nil, []byte("after-renew-cancel")); err != nil {
		t.Fatalf("store bricked: %v", err)
	}
}

func TestMutationGateReacquireReconcilesAppliedCancellation(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	base := s3backend.NewMemory()
	plain := newTestStore(t, base, &now)
	if _, err := plain.CommitRoot(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wrapped := &cancelAppliedGateBackend{Backend: base, cancel: cancel, cancelAt: 1}
	s := newTestStore(t, wrapped, &now)
	g, err := s.acquireMutationGate(ctx, "cancel-reacquire")
	if err != nil {
		t.Fatalf("reacquire=%v", err)
	}
	if err = s.releaseMutationGate(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	info, err := s.MutationGate(context.Background())
	if err != nil || info.Held {
		t.Fatalf("gate=%#v err=%v", info, err)
	}
}

func TestListAndGetGCPlans(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s := newTestStore(t, m, &now)
	plan, err := s.PlanGC(ctx, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetGCPlan(ctx, plan.ID)
	if err != nil || got.ID != plan.ID {
		t.Fatalf("get=%#v %v", got, err)
	}
	plans, err := s.ListGCPlans(ctx)
	if err != nil || len(plans) != 1 || plans[0].ID != plan.ID {
		t.Fatalf("plans=%#v %v", plans, err)
	}
}

func TestSweepPersistedPlanSurvivesGraceIncrease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := s3backend.NewMemory()
	m.SetClock(func() time.Time { return now })
	s1, err := New(m, "grace-change", WithBlobSizeRange(1, 1024), WithGCGrace(time.Minute), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s1.CommitRoot(ctx, nil, []byte("candidate")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	plan, err := s1.PlanGC(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes) == 0 {
		t.Fatal("expected candidate-bearing plan")
	}
	s2, err := New(m, "grace-change", WithBlobSizeRange(1, 1024), WithGCGrace(time.Hour), WithClockSkewHint(90*time.Minute), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	result, err := s2.SweepGC(ctx, plan)
	if err != nil {
		t.Fatalf("persisted plan rejected after config change: %v", err)
	}
	if result.NodesDeleted == 0 {
		t.Fatalf("candidate not swept: %#v", result)
	}
}
