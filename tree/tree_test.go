package tree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/damianb/s3collections/storage"
)

// memKV is a minimal in-memory storage.KV.
type memKV struct {
	m   map[string][]byte
	txn bool
}

func newMemKV() *memKV { return &memKV{m: map[string][]byte{}} }

func (k *memKV) Get(_ context.Context, key string) ([]byte, error) {
	v, ok := k.m[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (k *memKV) Put(_ context.Context, key string, value []byte) error {
	k.m[key] = append([]byte(nil), value...)
	return nil
}

func (k *memKV) Delete(_ context.Context, key string) error {
	delete(k.m, key)
	return nil
}

func (k *memKV) Scan(_ context.Context, opts storage.ScanOptions) ([]storage.Entry, error) {
	var keys []string
	for key := range k.m {
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" && key <= opts.StartAfter {
			continue
		}
		keys = append(keys, key)
	}
	sortStrings(keys)
	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	if opts.Limit > 0 && len(keys) > opts.Limit {
		keys = keys[:opts.Limit]
	}
	out := make([]storage.Entry, 0, len(keys))
	for _, key := range keys {
		out = append(out, storage.Entry{Key: key, Value: append([]byte(nil), k.m[key]...)})
	}
	return out, nil
}

// memTx adapts memKV to storage.Tx.
type memTx struct{ k *memKV }

func (tx memTx) Get(key string) ([]byte, error) { return tx.k.Get(context.Background(), key) }
func (tx memTx) Put(key string, value []byte) error {
	return tx.k.Put(context.Background(), key, value)
}
func (tx memTx) Delete(key string) error { return tx.k.Delete(context.Background(), key) }
func (tx memTx) Scan(opts storage.ScanOptions) ([]storage.Entry, error) {
	return tx.k.Scan(context.Background(), opts)
}

func (k *memKV) Transaction(ctx context.Context, fn func(storage.Tx) error) error {
	return fn(memTx{k})
}

func (k *memKV) Close() error { return nil }

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// memBlobs is a minimal in-memory storage.BlobStore.
type memBlobs struct{ m map[string][]byte }

func newMemBlobs() *memBlobs { return &memBlobs{m: map[string][]byte{}} }

func (b *memBlobs) Put(_ context.Context, key string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if size >= 0 && int64(len(data)) != size {
		return storage.ErrSizeMismatch
	}
	b.m[key] = data
	return nil
}

func (b *memBlobs) Open(_ context.Context, key string) (io.ReadCloser, error) {
	data, ok := b.m[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *memBlobs) OpenRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	data, ok := b.m[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	if start > end {
		return nil, errors.New("bad range")
	}
	return io.NopCloser(bytes.NewReader(data[start:end])), nil
}

func (b *memBlobs) Stat(_ context.Context, key string) (storage.BlobInfo, error) {
	data, ok := b.m[key]
	if !ok {
		return storage.BlobInfo{}, storage.ErrNotFound
	}
	return storage.BlobInfo{Key: key, Size: int64(len(data))}, nil
}

func (b *memBlobs) Delete(_ context.Context, key string) error {
	delete(b.m, key)
	return nil
}

func (b *memBlobs) Close() error { return nil }

func newTestTree(t *testing.T) *Tree {
	t.Helper()
	eng, err := storage.NewEngine(newMemKV(), newMemBlobs())
	if err != nil {
		t.Fatal(err)
	}
	return New(eng)
}

func hashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestPutStatGetBlob(t *testing.T) {
	ctx := context.Background()
	tr := newTestTree(t)
	content := []byte("hello merkle world")
	h := hashOf(content)

	m, err := tr.PutBlob(ctx, h, bytes.NewReader(content), PutOptions{Key: "greeting", Size: int64(len(content))})
	if err != nil {
		t.Fatal(err)
	}
	if m.Hash != h || m.Size != int64(len(content)) || m.Key != "greeting" {
		t.Fatalf("bad manifest: %+v", m)
	}

	got, err := tr.StatBlob(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if got != m {
		t.Fatalf("stat mismatch: %+v != %+v", got, m)
	}

	rc, err := tr.GetBlob(ctx, h, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if !bytes.Equal(data, content) {
		t.Fatalf("content mismatch: %q", data)
	}
}

func TestPutBlobHashMismatch(t *testing.T) {
	ctx := context.Background()
	tr := newTestTree(t)
	_, err := tr.PutBlob(ctx, strings.Repeat("0", 64), strings.NewReader("data"), PutOptions{Size: 4})
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("want ErrHashMismatch, got %v", err)
	}
}

func TestGetBlobRange(t *testing.T) {
	ctx := context.Background()
	tr := newTestTree(t)
	content := []byte("0123456789abcdef")
	h := hashOf(content)
	if _, err := tr.PutBlob(ctx, h, bytes.NewReader(content), PutOptions{Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	rc, err := tr.GetBlob(ctx, h, 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if string(data) != "4567" {
		t.Fatalf("range mismatch: %q", data)
	}
}

func TestNodeDeterministicID(t *testing.T) {
	a := Node{Name: "root", Children: []string{"b", "a", "c"}}
	b := Node{Name: "root", Children: []string{"c", "a", "b"}}
	if a.ID() != b.ID() {
		t.Fatal("node ID must be independent of child ordering")
	}
	c := Node{Name: "root", Children: []string{"a", "b"}}
	if a.ID() == c.ID() {
		t.Fatal("different content must produce different IDs")
	}
}

func TestPutGetNode(t *testing.T) {
	ctx := context.Background()
	tr := newTestTree(t)
	n := Node{Name: "leaf", Leaf: true, Blobs: []string{"h2", "h1"}}
	id, err := tr.PutNode(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tr.GetNode(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != id || got.Name != "leaf" || !got.Leaf {
		t.Fatalf("bad node: %+v", got)
	}
	if _, err := tr.GetNode(ctx, strings.Repeat("f", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestRefCAS(t *testing.T) {
	ctx := context.Background()
	tr := newTestTree(t)
	node1, err := tr.PutNode(ctx, Node{Name: "node1", Leaf: true})
	if err != nil {
		t.Fatal(err)
	}
	node2, err := tr.PutNode(ctx, Node{Name: "node2", Leaf: true})
	if err != nil {
		t.Fatal(err)
	}
	r1, err := tr.CompareAndSwapRef(ctx, "main", node1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 1 || r1.NodeID != node1 {
		t.Fatalf("bad ref: %+v", r1)
	}
	if _, err := tr.CompareAndSwapRef(ctx, "main", node2, 0); !errors.Is(err, ErrRefConflict) {
		t.Fatalf("want ErrRefConflict, got %v", err)
	}
	r2, err := tr.CompareAndSwapRef(ctx, "main", node2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Version != 2 || r2.NodeID != node2 {
		t.Fatalf("bad ref: %+v", r2)
	}
	cur, err := tr.GetRef(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if cur != r2 {
		t.Fatalf("ref mismatch: %+v", cur)
	}
}

func TestLeaseFence(t *testing.T) {
	ctx := context.Background()
	tr := newTestTree(t)
	l1, err := tr.AcquireLease(ctx, "writer")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.CheckFence(ctx, "writer", l1.Token); err != nil {
		t.Fatal(err)
	}
	l2, err := tr.AcquireLease(ctx, "writer")
	if err != nil {
		t.Fatal(err)
	}
	if l2.Token <= l1.Token {
		t.Fatal("fencing token must increase")
	}
	if err := tr.CheckFence(ctx, "writer", l1.Token); !errors.Is(err, ErrFenced) {
		t.Fatalf("want ErrFenced, got %v", err)
	}
	if err := tr.CheckFence(ctx, "writer", l2.Token); err != nil {
		t.Fatal(err)
	}
}

func TestPlanAndSweepGC(t *testing.T) {
	ctx := context.Background()
	tr := newTestTree(t)

	keep := []byte("keep")
	drop := []byte("drop")
	keepH := hashOf(keep)
	dropH := hashOf(drop)
	if _, err := tr.PutBlob(ctx, keepH, bytes.NewReader(keep), PutOptions{Size: int64(len(keep))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.PutBlob(ctx, dropH, bytes.NewReader(drop), PutOptions{Size: int64(len(drop))}); err != nil {
		t.Fatal(err)
	}

	leafID, err := tr.PutNode(ctx, Node{Name: "leaf", Leaf: true, Blobs: []string{keepH}})
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := tr.PutNode(ctx, Node{Name: "root", Children: []string{leafID}})
	if err != nil {
		t.Fatal(err)
	}
	orphanID, err := tr.PutNode(ctx, Node{Name: "orphan", Leaf: true, Blobs: []string{dropH}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.CompareAndSwapRef(ctx, "main", rootID, 0); err != nil {
		t.Fatal(err)
	}

	plan, err := tr.PlanGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes) != 1 || plan.Nodes[0] != orphanID {
		t.Fatalf("bad node plan: %v", plan.Nodes)
	}
	if len(plan.Blobs) != 1 || plan.Blobs[0] != dropH {
		t.Fatalf("bad blob plan: %v", plan.Blobs)
	}

	if err := tr.SweepGC(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.GetNode(ctx, orphanID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan node not swept: %v", err)
	}
	if _, err := tr.StatBlob(ctx, dropH); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan blob not swept: %v", err)
	}
	if _, err := tr.GetNode(ctx, rootID); err != nil {
		t.Fatalf("reachable node swept: %v", err)
	}
	if _, err := tr.StatBlob(ctx, keepH); err != nil {
		t.Fatalf("reachable blob swept: %v", err)
	}
}

func TestFencedRefMutation(t *testing.T) {
	ctx := context.Background()
	eng, err := storage.NewEngine(storage.NewMemoryKV(), storage.NewMemoryBlobStore())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	tr := New(eng)
	nodeID, err := tr.PutNode(ctx, Node{Name: "root", Leaf: true})
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := tr.AcquireLease(ctx, "writer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.AcquireLease(ctx, "writer"); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.CompareAndSwapRefFenced(ctx, "writer", oldLease.Token, "main", nodeID, 0); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale mutation: %v", err)
	}
	current, _ := tr.AcquireLease(ctx, "current")
	if _, err := tr.CompareAndSwapRefFenced(ctx, "current", current.Token, "main", nodeID, 0); err != nil {
		t.Fatalf("current mutation: %v", err)
	}
}

func TestConcurrentBlobUploadsUseDistinctObjects(t *testing.T) {
	ctx := context.Background()
	eng, err := storage.NewEngine(storage.NewMemoryKV(), storage.NewMemoryBlobStore())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	tr := New(eng)
	const n = 12
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			data := bytes.Repeat([]byte{byte(i + 1)}, 64<<10)
			m, err := tr.PutBlob(ctx, hashOf(data), bytes.NewReader(data), PutOptions{Size: int64(len(data))})
			if err == nil {
				rc, openErr := tr.GetBlob(ctx, m.Hash, 0, -1)
				if openErr != nil {
					err = openErr
				} else {
					got, readErr := io.ReadAll(rc)
					_ = rc.Close()
					if readErr != nil {
						err = readErr
					} else if !bytes.Equal(got, data) {
						err = errors.New("body mismatch")
					}
				}
			}
			errCh <- err
		}(i)
	}
	for range n {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}
