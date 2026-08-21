package lru

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/damianb/s3collections/storage"
)

func newStore(t *testing.T, opts Options) (*Store, storage.KV) {
	t.Helper()
	kv := storage.NewMemoryKV()
	s, err := New(kv, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, kv
}

func TestNewInvalid(t *testing.T) {
	if _, err := New(nil, Options{}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("nil KV: got %v", err)
	}
	kv := storage.NewMemoryKV()
	if _, err := New(kv, Options{ShardCount: -1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative shards: got %v", err)
	}
}

func TestSetGetRevision(t *testing.T) {
	s, _ := newStore(t, Options{})
	ctx := context.Background()

	if _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: got %v", err)
	}
	meta := EntryMeta{SizeBytes: 100}
	if err := s.Set(ctx, "a", meta); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if e.Key != "a" || e.Meta.SizeBytes != 100 || e.Revision != 1 {
		t.Fatalf("unexpected entry: %+v", e)
	}
	if e.Meta.CreatedAt.IsZero() || e.Meta.LastAccessAt.IsZero() {
		t.Fatalf("timestamps not defaulted: %+v", e.Meta)
	}
	created := e.Meta.CreatedAt

	// Update bumps revision, preserves CreatedAt.
	if err := s.Set(ctx, "a", EntryMeta{SizeBytes: 200}); err != nil {
		t.Fatal(err)
	}
	e, err = s.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if e.Revision != 2 || e.Meta.SizeBytes != 200 {
		t.Fatalf("after update: %+v", e)
	}
	if !e.Meta.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt changed: %v vs %v", e.Meta.CreatedAt, created)
	}
}

func TestTouch(t *testing.T) {
	s, _ := newStore(t, Options{})
	ctx := context.Background()
	if err := s.Touch(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("touch missing: got %v", err)
	}
	past := time.Now().Add(-time.Hour)
	if err := s.Set(ctx, "a", EntryMeta{SizeBytes: 1, LastAccessAt: past}); err != nil {
		t.Fatal(err)
	}
	if err := s.Touch(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if e.Revision != 2 {
		t.Fatalf("revision after touch: %d", e.Revision)
	}
	if !e.Meta.LastAccessAt.After(past) {
		t.Fatalf("LastAccessAt not refreshed: %v", e.Meta.LastAccessAt)
	}
}

func TestTouchOnGet(t *testing.T) {
	s, _ := newStore(t, Options{TouchOnGet: true})
	ctx := context.Background()
	past := time.Now().Add(-time.Hour)
	if err := s.Set(ctx, "a", EntryMeta{SizeBytes: 1, LastAccessAt: past}); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if e.Revision != 2 || !e.Meta.LastAccessAt.After(past) {
		t.Fatalf("touch-on-get entry: %+v", e)
	}
}

func TestDelete(t *testing.T) {
	s, _ := newStore(t, Options{})
	ctx := context.Background()
	if err := s.Delete(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing: got %v", err)
	}
	if err := s.Set(ctx, "a", EntryMeta{SizeBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
}

func TestLenAndStats(t *testing.T) {
	s, _ := newStore(t, Options{CapacityItems: 10, CapacityBytes: 1000})
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := s.Set(ctx, fmt.Sprintf("k%d", i), EntryMeta{SizeBytes: 100}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.Len(ctx)
	if err != nil || n != 5 {
		t.Fatalf("Len = %d, %v", n, err)
	}
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Items != 5 || st.SizeBytes != 500 {
		t.Fatalf("Stats: %+v", st)
	}
	if st.CapacityItems != 10 || st.CapacityBytes != 1000 {
		t.Fatalf("caps: %+v", st)
	}
}

func TestEvictOldestDeterministic(t *testing.T) {
	s, _ := newStore(t, Options{CapacityItems: 2, ShardCount: 4})
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	// Insert out of access order.
	for i, key := range []string{"b", "c", "a"} {
		if err := s.Set(ctx, key, EntryMeta{
			SizeBytes:    1,
			CreatedAt:    base,
			LastAccessAt: base.Add(time.Duration(i) * time.Minute), // b oldest, c, a newest
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.evictOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("b should be evicted: %v", err)
	}
	for _, k := range []string{"a", "c"} {
		if _, err := s.Get(ctx, k); err != nil {
			t.Fatalf("%s should survive: %v", k, err)
		}
	}
	n, _ := s.Len(ctx)
	if n != 2 {
		t.Fatalf("len after evict: %d", n)
	}
}

func TestEvictByBytes(t *testing.T) {
	s, _ := newStore(t, Options{CapacityBytes: 200})
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 4; i++ {
		if err := s.Set(ctx, fmt.Sprintf("k%d", i), EntryMeta{
			SizeBytes:    100,
			LastAccessAt: base.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.evictOnce(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Items != 2 || st.SizeBytes != 200 {
		t.Fatalf("after byte evict: %+v", st)
	}
	if _, err := s.Get(ctx, "k0"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("k0 should be evicted")
	}
	if _, err := s.Get(ctx, "k1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("k1 should be evicted")
	}
}

func TestStartEvictorAndClose(t *testing.T) {
	kv := storage.NewMemoryKV()
	s, err := New(kv, Options{CapacityItems: 1, EvictorInterval: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.StartEvictor(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.StartEvictor(ctx); !errors.Is(err, ErrEvictorRunning) {
		t.Fatalf("second start: %v", err)
	}
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 3; i++ {
		if err := s.Set(ctx, fmt.Sprintf("k%d", i), EntryMeta{
			SizeBytes:    1,
			LastAccessAt: base.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		n, err := s.Len(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n <= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("evictor did not trim: len=%d", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second close: %v", err)
	}
	if _, err := s.Get(ctx, "k0"); !errors.Is(err, ErrClosed) {
		t.Fatalf("get after close: %v", err)
	}
	// Close must not close the KV.
	if err := kv.Put(ctx, "probe", []byte("x")); err != nil {
		t.Fatalf("KV closed by store: %v", err)
	}
}
