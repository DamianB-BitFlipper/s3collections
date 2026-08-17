package lru

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	store, err := New(s3backend.NewMemory(), Options{
		Prefix:     "lru/",
		ShardCount: 4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	key := "user:1234:profile"
	meta := EntryMeta{SizeBytes: 16 << 10, CreatedAt: time.Now(), LastAccessAt: time.Now(), AccessCount: 1}
	if err := store.Set(ctx, key, meta); err != nil {
		t.Fatalf("Set: %v", err)
	}

	e, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e.Key != key {
		t.Errorf("key mismatch: got %q", e.Key)
	}
	if e.Meta.SizeBytes != meta.SizeBytes {
		t.Errorf("size mismatch")
	}
	if e.Revision == 0 {
		t.Errorf("revision not set")
	}

	meta2 := EntryMeta{SizeBytes: 32 << 10, CreatedAt: meta.CreatedAt, LastAccessAt: time.Now(), AccessCount: 2}
	if err := store.Set(ctx, key, meta2); err != nil {
		t.Fatalf("Set update: %v", err)
	}
	e, err = store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if e.Meta.SizeBytes != meta2.SizeBytes {
		t.Errorf("update did not change size")
	}

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestResurrectAfterEvict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	meter := s3collections.NewCaptureMeter()
	store, err := New(s3backend.NewMemory(), Options{
		Prefix:          "lru/",
		ShardCount:      4,
		CapacityBytes:   1 << 10,
		EvictorInterval: 50 * time.Millisecond,
		EvictorWorkers:  1,
		Meter:           meter,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()
	if err := store.StartEvictor(ctx); err != nil {
		t.Fatalf("StartEvictor: %v", err)
	}

	key := "big"
	meta := EntryMeta{SizeBytes: 8 << 10, CreatedAt: time.Now(), LastAccessAt: time.Now(), AccessCount: 1}
	if err := store.Set(ctx, key, meta); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Wait for eviction.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := store.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("entry was not evicted")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if meter.CounterSum(metricEvictions) == 0 {
		t.Errorf("expected eviction counter > 0")
	}

	// Resurrect.
	meta2 := EntryMeta{SizeBytes: 4 << 10, CreatedAt: time.Now(), LastAccessAt: time.Now(), AccessCount: 1}
	if err := store.Set(ctx, key, meta2); err != nil {
		t.Fatalf("resurrect Set: %v", err)
	}
	e, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after resurrect: %v", err)
	}
	if e.Meta.SizeBytes != meta2.SizeBytes {
		t.Errorf("resurrected size mismatch")
	}
}

func TestTouchCoalescing(t *testing.T) {
	ctx := context.Background()
	meter := s3collections.NewCaptureMeter()
	store, err := New(s3backend.NewMemory(), Options{
		Prefix:     "lru/",
		ShardCount: 4,
		TouchPolicy: TouchPolicy{
			CoalesceWindow:    200 * time.Millisecond,
			UpdateAccessCount: true,
			UpdateLastAccess:  true,
		},
		Meter: meter,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	key := "k"
	meta := EntryMeta{SizeBytes: 100, CreatedAt: time.Now(), LastAccessAt: time.Now(), AccessCount: 1}
	if err := store.Set(ctx, key, meta); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Rapid touches inside the coalesce window should skip writes.
	for i := 0; i < 20; i++ {
		if err := store.Touch(ctx, key); err != nil {
			t.Fatalf("Touch %d: %v", i, err)
		}
	}

	writes := meter.CounterSum(metricTouchWrites)
	skips := meter.CounterSum(metricTouchSkips)
	if writes > 1 {
		t.Errorf("expected <=1 touch write inside window, got %v", writes)
	}
	if skips < 19 {
		t.Errorf("expected >=19 touch skips, got %v", skips)
	}

	// After the window passes, a touch should write.
	time.Sleep(250 * time.Millisecond)
	if err := store.Touch(ctx, key); err != nil {
		t.Fatalf("Touch after window: %v", err)
	}
	if meter.CounterSum(metricTouchWrites) < 1 {
		t.Errorf("expected a touch write after window")
	}
}

func TestLenAndStats(t *testing.T) {
	ctx := context.Background()
	store, err := New(s3backend.NewMemory(), Options{
		Prefix:     "lru/",
		ShardCount: 4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	for i := 0; i < 10; i++ {
		meta := EntryMeta{SizeBytes: 100, CreatedAt: time.Now(), LastAccessAt: time.Now(), AccessCount: 1}
		if err := store.Set(ctx, keyf("key-%d", i), meta); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	n, err := store.Len(ctx)
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	if n != 10 {
		t.Errorf("Len = %d, want 10", n)
	}

	st, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Shards != 4 {
		t.Errorf("Shards = %d", st.Shards)
	}
	if st.ApproxItems != 10 {
		t.Errorf("ApproxItems = %d, want 10", st.ApproxItems)
	}
	if st.ApproxBytes != 10*100 {
		t.Errorf("ApproxBytes = %d, want 1000", st.ApproxBytes)
	}
}

func keyf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}

func TestStartEvictorIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := New(s3backend.NewMemory(), Options{Prefix: "lru/", ShardCount: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	if err := store.StartEvictor(ctx); err != nil {
		t.Fatalf("first StartEvictor: %v", err)
	}
	if err := store.StartEvictor(ctx); err != nil {
		t.Fatalf("second StartEvictor: %v", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := New(s3backend.NewMemory(), Options{Prefix: "lru/", ShardCount: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = store.StartEvictor(ctx)
	if err := store.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := store.Len(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}

func TestShardFormatting(t *testing.T) {
	store, err := New(s3backend.NewMemory(), Options{Prefix: "lru/", ShardCount: 128})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()
	if got := store.shardStr(0); got != "000" {
		t.Errorf("shard 0 = %q, want 000", got)
	}
	if got := store.shardStr(127); got != "127" {
		t.Errorf("shard 127 = %q, want 127", got)
	}
	if got := store.shardStr(63); got != "063" {
		t.Errorf("shard 63 = %q, want 063", got)
	}
}
