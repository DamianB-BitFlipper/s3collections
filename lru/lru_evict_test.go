package lru

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

func TestConcurrentMixedWorkload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capBytes := int64(50 << 10)
	meter := s3collections.NewCaptureMeter()
	store, err := New(s3backend.NewMemory(), Options{
		Prefix:          "lru/",
		ShardCount:      8,
		CapacityBytes:   capBytes,
		EvictorInterval: 75 * time.Millisecond,
		EvictorWorkers:  2,
		TouchOnGet:      true,
		TouchPolicy: TouchPolicy{
			CoalesceWindow:    50 * time.Millisecond,
			UpdateAccessCount: true,
			UpdateLastAccess:  true,
		},
		Meter: meter,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()
	if err := store.StartEvictor(ctx); err != nil {
		t.Fatalf("StartEvictor: %v", err)
	}

	const workers = 16
	const keysPerWorker = 50
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for j := 0; j < keysPerWorker; j++ {
				key := fmt.Sprintf("w-%d-%d", id, j)
				meta := EntryMeta{
					SizeBytes:    int64(1<<10 + rnd.Intn(4<<10)),
					CreatedAt:    time.Now(),
					LastAccessAt: time.Now(),
					AccessCount:  1,
				}
				if err := store.Set(ctx, key, meta); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				if j%3 == 0 {
					_, _ = store.Get(ctx, key)
				}
				if j%7 == 0 {
					_ = store.Touch(ctx, key)
				}
				if j%13 == 0 {
					_ = store.Delete(ctx, key)
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	// Let evictor converge.
	time.Sleep(2 * time.Second)

	st, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	slack := int64(float64(capBytes) * 1.20)
	if st.ApproxBytes > slack {
		t.Errorf("live bytes %d exceed cap+slack %d", st.ApproxBytes, slack)
	}
}

func TestStaleCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	meter := s3collections.NewCaptureMeter()
	store, err := New(s3backend.NewMemory(), Options{
		Prefix:          "lru/",
		ShardCount:      4,
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

	key := "orphan"
	old := time.Now().Add(-10 * time.Minute)
	meta := EntryMeta{SizeBytes: 100, CreatedAt: old, LastAccessAt: old, AccessCount: 0}
	if err := store.Set(ctx, key, meta); err != nil {
		t.Fatalf("Set: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := store.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale entry was not cleaned")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if meter.CounterSum(metricEvictions) == 0 {
		t.Errorf("expected stale eviction counter > 0")
	}
}

func TestCapacityConvergence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capBytes := int64(80 << 10)
	store, err := New(s3backend.NewMemory(), Options{
		Prefix:          "lru/",
		ShardCount:      8,
		CapacityBytes:   capBytes,
		EvictorInterval: 75 * time.Millisecond,
		EvictorWorkers:  2,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()
	if err := store.StartEvictor(ctx); err != nil {
		t.Fatalf("StartEvictor: %v", err)
	}

	const k = 16
	const avgSize = 4096
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("writer-%d-key-%d", id, j)
				size := int64(avgSize/2 + rnd.Intn(avgSize))
				meta := EntryMeta{
					SizeBytes:    size,
					CreatedAt:    time.Now(),
					LastAccessAt: time.Now(),
					AccessCount:  1,
				}
				if err := store.Set(ctx, key, meta); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	// Let the evictor converge.
	time.Sleep(2 * time.Second)

	st, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	slack := int64(float64(capBytes) * 1.15)
	if st.ApproxBytes > slack {
		t.Errorf("live bytes %d exceed cap+slack %d", st.ApproxBytes, slack)
	}
}

func TestTombstoneGC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mem := s3backend.NewMemory()
	store, err := New(mem, Options{
		Prefix:          "lru/",
		ShardCount:      4,
		EvictorInterval: 50 * time.Millisecond,
		EvictorWorkers:  1,
		TombstoneMinAge: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()
	if err := store.StartEvictor(ctx); err != nil {
		t.Fatalf("StartEvictor: %v", err)
	}

	key := "dead"
	meta := EntryMeta{SizeBytes: 100, CreatedAt: time.Now(), LastAccessAt: time.Now(), AccessCount: 1}
	if err := store.Set(ctx, key, meta); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Wait until the rotating GC has visited every shard.
	time.Sleep(500 * time.Millisecond)

	_, err = store.Get(ctx, key)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after GC, got %v", err)
	}
}
