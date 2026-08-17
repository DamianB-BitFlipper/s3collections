package cas

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

func TestConcurrencyUpdateHotKey(t *testing.T) {
	ctx := context.Background()
	m := s3collections.NewCaptureMeter()
	store, err := New(s3backend.NewMemory(), "stress/", WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}
	key := "counter"
	if _, err := store.Create(ctx, key, encodeInt64(0)); err != nil {
		t.Fatal(err)
	}

	workers := 64
	var success int64
	var wg sync.WaitGroup
	start := time.Now()
	policy := RetryPolicy{MaxAttempts: 100, Base: 1 * time.Millisecond, Max: 50 * time.Millisecond, Jitter: 0.5}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Update(ctx, key, func(_ context.Context, cur Record) ([]byte, error) {
				v := decodeInt64(cur.Value)
				return encodeInt64(v + 1), nil
			}, WithRetryPolicy(policy))
			if err == nil {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	dur := time.Since(start)

	rec, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("final get: %v", err)
	}
	if rec.Revision != 1+uint64(success) {
		t.Fatalf("revision=%d want %d (success=%d)", rec.Revision, 1+success, success)
	}
	if decodeInt64(rec.Value) != success {
		t.Fatalf("value=%d want %d", decodeInt64(rec.Value), success)
	}
	if success == int64(workers) {
		t.Logf("all %d updates succeeded in %v", workers, dur)
	} else {
		t.Logf("only %d/%d succeeded in %v", success, workers, dur)
	}
	if m.CounterSum("s3collections_conflicts_total") == 0 {
		t.Fatal("expected conflicts")
	}
}

func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func decodeInt64(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}
