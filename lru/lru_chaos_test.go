package lru

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/damianb/s3collections/s3backend"
)

// lockedBackend serializes access to a Backend so that s3backend.Chaos'
// non-thread-safe rand.Rand can be used from multiple goroutines without
// triggering the race detector.
type lockedBackend struct {
	mu sync.Mutex
	b  s3backend.Backend
}

func (l *lockedBackend) Get(ctx context.Context, key string) (*s3backend.Object, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Get(ctx, key)
}

func (l *lockedBackend) Put(ctx context.Context, key string, body []byte, pre *s3backend.Preconditions) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Put(ctx, key, body, pre)
}

func (l *lockedBackend) Delete(ctx context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Delete(ctx, key)
}

func (l *lockedBackend) List(ctx context.Context, prefix string, opts *s3backend.ListOptions) (*s3backend.ListPage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.List(ctx, prefix, opts)
}

func TestChaosConvergence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chaos := s3backend.NewChaos(s3backend.NewMemory(), s3backend.ChaosConfig{
		Rand:               rand.New(rand.NewSource(7)),
		ErrorRate:          0.01,
		AmbiguousWriteRate: 0.005,
		DelayRate:          0.03,
		Delay:              time.Millisecond,
	})
	backend := &lockedBackend{b: chaos}

	capBytes := int64(40 << 10)
	store, err := New(backend, Options{
		Prefix:          "lru/",
		ShardCount:      8,
		CapacityBytes:   capBytes,
		EvictorInterval: 100 * time.Millisecond,
		EvictorWorkers:  2,
		TouchOnGet:      true,
		Retry:           RetryPolicy{MaxAttempts: 20, Base: 10 * time.Millisecond, Max: 200 * time.Millisecond, Jitter: 1.0},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()
	if err := store.StartEvictor(ctx); err != nil {
		t.Fatalf("StartEvictor: %v", err)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("c-%d", i)
		meta := EntryMeta{
			SizeBytes:    int64(1<<10 + rnd.Intn(6<<10)),
			CreatedAt:    time.Now(),
			LastAccessAt: time.Now(),
			AccessCount:  1,
		}
		_ = store.Set(ctx, key, meta)
		_, _ = store.Get(ctx, key)
		if i%5 == 0 {
			_ = store.Delete(ctx, key)
		}
	}

	time.Sleep(2 * time.Second)

	var st Stats
	for i := 0; i < 20; i++ {
		st, err = store.Stats(ctx)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	slack := int64(float64(capBytes) * 1.30)
	if st.ApproxBytes > slack {
		t.Errorf("live bytes %d exceed cap+slack %d", st.ApproxBytes, slack)
	}
}
