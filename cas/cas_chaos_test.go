package cas

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/damianb/s3collections/s3backend"
)

func TestChaosCRUD(t *testing.T) {
	ctx := context.Background()
	base := s3backend.NewMemory()
	chaos := s3backend.NewChaos(base, s3backend.ChaosConfig{
		ErrorRate:          0.1,
		AmbiguousWriteRate: 0.05,
		DelayRate:          0.1,
		Delay:              5 * time.Millisecond,
	})
	store, err := New(chaos, "chaos/", WithRetry(RetryPolicy{MaxAttempts: 20, Base: 1 * time.Millisecond, Max: 50 * time.Millisecond, Jitter: 1.0}))
	if err != nil {
		t.Fatal(err)
	}

	key := "chaos-key"
	_, err = store.Create(ctx, key, []byte("init"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 20; i++ {
		_, err := store.Update(ctx, key, func(_ context.Context, cur Record) ([]byte, error) {
			return append([]byte{}, cur.Value...), nil
		})
		if err != nil {
			t.Fatalf("update iter %d: %v", i, err)
		}
	}
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Value) != "init" {
		t.Fatalf("unexpected value %q", got.Value)
	}
	_, err = store.Delete(ctx, key, got.Revision)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = store.Get(ctx, key)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}
