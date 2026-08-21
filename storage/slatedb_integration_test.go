//go:build slatedb

package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func newSlateTestKV(t *testing.T) KV {
	t.Helper()
	kv, err := OpenSlateDB(SlateDBConfig{
		Path:           fmt.Sprintf("storage-test-%s", randomTestID()),
		ObjectStoreURL: "memory:///",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Error(err)
		}
	})
	return kv
}

var slateTestSequence atomic.Uint64

func randomTestID() string {
	return fmt.Sprintf("%d", slateTestSequence.Add(1))
}

func TestSlateDBOfficialBindingSmoke(t *testing.T) {
	ctx := context.Background()
	kv := newSlateTestKV(t)
	if err := kv.Put(ctx, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if got, err := kv.Get(ctx, "a"); err != nil || string(got) != "1" {
		t.Fatalf("Get=%q err=%v", got, err)
	}
	if err := kv.Transaction(ctx, func(tx Tx) error {
		if err := tx.Put("a", []byte("2")); err != nil {
			return err
		}
		return tx.Put("b", []byte("3"))
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := kv.Scan(ctx, ScanOptions{})
	if err != nil || len(rows) != 2 {
		t.Fatalf("Scan=%v err=%v", rows, err)
	}
}

func TestSlateDBConcurrentIncrement(t *testing.T) {
	ctx := context.Background()
	kv := newSlateTestKV(t)
	if err := kv.Put(ctx, "n", []byte{0}); err != nil {
		t.Fatal(err)
	}
	const workers, each = 4, 10
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				for {
					err := kv.Transaction(ctx, func(tx Tx) error {
						v, err := tx.Get("n")
						if err != nil {
							return err
						}
						return tx.Put("n", []byte{v[0] + 1})
					})
					if errors.Is(err, ErrConflict) {
						continue
					}
					if err != nil {
						errs <- err
						return
					}
					break
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	got, err := kv.Get(ctx, "n")
	if err != nil || len(got) != 1 || got[0] != workers*each {
		t.Fatalf("n=%v err=%v", got, err)
	}
}

func TestSlateDBReverseContinuation(t *testing.T) {
	ctx := context.Background()
	kv := newSlateTestKV(t)
	for _, key := range []string{"a", "b", "c", "d"} {
		if err := kv.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := kv.Scan(ctx, ScanOptions{Reverse: true, StartAfter: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Key != "b" || rows[1].Key != "a" {
		t.Fatalf("rows=%+v", rows)
	}
}
