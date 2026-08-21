package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

func testCtx(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

// --- MemoryKV tests ---

func TestMemoryKVGetPutDelete(t *testing.T) {
	ctx := testCtx(t)
	kv := NewMemoryKV()
	defer kv.Close()

	if _, err := kv.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: want ErrNotFound, got %v", err)
	}
	if err := kv.Put(ctx, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, err := kv.Get(ctx, "a")
	if err != nil || string(v) != "1" {
		t.Fatalf("Get: %q, %v", v, err)
	}
	// Returned values are copies.
	v[0] = 'X'
	v2, _ := kv.Get(ctx, "a")
	if string(v2) != "1" {
		t.Fatal("stored value mutated through returned slice")
	}
	if err := kv.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: want ErrNotFound, got %v", err)
	}
	// Deleting a missing key is fine.
	if err := kv.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryKVScan(t *testing.T) {
	ctx := testCtx(t)
	kv := NewMemoryKV()
	defer kv.Close()
	for _, k := range []string{"a/1", "a/2", "a/3", "b/1", "b/2", "c"} {
		if err := kv.Put(ctx, k, []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := kv.Scan(ctx, ScanOptions{Prefix: "a/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].Key != "a/1" || entries[2].Key != "a/3" {
		t.Fatalf("prefix scan: %+v", entries)
	}

	entries, err = kv.Scan(ctx, ScanOptions{StartAfter: "a/1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 || entries[0].Key != "a/2" {
		t.Fatalf("start-after scan: %+v", entries)
	}

	entries, err = kv.Scan(ctx, ScanOptions{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Key != "a/1" {
		t.Fatalf("limit scan: %+v", entries)
	}

	entries, err = kv.Scan(ctx, ScanOptions{Reverse: true, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].Key != "c" || entries[2].Key != "b/1" {
		t.Fatalf("reverse scan: %+v", entries)
	}

	entries, err = kv.Scan(ctx, ScanOptions{Prefix: "b/", Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Key != "b/2" || entries[1].Key != "b/1" {
		t.Fatalf("prefix reverse scan: %+v", entries)
	}

	entries, err = kv.Scan(ctx, ScanOptions{Reverse: true, StartAfter: "b/2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 || entries[0].Key != "b/1" || entries[3].Key != "a/1" {
		t.Fatalf("reverse start-after scan: %+v", entries)
	}
}

func TestMemoryKVTransactionCommit(t *testing.T) {
	ctx := testCtx(t)
	kv := NewMemoryKV()
	defer kv.Close()

	err := kv.Transaction(ctx, func(tx Tx) error {
		if err := tx.Put("x", []byte("1")); err != nil {
			return err
		}
		if err := tx.Put("y", []byte("2")); err != nil {
			return err
		}
		return tx.Delete("y")
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err := kv.Get(ctx, "x")
	if err != nil || string(v) != "1" {
		t.Fatalf("x = %q, %v", v, err)
	}
	if _, err := kv.Get(ctx, "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("y should be deleted: %v", err)
	}
}

func TestMemoryKVTransactionRollback(t *testing.T) {
	ctx := testCtx(t)
	kv := NewMemoryKV()
	defer kv.Close()
	if err := kv.Put(ctx, "keep", []byte("orig")); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("boom")
	err := kv.Transaction(ctx, func(tx Tx) error {
		if err := tx.Put("keep", []byte("changed")); err != nil {
			return err
		}
		if err := tx.Put("new", []byte("v")); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
	v, _ := kv.Get(ctx, "keep")
	if string(v) != "orig" {
		t.Fatalf("rollback failed, keep = %q", v)
	}
	if _, err := kv.Get(ctx, "new"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back write visible: %v", err)
	}
}

func TestMemoryKVTransactionReadYourWrites(t *testing.T) {
	ctx := testCtx(t)
	kv := NewMemoryKV()
	defer kv.Close()

	err := kv.Transaction(ctx, func(tx Tx) error {
		if err := tx.Put("k", []byte("v")); err != nil {
			return err
		}
		v, err := tx.Get("k")
		if err != nil || string(v) != "v" {
			return fmt.Errorf("read own write: %q, %v", v, err)
		}
		if err := tx.Delete("k"); err != nil {
			return err
		}
		if _, err := tx.Get("k"); !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("read after tx delete: %v", err)
		}
		entries, err := tx.Scan(ScanOptions{})
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return fmt.Errorf("scan should be empty, got %+v", entries)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMemoryKVTransactionIsolation(t *testing.T) {
	ctx := testCtx(t)
	kv := NewMemoryKV()
	defer kv.Close()
	if err := kv.Put(ctx, "outside", []byte("0")); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = kv.Transaction(ctx, func(tx Tx) error {
			close(started)
			<-release
			return tx.Put("inside", []byte("1"))
		})
	}()
	<-started
	// The transaction holds the store lock, so a concurrent Get blocks until
	// it commits; after commit both keys are visible atomically.
	close(release)
	wg.Wait()
	entries, err := kv.Scan(ctx, ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %+v", entries)
	}
}

func TestMemoryKVClosedAndContext(t *testing.T) {
	kv := NewMemoryKV()
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, err := range []error{
		func() error { _, e := kv.Get(ctx, "a"); return e }(),
		kv.Put(ctx, "a", nil),
		kv.Delete(ctx, "a"),
		func() error { _, e := kv.Scan(ctx, ScanOptions{}); return e }(),
		kv.Transaction(ctx, func(Tx) error { return nil }),
	} {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("want ErrClosed, got %v", err)
		}
	}
	if err := kv.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("double close: %v", err)
	}

	// Cancelled context is honored even on an open store.
	kv2 := NewMemoryKV()
	defer kv2.Close()
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := kv2.Get(cctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// --- MemoryBlobStore tests ---

func TestMemoryBlobStoreRoundTrip(t *testing.T) {
	ctx := testCtx(t)
	bs := NewMemoryBlobStore()
	defer bs.Close()

	data := []byte("hello blob world")
	if err := bs.Put(ctx, "b1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	rc, err := bs.Open(ctx, "b1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("round trip: %q, %v", got, err)
	}

	info, err := bs.Stat(ctx, "b1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Key != "b1" || info.Size != int64(len(data)) || info.ETag == "" {
		t.Fatalf("stat: %+v", info)
	}

	if err := bs.Delete(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.Stat(ctx, "b1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stat after delete: %v", err)
	}
	if err := bs.Delete(ctx, "b1"); err != nil {
		t.Fatal(err) // idempotent delete
	}
}

func TestMemoryBlobStoreUnknownSize(t *testing.T) {
	ctx := testCtx(t)
	bs := NewMemoryBlobStore()
	defer bs.Close()
	data := []byte(strings.Repeat("x", 1000))
	if err := bs.Put(ctx, "k", bytes.NewReader(data), -1); err != nil {
		t.Fatal(err)
	}
	info, err := bs.Stat(ctx, "k")
	if err != nil || info.Size != 1000 {
		t.Fatalf("stat: %+v, %v", info, err)
	}
}

func TestMemoryBlobStoreExactLengthValidation(t *testing.T) {
	ctx := testCtx(t)
	bs := NewMemoryBlobStore()
	defer bs.Close()

	if err := bs.Put(ctx, "short", strings.NewReader("ab"), 5); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("short reader: %v", err)
	}
	if err := bs.Put(ctx, "long", strings.NewReader("abcdef"), 3); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("long reader: %v", err)
	}
	if _, err := bs.Stat(ctx, "short"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed put must not store: %v", err)
	}
	if _, err := bs.Stat(ctx, "long"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed put must not store: %v", err)
	}
}

func TestMemoryBlobStoreOpenRange(t *testing.T) {
	ctx := testCtx(t)
	bs := NewMemoryBlobStore()
	defer bs.Close()
	data := []byte("0123456789")
	if err := bs.Put(ctx, "r", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	rc, err := bs.OpenRange(ctx, "r", 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "234" {
		t.Fatalf("range [2,5): %q", got)
	}
	// Full range.
	rc, err = bs.OpenRange(ctx, "r", 0, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("full range: %q", got)
	}
	// Invalid ranges.
	if _, err := bs.OpenRange(ctx, "r", -1, 3); err == nil {
		t.Fatal("negative start accepted")
	}
	if _, err := bs.OpenRange(ctx, "r", 5, 2); err == nil {
		t.Fatal("end < start accepted")
	}
	if _, err := bs.OpenRange(ctx, "r", 0, 100); err == nil {
		t.Fatal("end beyond size accepted")
	}
	if _, err := bs.OpenRange(ctx, "missing", 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing blob range: %v", err)
	}
}

func TestMemoryBlobStoreClosedAndContext(t *testing.T) {
	bs := NewMemoryBlobStore()
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := bs.Put(ctx, "a", strings.NewReader("x"), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("put on closed: %v", err)
	}
	if _, err := bs.Open(ctx, "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("open on closed: %v", err)
	}
	if _, err := bs.Stat(ctx, "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("stat on closed: %v", err)
	}
	if err := bs.Delete(ctx, "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("delete on closed: %v", err)
	}
	if err := bs.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("double close: %v", err)
	}

	bs2 := NewMemoryBlobStore()
	defer bs2.Close()
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := bs2.Open(cctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestMemoryBlobStoreConcurrency(t *testing.T) {
	ctx := testCtx(t)
	bs := NewMemoryBlobStore()
	defer bs.Close()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("blob-%d", i)
			data := bytes.Repeat([]byte{byte(i)}, 64)
			if err := bs.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Error(err)
				return
			}
			rc, err := bs.Open(ctx, key)
			if err != nil {
				t.Error(err)
				return
			}
			got, _ := io.ReadAll(rc)
			rc.Close()
			if !bytes.Equal(got, data) {
				t.Errorf("blob %s corrupted", key)
			}
		}(i)
	}
	wg.Wait()
}

// --- Engine tests ---

func TestNewEngine(t *testing.T) {
	if _, err := NewEngine(nil, NewMemoryBlobStore()); err == nil {
		t.Fatal("nil KV accepted")
	}
	if _, err := NewEngine(NewMemoryKV(), nil); err == nil {
		t.Fatal("nil BlobStore accepted")
	}
	kv, bs := NewMemoryKV(), NewMemoryBlobStore()
	e, err := NewEngine(kv, bs)
	if err != nil {
		t.Fatal(err)
	}
	if e.Metadata != KV(kv) || e.Blobs != BlobStore(bs) {
		t.Fatal("engine fields not wired")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	// Both stores are now closed.
	if _, err := kv.Get(context.Background(), "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("kv not closed: %v", err)
	}
	if _, err := bs.Stat(context.Background(), "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("blob store not closed: %v", err)
	}
}

func TestExactSizeReaderStreamsAndValidates(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("x"), 1<<20)
	r := &exactSizeReader{ctx: ctx, r: bytes.NewReader(data), remaining: int64(len(data))}
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("exact: len=%d err=%v", len(got), err)
	}
	short := &exactSizeReader{ctx: ctx, r: bytes.NewReader(data[:10]), remaining: 11}
	if _, err := io.ReadAll(short); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("short: %v", err)
	}
	long := &exactSizeReader{ctx: ctx, r: bytes.NewReader(data[:11]), remaining: 10}
	if _, err := io.ReadAll(long); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("long: %v", err)
	}
}
