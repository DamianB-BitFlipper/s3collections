package cas

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

func TestCreateGetCASUpdateDelete(t *testing.T) {
	ctx := context.Background()
	store, err := New(s3backend.NewMemory(), "test/", WithWriterID("writer-A"))
	if err != nil {
		t.Fatal(err)
	}

	key := "users/42"
	val1 := []byte("hello")

	// Create
	rec, err := store.Create(ctx, key, val1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rec.Revision != 1 || rec.State != Live || string(rec.Value) != "hello" {
		t.Fatalf("unexpected create record: %+v", rec)
	}

	// Create again -> ErrAlreadyExists
	_, err = store.Create(ctx, key, []byte("again"))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// Get
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Revision != 1 || !bytes.Equal(got.Value, val1) {
		t.Fatalf("unexpected get record: %+v", got)
	}

	// CAS
	val2 := []byte("world")
	rec2, err := store.CompareAndSwap(ctx, key, 1, val2)
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	if rec2.Revision != 2 || !bytes.Equal(rec2.Value, val2) {
		t.Fatalf("unexpected cas record: %+v", rec2)
	}

	// CAS stale -> ErrConflict
	_, err = store.CompareAndSwap(ctx, key, 1, []byte("stale"))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// Update no-op
	rec3, err := store.Update(ctx, key, func(_ context.Context, cur Record) ([]byte, error) {
		return cur.Value, nil
	})
	if err != nil {
		t.Fatalf("update no-op: %v", err)
	}
	if rec3.Revision != 2 {
		t.Fatalf("no-op update changed revision: %d", rec3.Revision)
	}

	// Update change
	rec4, err := store.Update(ctx, key, func(_ context.Context, cur Record) ([]byte, error) {
		return append(cur.Value, "!"...), nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if rec4.Revision != 3 || string(rec4.Value) != "world!" {
		t.Fatalf("unexpected update record: %+v", rec4)
	}

	// Delete
	del, err := store.Delete(ctx, key, 3)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if del.State != Tombstone || del.Revision != 4 {
		t.Fatalf("unexpected delete record: %+v", del)
	}

	// Get after delete -> NotFoundError
	_, err = store.Get(ctx, key)
	var nfe *NotFoundError
	if !errors.As(err, &nfe) || !nfe.Tombstoned || nfe.Revision != 4 {
		t.Fatalf("expected tombstone NotFoundError, got %v", err)
	}

	// GetMeta returns tombstone
	meta, err := store.GetMeta(ctx, key)
	if err != nil {
		t.Fatalf("getmeta: %v", err)
	}
	if meta.State != Tombstone {
		t.Fatalf("expected tombstone meta, got %+v", meta)
	}

	// Delete idempotence: expect==tombRev and tombRev-1
	if _, err := store.Delete(ctx, key, 4); err != nil {
		t.Fatalf("delete idempotent tombRev: %v", err)
	}
	if _, err := store.Delete(ctx, key, 3); err != nil {
		t.Fatalf("delete idempotent liveRev: %v", err)
	}
	if _, err := store.Delete(ctx, key, 2); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// Create blocked by tombstone
	_, err = store.Create(ctx, key, []byte("new"))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on tombstone, got %v", err)
	}

	// CAS into tombstone -> ErrDeleted
	_, err = store.CompareAndSwap(ctx, key, 4, []byte("res"))
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("expected ErrDeleted, got %v", err)
	}
}

func TestCreateValueTooLarge(t *testing.T) {
	ctx := context.Background()
	store, err := New(s3backend.NewMemory(), "test/", WithMaxValueBytes(5))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Create(ctx, "k", []byte("123456"))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}

func TestCaptureMeter(t *testing.T) {
	ctx := context.Background()
	m := s3collections.NewCaptureMeter()
	store, err := New(s3backend.NewMemory(), "test/", WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	for k := range m.Histograms {
		if len(k) >= len("s3collections_latency_seconds") && k[:len("s3collections_latency_seconds")] == "s3collections_latency_seconds" {
			return
		}
	}
	t.Fatal("expected latency observations")
}
