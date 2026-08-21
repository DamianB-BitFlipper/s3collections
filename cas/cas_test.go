package cas

import (
	"context"
	"errors"
	"github.com/damianb/s3collections/storage"
	"testing"
	"time"
)

func TestCRUDRevisionAndNoop(t *testing.T) {
	ctx := context.Background()
	kv := storage.NewMemoryKV()
	defer kv.Close()
	s := New(kv, "cas/")
	r, err := s.Create(ctx, "key", []byte("one"))
	if err != nil || r.Revision != 1 {
		t.Fatalf("Create=%+v %v", r, err)
	}
	r2, err := s.CompareAndSwap(ctx, "key", r.Revision, []byte("one"))
	if err != nil || r2.Revision != 1 {
		t.Fatalf("noop=%+v %v", r2, err)
	}
	r3, err := s.Update(ctx, "key", func(_ context.Context, cur Record) ([]byte, error) { return []byte("two"), nil })
	if err != nil || r3.Revision != 2 {
		t.Fatalf("Update=%+v %v", r3, err)
	}
	if _, err := s.CompareAndSwap(ctx, "key", 1, []byte("bad")); !errors.Is(err, ErrRevisionMismatch) {
		t.Fatalf("stale=%v", err)
	}
	d, err := s.Delete(ctx, "key", 2)
	if err != nil || d.Revision != 3 || d.State != StateTombstone {
		t.Fatalf("Delete=%+v %v", d, err)
	}
	if _, err := s.Get(ctx, "key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get deleted=%v", err)
	}
	meta, err := s.GetMeta(ctx, "key")
	if err != nil || meta.State != StateTombstone || meta.Revision != 3 {
		t.Fatalf("GetMeta=%+v %v", meta, err)
	}
}

func TestRollbackSizeAndResurrection(t *testing.T) {
	ctx := context.Background()
	kv := storage.NewMemoryKV()
	defer kv.Close()
	s := New(kv, "cas/", WithMaxValueBytes(3), WithAllowResurrection(true))
	if _, err := s.Create(ctx, "k", []byte("long")); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("size=%v", err)
	}
	r, _ := s.Create(ctx, "k", []byte("one"))
	d, _ := s.Delete(ctx, "k", r.Revision)
	rr, err := s.Create(ctx, "k", []byte("new"))
	if err != nil || rr.Revision != d.Revision+1 || !rr.CreatedAt.Equal(r.CreatedAt) {
		t.Fatalf("resurrect=%+v %v", rr, err)
	}
	sentinel := errors.New("stop")
	_, err = s.Update(ctx, "k", func(_ context.Context, _ Record) ([]byte, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "k")
	if string(got.Value) != "new" {
		t.Fatalf("rollback=%s", got.Value)
	}
}

func TestListAndGC(t *testing.T) {
	ctx := context.Background()
	kv := storage.NewMemoryKV()
	defer kv.Close()
	s := New(kv, "cas/", WithTombstoneRetention(time.Nanosecond))
	a, _ := s.Create(ctx, "a", []byte("a"))
	_, _ = s.Delete(ctx, "a", a.Revision)
	_, _ = s.Create(ctx, "b", []byte("b"))
	p, err := s.List(ctx, ListOptions{IncludeTombstones: true, Limit: 1})
	if err != nil || len(p.Records) != 1 || p.NextStartAfter == "" {
		t.Fatalf("page=%+v %v", p, err)
	}
	time.Sleep(time.Millisecond)
	n, err := s.GC(ctx)
	if err != nil || n != 1 {
		t.Fatalf("gc=%d %v", n, err)
	}
}
