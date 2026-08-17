package cas

import (
	"context"
	"errors"
	"testing"

	"github.com/damianb/s3collections/s3backend"
)

func TestResurrect(t *testing.T) {
	ctx := context.Background()
	s, err := New(s3backend.NewMemory(), "res/")
	if err != nil {
		t.Fatal(err)
	}
	key := "k"
	r, err := s.Create(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	created := r.CreatedAt
	tomb, err := s.Delete(ctx, key, r.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if tomb.Revision != 2 {
		t.Fatalf("expected tomb rev 2, got %d", tomb.Revision)
	}

	// Without Resurrect, Update on tombstone returns ErrDeleted even with IncludeTombstone.
	_, err = s.Update(ctx, key, func(_ context.Context, cur Record) ([]byte, error) {
		return []byte("new"), nil
	}, WithIncludeTombstone())
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("expected ErrDeleted without resurrect, got %v", err)
	}

	// With Resurrect, fn may turn tombstone live.
	rec, err := s.Update(ctx, key, func(_ context.Context, cur Record) ([]byte, error) {
		if cur.State != Tombstone {
			t.Fatalf("expected tombstone in fn, got %v", cur.State)
		}
		return []byte("new"), nil
	}, WithIncludeTombstone(), WithResurrect())
	if err != nil {
		t.Fatalf("resurrect: %v", err)
	}
	if rec.State != Live {
		t.Fatalf("expected live, got %v", rec.State)
	}
	if rec.Revision != 3 {
		t.Fatalf("expected rev 3, got %d", rec.Revision)
	}
	if !rec.CreatedAt.Equal(created) {
		t.Fatalf("created_at not preserved: %v vs %v", rec.CreatedAt, created)
	}
	if string(rec.Value) != "new" {
		t.Fatalf("unexpected value %q", rec.Value)
	}
}
