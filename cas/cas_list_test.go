package cas

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/damianb/s3collections/s3backend"
)

func seedRecords(t *testing.T, ctx context.Context, s *Store, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := prefix + indexKey(i)
		if _, err := s.Create(ctx, k, []byte("v")); err != nil {
			t.Fatalf("create %s: %v", k, err)
		}
	}
}

func indexKey(i int) string {
	return string(rune('a'+i/26)) + string(rune('a'+i%26))
}

func TestListPaginationAndPrefix(t *testing.T) {
	ctx := context.Background()
	s, err := New(s3backend.NewMemory(), "list/")
	if err != nil {
		t.Fatal(err)
	}
	seedRecords(t, ctx, s, "alpha/", 10)
	seedRecords(t, ctx, s, "beta/", 5)

	// Full prefix list with small page size.
	var seen []string
	tok := ""
	for {
		page, err := s.List(ctx, &ListOptions{Prefix: "alpha/", ContinuationToken: tok, MaxKeys: 3})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, r := range page.Records {
			seen = append(seen, r.Key)
		}
		if !page.IsTruncated {
			break
		}
		tok = page.NextContinuationToken
		if tok == "" {
			t.Fatal("truncated but no token")
		}
	}
	if len(seen) != 10 {
		t.Fatalf("expected 10 alpha records, got %d", len(seen))
	}

	// StartAfter excludes the first key.
	page, err := s.List(ctx, &ListOptions{Prefix: "alpha/", StartAfter: "alpha/aa", MaxKeys: 10})
	if err != nil {
		t.Fatalf("list startafter: %v", err)
	}
	if len(page.Records) != 9 {
		t.Fatalf("expected 9 after startAfter, got %d", len(page.Records))
	}

	// Prefix "beta/" returns only beta records.
	page, err = s.List(ctx, &ListOptions{Prefix: "beta/", MaxKeys: 100})
	if err != nil {
		t.Fatalf("list beta: %v", err)
	}
	if len(page.Records) != 5 {
		t.Fatalf("expected 5 beta records, got %d", len(page.Records))
	}
}

func TestListTombstonesAndCorrupt(t *testing.T) {
	ctx := context.Background()
	b := s3backend.NewMemory()
	s, err := New(b, "list2/")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		k := indexKey(i)
		r, err := s.Create(ctx, k, []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			if _, err := s.Delete(ctx, k, r.Revision); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Inject a corrupt object under the prefix.
	b.Put(ctx, "list2/corrupt.cas.v1.json", []byte("not json"), nil)

	page, err := s.List(ctx, &ListOptions{MaxKeys: 100})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("expected 3 records (including tombstone), got %d", len(page.Records))
	}
	var tombstones int
	for _, r := range page.Records {
		if r.State == Tombstone {
			tombstones++
		}
	}
	if tombstones != 1 {
		t.Fatalf("expected 1 tombstone, got %d", tombstones)
	}
}

func TestGC(t *testing.T) {
	ctx := context.Background()
	s, err := New(s3backend.NewMemory(), "gc/", WithTombstoneRetention(100*time.Millisecond), WithClockSkewHint(1*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		k := indexKey(i)
		r, err := s.Create(ctx, k, []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Delete(ctx, k, r.Revision); err != nil {
			t.Fatal(err)
		}
	}
	// Leave one live record.
	if _, err := s.Create(ctx, "live", []byte("live")); err != nil {
		t.Fatal(err)
	}

	// GC too soon deletes nothing.
	n, err := s.GC(ctx, &GCOptions{})
	if err != nil {
		t.Fatalf("gc early: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 early deletes, got %d", n)
	}

	time.Sleep(150 * time.Millisecond)
	n, err = s.GC(ctx, &GCOptions{MaxDeletes: 3})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 deletes, got %d", n)
	}

	// Second pass deletes remaining tombstones.
	n, err = s.GC(ctx, &GCOptions{})
	if err != nil {
		t.Fatalf("gc second: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 remaining deletes, got %d", n)
	}

	// Live record still exists.
	if _, err := s.Get(ctx, "live"); err != nil {
		t.Fatalf("live record gone: %v", err)
	}
	// Tombstoned records can now be recreated.
	if _, err := s.Create(ctx, indexKey(0), []byte("new")); err != nil {
		t.Fatalf("recreate after gc: %v", err)
	}
}

func TestListLargePagination(t *testing.T) {
	ctx := context.Background()
	s, err := New(s3backend.NewMemory(), "biglist/")
	if err != nil {
		t.Fatal(err)
	}

	// Seed ~2500 keys across nested prefixes to exercise pagination and
	// prefix encoding through the strict-token Memory backend.
	const (
		totalKeys = 2500
		pageSize  = 10
	)
	want := make(map[string]struct{}, totalKeys)
	for i := 0; i < totalKeys; i++ {
		var prefix string
		switch i % 4 {
		case 0:
			prefix = "a/"
		case 1:
			prefix = "a/b/"
		case 2:
			prefix = "a/b/c/"
		case 3:
			prefix = "z/"
		}
		k := fmt.Sprintf("%skey-%04d", prefix, i)
		if _, err := s.Create(ctx, k, []byte("v")); err != nil {
			t.Fatalf("create %s: %v", k, err)
		}
		want[k] = struct{}{}
	}

	// Page through the entire store using the backend continuation tokens.
	seen := make(map[string]struct{})
	tok := ""
	pages := 0
	for {
		page, err := s.List(ctx, &ListOptions{ContinuationToken: tok, MaxKeys: pageSize})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		pages++
		for _, r := range page.Records {
			if _, ok := seen[r.Key]; ok {
				t.Fatalf("key %q seen twice", r.Key)
			}
			seen[r.Key] = struct{}{}
		}
		if !page.IsTruncated {
			break
		}
		tok = page.NextContinuationToken
		if tok == "" {
			t.Fatal("truncated but no token")
		}
	}
	if len(seen) != totalKeys {
		t.Fatalf("expected %d keys, got %d", totalKeys, len(seen))
	}
	if pages < totalKeys/pageSize {
		t.Fatalf("expected at least %d pages, got %d", totalKeys/pageSize, pages)
	}

	// Prefix "a/b/" should be listable and return only its descendants.
	seenPrefix := make(map[string]struct{})
	tok = ""
	for {
		page, err := s.List(ctx, &ListOptions{Prefix: "a/b/", ContinuationToken: tok, MaxKeys: pageSize})
		if err != nil {
			t.Fatalf("list prefix: %v", err)
		}
		for _, r := range page.Records {
			if !strings.HasPrefix(r.Key, "a/b/") {
				t.Fatalf("key %q does not match prefix a/b/", r.Key)
			}
			seenPrefix[r.Key] = struct{}{}
		}
		if !page.IsTruncated {
			break
		}
		tok = page.NextContinuationToken
	}
	wantPrefixCount := 0
	for k := range want {
		if strings.HasPrefix(k, "a/b/") {
			wantPrefixCount++
		}
	}
	if len(seenPrefix) != wantPrefixCount {
		t.Fatalf("expected %d keys under a/b/, got %d", wantPrefixCount, len(seenPrefix))
	}
}

func TestListInvalidContinuationToken(t *testing.T) {
	ctx := context.Background()
	s, err := New(s3backend.NewMemory(), "tok/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	// A garbage continuation token must fail cleanly (error, not panic).
	_, err = s.List(ctx, &ListOptions{ContinuationToken: "not-a-token", MaxKeys: 1})
	if err == nil {
		t.Fatal("expected error for garbage continuation token")
	}
	if !errors.Is(err, s3backend.ErrNotFound) {
		// s3backend.Memory returns a non-retryable s3backend.Error; ensure
		// it is propagated rather than swallowed.
		var s3err *s3backend.Error
		if !errors.As(err, &s3err) {
			t.Fatalf("expected *s3backend.Error, got %T: %v", err, err)
		}
		if s3err.Code != "InvalidContinuationToken" {
			t.Fatalf("expected InvalidContinuationToken, got %s", s3err.Code)
		}
	}
}

func TestListContinuationTokenIsNotSynthesizedFromKey(t *testing.T) {
	ctx := context.Background()
	s, err := New(s3backend.NewMemory(), "tokshape/")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := s.Create(ctx, indexKey(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	// Regression: the returned token must be the opaque backend token, not
	// the raw object key. We verify by checking that the token has the
	// backend-issued prefix and that paging proceeds without an
	// InvalidContinuationToken error.
	page, err := s.List(ctx, &ListOptions{MaxKeys: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !page.IsTruncated {
		t.Fatal("expected truncated page")
	}
	if page.NextContinuationToken == "" {
		t.Fatal("expected continuation token")
	}
	if !strings.HasPrefix(page.NextContinuationToken, "ctok-v1.") {
		t.Fatalf("expected opaque backend token, got %q", page.NextContinuationToken)
	}

	// Resuming with the token must succeed.
	page2, err := s.List(ctx, &ListOptions{ContinuationToken: page.NextContinuationToken, MaxKeys: 2})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(page2.Records) == 0 {
		t.Fatal("expected records on resumed page")
	}
}
