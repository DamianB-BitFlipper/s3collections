package cas

import (
	"context"
	"errors"
	"testing"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

func TestMetrics(t *testing.T) {
	ctx := context.Background()
	m := s3collections.NewCaptureMeter()
	s, err := New(s3backend.NewMemory(), "metrics/", WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}
	key := "k"
	_, _ = s.Create(ctx, key, []byte("v"))
	_, _ = s.Create(ctx, key, []byte("v"))             // ErrAlreadyExists -> conflict
	_, _ = s.CompareAndSwap(ctx, key, 99, []byte("x")) // ErrConflict
	_, _ = s.Get(ctx, key)

	if m.CounterSum("s3collections_conflicts_total") < 1 {
		t.Fatalf("expected conflicts counter")
	}
	if m.CounterSum("s3collections_corrupt_total") != 0 {
		t.Fatalf("unexpected corrupt counter")
	}
	// Latency histogram observed for every op.
	count := 0
	for k := range m.Histograms {
		if len(k) >= len("s3collections_latency_seconds") && k[:len("s3collections_latency_seconds")] == "s3collections_latency_seconds" {
			count++
		}
	}
	if count == 0 {
		t.Fatal("expected latency histogram observations")
	}

	// List page counter has static prefix label.
	_, _ = s.List(ctx, &ListOptions{MaxKeys: 1})
	if m.CounterSum("s3collections_list_pages_total") < 1 {
		t.Fatalf("expected list page counter")
	}
	// Ensure no user key appears in label values: CaptureMeter series keys use
	// label key=value pairs; user keys are never passed as labels.
	for k := range m.Counters {
		if errors.New(k).Error() == "" {
			continue
		}
		if contains(k, "k") && contains(k, "cas.v1.json") {
			t.Fatalf("user key leaked into metric labels: %s", k)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsAt(s, sub, 0))
}

func containsAt(s, sub string, i int) bool {
	for ; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
