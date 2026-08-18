package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

// enqueueAndDead creates n dead-letter markers in the given shard using
// deterministic timestamps.
func enqueueAndDead(t *testing.T, q *Queue, clk *fakeClock, shard uint16, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		_, _, err := q.Enqueue(ctx, []byte(fmt.Sprintf("s%d-%d", shard, i)), EnqueueOptions{Shard: &shard})
		if err != nil {
			t.Fatalf("Enqueue shard %d job %d: %v", shard, i, err)
		}
	}
}

// claimAll claims every pending job in q and returns them grouped by shard.
func claimAll(t *testing.T, q *Queue) map[uint16][]*Job {
	t.Helper()
	ctx := context.Background()
	out := make(map[uint16][]*Job)
	for {
		job, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: time.Hour})
		if errors.Is(err, ErrEmpty) {
			break
		}
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		out[job.Shard] = append(out[job.Shard], job)
	}
	return out
}

// deadInOrder dead-letters the supplied per-shard jobs round-robin by shard,
// advancing the clock one microsecond per job so each marker gets a unique,
// globally ordered timestamp.
func deadInOrder(t *testing.T, q *Queue, clk *fakeClock, byShard map[uint16][]*Job) {
	t.Helper()
	ctx := context.Background()
	for i := 0; ; i++ {
		advanced := false
		for shard := uint16(0); int(shard) < len(byShard); shard++ {
			if i >= len(byShard[shard]) {
				continue
			}
			advanced = true
			clk.Advance(time.Microsecond)
			if err := byShard[shard][i].Dead(ctx, "boom"); err != nil {
				t.Fatalf("Dead shard %d job %d: %v", shard, i, err)
			}
		}
		if !advanced {
			break
		}
	}
}

func TestListDeadAllShardsPagination(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "dead-all-paginate", WithShards(3))
	ctx := context.Background()

	counts := []int{1500, 3, 700}
	for shard, n := range counts {
		enqueueAndDead(t, q, clk, uint16(shard), n)
	}
	byShard := claimAll(t, q)
	wantTotal := 0
	for _, n := range counts {
		wantTotal += n
	}
	if got := len(byShard[0]) + len(byShard[1]) + len(byShard[2]); got != wantTotal {
		t.Fatalf("claimed %d jobs, want %d", got, wantTotal)
	}
	deadInOrder(t, q, clk, byShard)

	var got []DeadItem
	cursor := ""
	limits := []int{1000, 997}
	page := 0
	for {
		limit := 1
		if page < len(limits) {
			limit = limits[page]
		}
		page++
		items, next, err := q.ListDead(ctx, ListDeadOptions{StartAfter: cursor, Limit: limit})
		if err != nil {
			t.Fatalf("ListDead page %d: %v", page, err)
		}
		if len(items) > limit {
			t.Fatalf("page %d returned %d items, limit %d", page, len(items), limit)
		}
		if len(items) == 0 {
			break
		}
		got = append(got, items...)
		cursor = next
	}

	if len(got) != wantTotal {
		t.Fatalf("returned %d items, want %d", len(got), wantTotal)
	}
	seen := make(map[string]struct{}, len(got))
	for i, it := range got {
		if _, ok := seen[it.ID]; ok {
			t.Fatalf("duplicate job id %q at index %d", it.ID, i)
		}
		seen[it.ID] = struct{}{}
		if i == 0 {
			continue
		}
		prev := got[i-1]
		if it.When.Before(prev.When) || (it.When.Equal(prev.When) && it.ID < prev.ID) {
			t.Fatalf("order violation at %d: prev=%+v cur=%+v", i, prev, it)
		}
	}
}

func TestListDeadAllShardsLimitOne(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "dead-all-one", WithShards(3))
	ctx := context.Background()

	counts := []int{2, 2, 2}
	for shard, n := range counts {
		enqueueAndDead(t, q, clk, uint16(shard), n)
	}
	byShard := claimAll(t, q)
	deadInOrder(t, q, clk, byShard)

	var got []DeadItem
	cursor := ""
	for {
		items, next, err := q.ListDead(ctx, ListDeadOptions{StartAfter: cursor, Limit: 1})
		if err != nil {
			t.Fatalf("ListDead: %v", err)
		}
		if len(items) == 0 {
			break
		}
		if len(items) != 1 {
			t.Fatalf("Limit=1 returned %d items", len(items))
		}
		got = append(got, items...)
		cursor = next
	}
	if len(got) != 6 {
		t.Fatalf("got %d items, want 6", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].When.Before(got[i-1].When) {
			t.Fatalf("order violation at %d", i)
		}
	}
}

func TestListDeadAllShardsCorruptCursor(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "dead-cursor", WithShards(3))
	ctx := context.Background()

	enqueueAndDead(t, q, clk, 0, 1)
	byShard := claimAll(t, q)
	deadInOrder(t, q, clk, byShard)

	corrupt := []string{
		"not-json",
		`{"v":2}`,
		`{"v":1,"s":{"0000":{"suffix":"bad"}}}`,
		`{"v":1,"s":{"gggg":{"suffix":"00000000000000000000/job"}}}`,
		`{"v":1,"s":{"0004":{"suffix":"00000000000000000000/job"}}}`,
	}
	for _, c := range corrupt {
		_, _, err := q.ListDead(ctx, ListDeadOptions{StartAfter: c})
		if err == nil {
			t.Errorf("cursor %q returned no error", c)
		}
	}
}

func TestListDeadAllShardsExhaustion(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "dead-exhaust", WithShards(3))
	ctx := context.Background()

	counts := []int{5, 1, 3}
	for shard, n := range counts {
		enqueueAndDead(t, q, clk, uint16(shard), n)
	}
	byShard := claimAll(t, q)
	deadInOrder(t, q, clk, byShard)

	// Walk the whole set in one call.
	items, cursor, err := q.ListDead(ctx, ListDeadOptions{Limit: 100})
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(items) != 9 || cursor == "" {
		t.Fatalf("first pass: %d items, cursor=%q", len(items), cursor)
	}

	// Replaying the terminal cursor must return an empty page with an
	// empty cursor.
	items2, cursor2, err := q.ListDead(ctx, ListDeadOptions{StartAfter: cursor, Limit: 100})
	if err != nil {
		t.Fatalf("ListDead terminal: %v", err)
	}
	if len(items2) != 0 || cursor2 != "" {
		t.Fatalf("terminal pass: %d items, cursor=%q", len(items2), cursor2)
	}
}

func TestListDeadSingleShardStillWorks(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "dead-single", WithShards(4))
	ctx := context.Background()

	for shard := uint16(0); shard < 4; shard++ {
		enqueueAndDead(t, q, clk, shard, 3)
	}
	byShard := claimAll(t, q)
	deadInOrder(t, q, clk, byShard)

	// Scan shard 2 with page size 2.
	var got []DeadItem
	cursor := ""
	for {
		items, next, err := q.ListDead(ctx, ListDeadOptions{Shards: []uint16{2}, StartAfter: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("ListDead single: %v", err)
		}
		got = append(got, items...)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(got) != 3 {
		t.Fatalf("single shard returned %d items, want 3", len(got))
	}
	for _, it := range got {
		if it.Shard != 2 {
			t.Fatalf("wrong shard %+v", it)
		}
	}
}

// TestListDeadAllShardsRespectsExplicitShardsList verifies that a non-empty
// Shards slice with more than one element is rejected rather than misread.
func TestListDeadAllShardsRejectsMultiShardSlice(t *testing.T) {
	t.Parallel()
	q, _, _ := testQueue(t, "dead-multi-reject")
	ctx := context.Background()
	_, _, err := q.ListDead(ctx, ListDeadOptions{Shards: []uint16{0, 1}})
	if err == nil {
		t.Fatal("expected error for multi-element Shards slice")
	}
}
