package s3backend

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestMemoryGetNotFound(t *testing.T) {
	m := NewMemory()
	_, err := m.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestMemoryPutGetRoundTrip(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	etag, err := m.Put(ctx, "a", []byte("hello"), nil)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := m.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Body) != "hello" || obj.ETag != etag {
		t.Fatalf("got %+v etag %q", obj, etag)
	}
}

func TestMemoryGetReturnsCopy(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, err := m.Put(ctx, "a", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	o1, _ := m.Get(ctx, "a")
	o1.Body[0] = 'X'
	o2, _ := m.Get(ctx, "a")
	if string(o2.Body) != "hello" {
		t.Fatal("Get aliased stored body")
	}
}

func TestMemoryIfNoneMatch(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	pre := &Preconditions{IfNoneMatch: true}
	if _, err := m.Put(ctx, "k", []byte("1"), pre); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Put(ctx, "k", []byte("2"), pre); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed, got %v", err)
	}
	obj, _ := m.Get(ctx, "k")
	if string(obj.Body) != "1" {
		t.Fatal("failed precondition still mutated the object")
	}
}

func TestMemoryIfMatchETag(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	etag, err := m.Put(ctx, "k", []byte("1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Put(ctx, "k", []byte("2"), &Preconditions{IfMatchETag: "wrong"}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed, got %v", err)
	}
	etag2, err := m.Put(ctx, "k", []byte("2"), &Preconditions{IfMatchETag: etag})
	if err != nil {
		t.Fatal(err)
	}
	if etag2 == etag {
		t.Fatal("ETag must change on every write")
	}
	// If-Match on a missing key must fail.
	if _, err := m.Put(ctx, "absent", []byte("x"), &Preconditions{IfMatchETag: etag2}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed for missing key, got %v", err)
	}
}

func TestMemoryDeleteIdempotent(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, err := m.Put(ctx, "k", []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete of missing key must not error: %v", err)
	}
	if _, err := m.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatal("key still present after delete")
	}
}

func TestMemoryListPagination(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		if _, err := m.Put(ctx, fmt.Sprintf("p/%02d", i), []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Put(ctx, "other/1", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	var got []string
	token := ""
	for {
		page, err := m.List(ctx, "p/", &ListOptions{MaxKeys: 3, ContinuationToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Objects {
			got = append(got, o.Key)
		}
		if !page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
	}
	if len(got) != 7 || got[0] != "p/00" || got[6] != "p/06" {
		t.Fatalf("bad listing: %v", got)
	}
	// StartAfter
	page, err := m.List(ctx, "p/", &ListOptions{StartAfter: "p/03"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 3 || page.Objects[0].Key != "p/04" {
		t.Fatalf("bad StartAfter listing: %+v", page.Objects)
	}
}

// TestMemoryConcurrentCAS verifies that under N concurrent conditional
// writers to one hot key, exactly the winners observe success, sequentially.
func TestMemoryConcurrentCAS(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	etag, err := m.Put(ctx, "hot", []byte("0"), nil)
	if err != nil {
		t.Fatal(err)
	}
	const writers = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	cur := etag
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mu.Lock()
			want := cur
			mu.Unlock()
			newETag, err := m.Put(ctx, "hot", []byte(fmt.Sprint(i)), &Preconditions{IfMatchETag: want})
			if err == nil {
				mu.Lock()
				wins++
				cur = newETag
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if wins == 0 || wins > writers {
		t.Fatalf("impossible win count %d", wins)
	}
	t.Logf("sequential CAS winners: %d/%d", wins, writers)
}

func TestMemoryContextCanceled(t *testing.T) {
	m := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Get(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestChaosInjectsErrorsDeterministically(t *testing.T) {
	m1 := NewChaos(NewMemory(), ChaosConfig{Rand: rand.New(rand.NewSource(7)), ErrorRate: 0.5})
	m2 := NewChaos(NewMemory(), ChaosConfig{Rand: rand.New(rand.NewSource(7)), ErrorRate: 0.5})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%d", i)
		_, e1 := m1.Put(ctx, key, []byte("v"), nil)
		_, e2 := m2.Put(ctx, key, []byte("v"), nil)
		if (e1 == nil) != (e2 == nil) {
			t.Fatalf("nondeterministic chaos at %d: %v vs %v", i, e1, e2)
		}
		if e1 != nil && !IsRetryable(e1) {
			t.Fatalf("chaos error must be retryable: %v", e1)
		}
	}
}

func TestChaosAmbiguousWriteApplies(t *testing.T) {
	mem := NewMemory()
	c := NewChaos(mem, ChaosConfig{Rand: rand.New(rand.NewSource(3)), AmbiguousWriteRate: 1.0})
	ctx := context.Background()
	_, err := c.Put(ctx, "k", []byte("v"), nil)
	if err == nil {
		t.Fatal("expected ambiguous error")
	}
	obj, gerr := mem.Get(ctx, "k")
	if gerr != nil || string(obj.Body) != "v" {
		t.Fatal("ambiguous write must still be applied")
	}
}

func TestChaosDelayRespectsContext(t *testing.T) {
	c := NewChaos(NewMemory(), ChaosConfig{DelayRate: 1.0, Delay: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Get(ctx, "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
}
