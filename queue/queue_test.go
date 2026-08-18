package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
)

// fakeClock is a test clock that can be advanced deterministically.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func testQueue(t *testing.T, name string, opts ...Option) (*Queue, *s3backend.Memory, *fakeClock) {
	t.Helper()
	mem := s3backend.NewMemory()
	clk := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	base := []Option{func(o *Options) {
		o.now = clk.Now
		o.ReaperInterval = 100 * time.Millisecond
		o.GCInterval = time.Hour
		o.ClaimShardProbe = 65535
		o.WorkerID = "worker-" + t.Name()
	}}
	q, err := New(mem, name, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q, mem, clk
}

func TestEnqueueCreatesJobAndMarker(t *testing.T) {
	t.Parallel()
	q, mem, _ := testQueue(t, "enqueue")
	ctx := context.Background()
	payload := []byte("hello")

	jobID, existed, err := q.Enqueue(ctx, payload, EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if existed {
		t.Fatal("new job returned existed=true")
	}
	if len(jobID) == 0 {
		t.Fatal("empty job id")
	}

	shard := shardForJob(jobID, q.opts.Shards)
	appKey := jobAppKey(shard, jobID)
	rec, err := q.store.Get(ctx, appKey)
	if err != nil {
		logUnexpected(t, err)
		t.Fatalf("Get canonical job: %v", err)
	}
	env, err := decodeJob(rec.Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.State != statePending {
		t.Fatalf("state = %s, want pending", env.State)
	}
	gotPayload, _ := jobPayload(env)
	if string(gotPayload) != string(payload) {
		t.Fatalf("payload mismatch: %q vs %q", gotPayload, payload)
	}

	readyPrefix := q.prefix + "shard/" + shardHex(shard) + "/ready/"
	page, err := mem.List(ctx, readyPrefix, nil)
	if err != nil {
		t.Fatalf("List ready: %v", err)
	}
	if len(page.Objects) != 1 {
		t.Fatalf("ready markers = %d, want 1", len(page.Objects))
	}
}

func TestEnqueueDedup(t *testing.T) {
	t.Parallel()
	q, mem, _ := testQueue(t, "dedup")
	ctx := context.Background()

	idemKey := "order-42"
	jobID1, existed1, err := q.Enqueue(ctx, []byte("a"), EnqueueOptions{IdempotencyKey: idemKey})
	if err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	if existed1 {
		t.Fatal("first enqueue returned existed=true")
	}

	jobID2, existed2, err := q.Enqueue(ctx, []byte("b"), EnqueueOptions{IdempotencyKey: idemKey})
	if err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}
	if !existed2 {
		t.Fatal("second enqueue returned existed=false")
	}
	if jobID1 != jobID2 {
		t.Fatalf("job ids differ: %q vs %q", jobID1, jobID2)
	}

	// Only one canonical object should exist.
	count := 0
	page, err := mem.List(ctx, q.prefix, nil)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, info := range page.Objects {
		if _, kind, _, _, ok := q.parseMarker(info.Key); ok && kind == "ready" {
			continue
		}
		count++
	}
	if count != 1 {
		t.Fatalf("canonical object count = %d, want 1", count)
	}
}

func TestEnqueueDelay(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "delay")
	ctx := context.Background()

	_, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{Delay: time.Hour})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Claim(ctx, ClaimOptions{}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Claim before delay: got %v, want ErrEmpty", err)
	}
	clk.Advance(2 * time.Hour)
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim after delay: %v", err)
	}
	if job == nil {
		t.Fatal("nil job")
	}
}

func TestClaimSingle(t *testing.T) {
	t.Parallel()
	q, mem, _ := testQueue(t, "claim-single")
	ctx := context.Background()
	payload := []byte("payload")

	jobID, _, err := q.Enqueue(ctx, payload, EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job.ID != jobID {
		t.Fatalf("job id mismatch: %q vs %q", job.ID, jobID)
	}
	if job.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", job.Attempts)
	}
	if job.Fence == 0 {
		t.Fatal("fence is zero")
	}
	if job.Lease.Owner != q.opts.WorkerID {
		t.Fatalf("lease owner = %q, want %q", job.Lease.Owner, q.opts.WorkerID)
	}
	if string(job.Payload) != string(payload) {
		t.Fatalf("payload mismatch")
	}

	shard := shardForJob(jobID, q.opts.Shards)
	readyPrefix := q.prefix + "shard/" + shardHex(shard) + "/ready/"
	page, err := mem.List(ctx, readyPrefix, nil)
	if err != nil {
		t.Fatalf("List ready: %v", err)
	}
	if len(page.Objects) != 0 {
		t.Fatalf("ready markers after claim = %d, want 0", len(page.Objects))
	}
	leasePrefix := q.prefix + "shard/" + shardHex(shard) + "/lease/"
	page, err = mem.List(ctx, leasePrefix, nil)
	if err != nil {
		t.Fatalf("List lease: %v", err)
	}
	if len(page.Objects) != 1 {
		t.Fatalf("lease markers after claim = %d, want 1", len(page.Objects))
	}
}

func TestClaimContention(t *testing.T) {
	t.Parallel()
	q, _, _ := testQueue(t, "claim-contention", func(o *Options) {
		o.Shards = 8
	})
	ctx := context.Background()
	n := 100
	for i := range n {
		_, _, err := q.Enqueue(ctx, []byte(fmt.Sprintf("job-%d", i)), EnqueueOptions{})
		if err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	const workers = 16
	var claimed atomic.Int64
	var dupChecks sync.Map
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, err := q.Claim(ctx, ClaimOptions{})
				if errors.Is(err, ErrEmpty) {
					return
				}
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if _, loaded := dupChecks.LoadOrStore(job.ID, true); loaded {
					t.Errorf("duplicate claim of %s", job.ID)
				}
				claimed.Add(1)
				if err := job.Complete(ctx); err != nil {
					t.Errorf("Complete: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if int(claimed.Load()) != n {
		t.Fatalf("claimed = %d, want %d", claimed.Load(), n)
	}
}

func TestRenewAndSteal(t *testing.T) {
	t.Parallel()
	q, mem, clk := testQueue(t, "renew-steal")
	ctx := context.Background()

	_, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	originalExpiry := job.Lease.Expiry
	if err := job.Renew(ctx, 30*time.Second); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !job.Lease.Expiry.After(originalExpiry) {
		t.Fatalf("lease not extended: %v vs %v", job.Lease.Expiry, originalExpiry)
	}

	// Another worker steals after the renewed lease expires.
	clk.Advance(65 * time.Second)
	q.reaperPass(ctx)

	q2, err := New(mem, "renew-steal", func(o *Options) {
		o.now = clk.Now
		o.WorkerID = "other-worker"
		o.ClaimShardProbe = 65535
		o.ReaperInterval = 100 * time.Millisecond
		o.GCInterval = time.Hour
	})
	if err != nil {
		t.Fatalf("New q2: %v", err)
	}
	job2, err := q2.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("steal Claim: %v", err)
	}
	if job2.ID != job.ID {
		t.Fatalf("stolen job id mismatch")
	}

	if err := job.Renew(ctx, time.Second); !(errors.Is(err, ErrStaleLease) || errors.Is(err, ErrNotLeased)) {
		t.Fatalf("Renew after steal: got %v, want ErrStaleLease or ErrNotLeased", err)
	}
}

func TestCompleteIdempotent(t *testing.T) {
	t.Parallel()
	q, _, _ := testQueue(t, "complete-idempotent")
	ctx := context.Background()

	_, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := job.Complete(ctx); err != nil {
		t.Fatalf("Complete 1: %v", err)
	}
	if err := job.Complete(ctx); err != nil {
		t.Fatalf("Complete 2: %v", err)
	}
	rec, err := q.store.GetMeta(ctx, job.appKey)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	env, err := decodeJob(rec.Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.State != stateCompleted {
		t.Fatalf("state = %s, want completed", env.State)
	}
}

func TestRetryBackoffMaxAttempts(t *testing.T) {
	t.Parallel()
	q, _, _ := testQueue(t, "retry-max", func(o *Options) {
		o.MaxAttempts = 3
	})
	ctx := context.Background()

	jobID, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First claim + retry => pending.
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim 1: %v", err)
	}
	if err := job.Retry(ctx, RetryOptions{Backoff: time.Millisecond, Reason: "r1"}); err != nil {
		t.Fatalf("Retry 1: %v", err)
	}
	job, err = q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim 2: %v", err)
	}
	if job.Attempts != 2 {
		t.Fatalf("attempts after 2nd claim = %d, want 2", job.Attempts)
	}
	if err := job.Retry(ctx, RetryOptions{Backoff: time.Millisecond, Reason: "r2"}); err != nil {
		t.Fatalf("Retry 2: %v", err)
	}

	// Second retry should dead-letter because the next claim would exceed MaxAttempts.
	rec, err := q.store.GetMeta(ctx, jobAppKey(job.Shard, jobID))
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	env, err := decodeJob(rec.Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.State != stateDead {
		t.Fatalf("state = %s, want dead", env.State)
	}
	if len(env.Reasons) == 0 {
		t.Fatal("no reasons recorded")
	}
	if env.Reasons[len(env.Reasons)-1].Reason != "r2" {
		t.Fatalf("last reason = %q, want r2", env.Reasons[len(env.Reasons)-1].Reason)
	}

	dead, _, err := q.ListDead(ctx, ListDeadOptions{Shards: []uint16{job.Shard}})
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != jobID {
		t.Fatalf("ListDead = %+v, want job %s", dead, jobID)
	}
}

func TestListDeadAndRequeue(t *testing.T) {
	t.Parallel()
	q, _, _ := testQueue(t, "dead-requeue")
	ctx := context.Background()

	jobID, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := job.Dead(ctx, "boom"); err != nil {
		t.Fatalf("Dead: %v", err)
	}

	dead, _, err := q.ListDead(ctx, ListDeadOptions{Shards: []uint16{job.Shard}})
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != jobID || dead[0].Reason != "boom" {
		t.Fatalf("ListDead = %+v", dead)
	}

	if err := q.RequeueDead(ctx, jobID, job.Shard); err != nil {
		t.Fatalf("RequeueDead: %v", err)
	}
	job2, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim after requeue: %v", err)
	}
	if job2.ID != jobID {
		t.Fatalf("requeued job id mismatch")
	}
	if err := job2.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}
}

func TestReaperReclaimsExpiredLease(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "reaper", func(o *Options) {
		o.ReaperInterval = 50 * time.Millisecond
		o.GCInterval = time.Hour
	})
	ctx := context.Background()

	_, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: time.Second})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	clk.Advance(2 * time.Second)
	q.reaperPass(ctx)

	j, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim after reap: %v", err)
	}
	if j.ID != job.ID {
		t.Fatal("reaper did not reclaim expired lease")
	}
}

func TestGCRetention(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "gc-retention", func(o *Options) {
		o.CompletedRetention = time.Second
		o.DeadRetention = time.Second
		o.ReaperInterval = time.Hour
		o.GCInterval = 50 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobID, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := job.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	clk.Advance(3 * time.Second)
	q.gcPass(ctx)

	appKey := jobAppKey(job.Shard, jobID)
	rec, err := q.store.GetMeta(ctx, appKey)
	if err != nil {
		t.Fatalf("GetMeta after GC: %v", err)
	}
	if rec.State != cas.Tombstone {
		t.Fatalf("GC did not tombstone completed job: state=%d", rec.State)
	}
}

func TestAtLeastOnceStress(t *testing.T) {
	t.Parallel()
	mem := s3backend.NewMemory()
	chaos := s3backend.NewChaos(mem, s3backend.ChaosConfig{
		Rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
		ErrorRate: 0.02,
		DelayRate: 0.02,
		Delay:     2 * time.Millisecond,
	})
	clk := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	q, err := New(chaos, "stress", func(o *Options) {
		o.now = clk.Now
		o.Shards = 16
		o.ClaimShardProbe = 16
		o.ReaperInterval = 50 * time.Millisecond
		o.GCInterval = time.Hour
		o.WorkerID = "stress-worker"
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	q.StartMaintenance(ctx)

	const nJobs = 1000
	for i := range nJobs {
		var jobID string
		var enqueueErr error
		for attempt := 0; attempt < 10; attempt++ {
			var existed bool
			jobID, existed, enqueueErr = q.Enqueue(ctx, []byte(fmt.Sprintf("payload-%d", i)), EnqueueOptions{})
			if enqueueErr == nil || existed {
				enqueueErr = nil
				break
			}
			time.Sleep(time.Millisecond)
		}
		if enqueueErr != nil {
			t.Fatalf("Enqueue %s: %v", jobID, enqueueErr)
		}
	}

	var claims atomic.Int64
	var completed sync.Map
	var completedCount atomic.Int64
	var wg sync.WaitGroup
	const workers = 32
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if completedCount.Load() >= int64(nJobs) {
					return
				}
				job, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: 10 * time.Second})
				if errors.Is(err, ErrEmpty) {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Logf("Claim error: %v", err)
					time.Sleep(5 * time.Millisecond)
					continue
				}
				claims.Add(1)
				if err := job.Complete(ctx); err != nil {
					t.Logf("Complete error: %v", err)
					continue
				}
				if _, loaded := completed.LoadOrStore(job.ID, true); !loaded {
					completedCount.Add(1)
				}
			}
		}()
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if completedCount.Load() >= int64(nJobs) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if claims.Load() < nJobs {
		t.Fatalf("claims = %d, want >= %d", claims.Load(), nJobs)
	}
	unique := 0
	completed.Range(func(_, _ any) bool {
		unique++
		return true
	})
	if unique != nJobs {
		t.Fatalf("unique completed = %d, want %d", unique, nJobs)
	}
}

func TestFenceMonotonicity(t *testing.T) {
	t.Parallel()
	q, _, _ := testQueue(t, "fence")
	ctx := context.Background()

	const n = 100
	for i := range n {
		_, _, err := q.Enqueue(ctx, []byte(fmt.Sprintf("p-%d", i)), EnqueueOptions{})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Simulate a downstream effect store keyed by job ID with Guard.
	effects := make(map[string]uint64)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: time.Minute})
				if errors.Is(err, ErrEmpty) {
					return
				}
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				mu.Lock()
				latest := effects[job.ID]
				if err := Guard(latest, job.Fence); err != nil {
					mu.Unlock()
					// Stale delivery; complete without side effect.
					_ = job.Complete(ctx)
					continue
				}
				effects[job.ID] = job.Fence
				mu.Unlock()
				if err := job.Complete(ctx); err != nil {
					t.Errorf("Complete: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(effects) != n {
		t.Fatalf("effects = %d, want %d", len(effects), n)
	}
}

func logUnexpected(t *testing.T, err error) {
	t.Helper()
	var nfe *cas.NotFoundError
	if errors.As(err, &nfe) {
		t.Logf("NotFoundError: key=%q tombstoned=%v", nfe.Key, nfe.Tombstoned)
	}
}

func TestSequencerEnabled(t *testing.T) {
	t.Parallel()
	q, _, _ := testQueue(t, "seq", func(o *Options) {
		o.SequencerEnabled = true
		o.Shards = 16
	})
	ctx := context.Background()
	ids := make([]string, 5)
	for i := range ids {
		id, _, err := q.Enqueue(ctx, []byte(fmt.Sprintf("p-%d", i)), EnqueueOptions{})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids[i] = id
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ids not increasing: %q vs %q", ids[i-1], ids[i])
		}
	}
}

func TestRenewCreatesLeaseMarkerAndFenceAdvances(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "renew-marker")
	ctx := context.Background()

	_, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	fenceAfterClaim := job.Fence
	originalExpiry := job.Lease.Expiry

	if err := job.Renew(ctx, 30*time.Second); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if job.Fence <= fenceAfterClaim {
		t.Fatalf("fence did not advance across renew: %d vs %d", job.Fence, fenceAfterClaim)
	}
	if !job.Lease.Expiry.After(originalExpiry) {
		t.Fatalf("lease not extended: %v vs %v", job.Lease.Expiry, originalExpiry)
	}

	// Original expiry passes; reaper must NOT reclaim the job.
	clk.Advance(10 * time.Second)
	q.reaperPass(ctx)
	if _, err := q.Claim(ctx, ClaimOptions{}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty after original expiry, got %v", err)
	}

	// Renewed expiry passes; reaper MUST reclaim the job.
	clk.Advance(35 * time.Second)
	q.reaperPass(ctx)
	job2, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim after renewed expiry: %v", err)
	}
	if job2.ID != job.ID {
		t.Fatalf("reaper reclaimed wrong job: %s vs %s", job2.ID, job.ID)
	}
}

func TestReaperBackfillsDeadMarker(t *testing.T) {
	t.Parallel()
	q, mem, clk := testQueue(t, "backfill-dead")
	ctx := context.Background()

	now := clk.Now()
	jobID := "dead-job-1"
	shard := uint16(0)
	env := newJobEnvelope(jobID, q.name, shard, []byte("payload"), now, now)
	env.State = stateDead
	env.Dead = &deadEnvelope{Reason: "crash", At: now}
	body, err := encodeJob(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := q.store.Create(ctx, jobAppKey(shard, jobID), body); err != nil {
		t.Fatalf("Create canonical dead job: %v", err)
	}

	deadPrefix := q.prefix + "shard/" + shardHex(shard) + "/dead/"
	page, err := mem.List(ctx, deadPrefix, nil)
	if err != nil {
		t.Fatalf("List dead markers: %v", err)
	}
	if len(page.Objects) != 0 {
		t.Fatalf("unexpected dead marker before reaper: %d", len(page.Objects))
	}

	q.reaperPass(ctx)

	page, err = mem.List(ctx, deadPrefix, nil)
	if err != nil {
		t.Fatalf("List dead markers after reaper: %v", err)
	}
	if len(page.Objects) != 1 {
		t.Fatalf("dead markers after reaper = %d, want 1", len(page.Objects))
	}
}

func TestListDeadDenseShardPagination(t *testing.T) {
	t.Parallel()
	q, _, clk := testQueue(t, "dead-dense", func(o *Options) {
		o.Shards = 1
	})
	ctx := context.Background()

	const total = 1500
	const limit = 1000
	shard := uint16(0)
	base := clk.Now()
	for i := 0; i < total; i++ {
		jobID := fmt.Sprintf("dead-%04d", i)
		when := base.Add(time.Duration(i) * time.Microsecond)
		env := newJobEnvelope(jobID, q.name, shard, []byte("p"), base, base)
		env.State = stateDead
		env.Dead = &deadEnvelope{Reason: "boom", At: when}
		body, err := encodeJob(env)
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		if _, err := q.store.Create(ctx, jobAppKey(shard, jobID), body); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		if err := q.createMarker(ctx, q.deadMarkerKey(shard, when, jobID)); err != nil {
			t.Fatalf("dead marker %d: %v", i, err)
		}
	}

	items, cursor, err := q.ListDead(ctx, ListDeadOptions{Shards: []uint16{shard}, Limit: limit})
	if err != nil {
		t.Fatalf("ListDead first page: %v", err)
	}
	if len(items) != limit {
		t.Fatalf("first page len = %d, want %d", len(items), limit)
	}
	if cursor == "" {
		t.Fatal("expected non-empty cursor after first page")
	}
	for i := 1; i < len(items); i++ {
		if items[i].When.Before(items[i-1].When) {
			t.Fatalf("first page not sorted at %d", i)
		}
	}

	items2, cursor2, err := q.ListDead(ctx, ListDeadOptions{
		Shards:     []uint16{shard},
		Limit:      limit,
		StartAfter: cursor,
	})
	if err != nil {
		t.Fatalf("ListDead second page: %v", err)
	}
	if len(items2) != total-limit {
		t.Fatalf("second page len = %d, want %d", len(items2), total-limit)
	}
	// The second page is short but non-empty, so its cursor is non-empty
	// (terminal form); replaying it must yield an empty page and "".
	if cursor2 == "" {
		t.Fatal("expected non-empty cursor after second page")
	}
	items3, cursor3, err := q.ListDead(ctx, ListDeadOptions{
		Shards:     []uint16{shard},
		Limit:      limit,
		StartAfter: cursor2,
	})
	if err != nil {
		t.Fatalf("ListDead third page: %v", err)
	}
	if len(items3) != 0 || cursor3 != "" {
		t.Fatalf("third page = %d items, cursor %q; want 0 items, empty cursor", len(items3), cursor3)
	}
	if items2[0].When.Before(items[limit-1].When) {
		t.Fatal("second page starts before first page ended")
	}
}

// flakyPutBackend fails the first N Put calls for marker keys with a retryable
// error, then delegates to the wrapped backend.
type flakyPutBackend struct {
	s3backend.Backend
	failFirstN int32
	count      atomic.Int32
}

func (f *flakyPutBackend) Put(ctx context.Context, key string, body []byte, pre *s3backend.Preconditions) (string, error) {
	if strings.Contains(key, "/ready/") && f.count.Add(1) <= f.failFirstN {
		return "", &s3backend.Error{
			Op:         "Put",
			Key:        key,
			StatusCode: 503,
			Code:       "SlowDown",
			Message:    "simulated retryable error",
			Retryable:  true,
		}
	}
	return f.Backend.Put(ctx, key, body, pre)
}

func TestPutMarkerRetryMetric(t *testing.T) {
	t.Parallel()
	meter := s3collections.NewCaptureMeter()
	base := s3backend.NewMemory()
	flaky := &flakyPutBackend{Backend: base, failFirstN: 1}
	q, err := New(flaky, "metrics-retry",
		WithMeter(meter),
		func(o *Options) {
			o.Retry = s3collections.RetryPolicy{
				MaxAttempts: 3,
				Base:        time.Millisecond,
				Max:         time.Millisecond,
				Jitter:      0,
			}
		},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if _, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got := meter.Counter("s3collections_retries_total",
		s3collections.L("component", "queue"),
		s3collections.L("op", "put_marker"),
		s3collections.L("reason", "backend")); got != 1 {
		t.Fatalf("put_marker retries = %v, want 1", got)
	}
}

func TestClaimConflictMetric(t *testing.T) {
	t.Parallel()
	meter := s3collections.NewCaptureMeter()
	q, _, _ := testQueue(t, "metrics-conflict", WithMeter(meter), func(o *Options) {
		o.Shards = 1
		o.ClaimShardProbe = 1
	})
	ctx := context.Background()

	if _, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A claim conflict requires two workers to lose the CAS race for the
	// same job; whether a late worker observes state=claimed (skip, no
	// conflict) or overlaps on the CAS (conflict) is timing-dependent, so
	// run bounded rounds of races until a conflict is observed.
	const rounds = 50
	for r := 0; r < rounds; r++ {
		if _, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		barrier := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-barrier
				job, err := q.Claim(ctx, ClaimOptions{})
				if err == nil {
					_ = job.Complete(ctx)
				}
			}()
		}
		close(barrier)
		wg.Wait()
		if got := meter.CounterSum("s3collections_conflicts_total"); got >= 1 {
			return
		}
	}
	t.Fatalf("no claim conflict observed in %d rounds", rounds)
}

func TestListDeadEmitsListPagesMetric(t *testing.T) {
	t.Parallel()
	meter := s3collections.NewCaptureMeter()
	q, _, _ := testQueue(t, "metrics-list", WithMeter(meter))
	ctx := context.Background()

	_, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := job.Dead(ctx, "boom"); err != nil {
		t.Fatalf("Dead: %v", err)
	}
	if _, _, err := q.ListDead(ctx, ListDeadOptions{Shards: []uint16{job.Shard}}); err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if got := meter.Counter("s3collections_list_pages_total",
		s3collections.L("component", "queue"),
		s3collections.L("op", "list_dead"),
		s3collections.L("prefix", "queue/<name>/shard/<hhhh>/dead/")); got < 1 {
		t.Fatalf("list_pages_total{list_dead} = %v", got)
	}
}
