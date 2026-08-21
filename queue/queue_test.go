package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/damianb/s3collections/storage"
)

// memKV is a simple in-memory storage.KV with copy-on-write transactions.
type memKV struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemKV() *memKV { return &memKV{data: map[string][]byte{}} }

func (m *memKV) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *memKV) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), value...)
	return nil
}

func (m *memKV) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memKV) Scan(_ context.Context, opts storage.ScanOptions) ([]storage.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var entries []storage.Entry
	for k, v := range m.data {
		if opts.Prefix != "" && !hasPrefix(k, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" && k <= opts.StartAfter {
			continue
		}
		entries = append(entries, storage.Entry{Key: k, Value: append([]byte(nil), v...)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	if opts.Reverse {
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
	if opts.Limit > 0 && len(entries) > opts.Limit {
		entries = entries[:opts.Limit]
	}
	return entries, nil
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func (m *memKV) Transaction(_ context.Context, fn func(storage.Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx := &memTx{data: make(map[string][]byte, len(m.data))}
	for k, v := range m.data {
		tx.data[k] = append([]byte(nil), v...)
	}
	if err := fn(tx); err != nil {
		return err
	}
	m.data = tx.data
	return nil
}

func (m *memKV) Close() error { return nil }

type memTx struct{ data map[string][]byte }

func (t *memTx) Get(key string) ([]byte, error) {
	v, ok := t.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (t *memTx) Put(key string, value []byte) error {
	t.data[key] = append([]byte(nil), value...)
	return nil
}

func (t *memTx) Delete(key string) error {
	delete(t.data, key)
	return nil
}

func (t *memTx) Scan(opts storage.ScanOptions) ([]storage.Entry, error) {
	var entries []storage.Entry
	for k, v := range t.data {
		if opts.Prefix != "" && !hasPrefix(k, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" && k <= opts.StartAfter {
			continue
		}
		entries = append(entries, storage.Entry{Key: k, Value: append([]byte(nil), v...)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	if opts.Reverse {
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
	if opts.Limit > 0 && len(entries) > opts.Limit {
		entries = entries[:opts.Limit]
	}
	return entries, nil
}

// memBlobs is an in-memory storage.BlobStore.
type memBlobs struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemBlobs() *memBlobs { return &memBlobs{data: map[string][]byte{}} }

func (b *memBlobs) Put(_ context.Context, key string, r io.Reader, size int64) error {
	buf, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if size >= 0 && int64(len(buf)) != size {
		return storage.ErrSizeMismatch
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = buf
	return nil
}

func (b *memBlobs) Open(_ context.Context, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(v)), nil
}

func (b *memBlobs) OpenRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	if end > int64(len(v)) {
		end = int64(len(v))
	}
	return io.NopCloser(bytes.NewReader(v[start:end])), nil
}

func (b *memBlobs) Stat(_ context.Context, key string) (storage.BlobInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.data[key]
	if !ok {
		return storage.BlobInfo{}, storage.ErrNotFound
	}
	return storage.BlobInfo{Key: key, Size: int64(len(v))}, nil
}

func (b *memBlobs) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, key)
	return nil
}

func (b *memBlobs) Close() error { return nil }

func newTestQueue(t *testing.T, opts ...Option) (*Queue, *memKV, *memBlobs) {
	t.Helper()
	kv := newMemKV()
	blobs := newMemBlobs()
	q := New(kv, blobs, "test", opts...)
	t.Cleanup(func() { _ = q.Close() })
	return q, kv, blobs
}

func TestEnqueueClaimComplete(t *testing.T) {
	q, _, blobs := newTestQueue(t)
	ctx := context.Background()

	id, created, err := q.Enqueue(ctx, []byte("hello"), EnqueueOptions{})
	if err != nil || !created {
		t.Fatalf("Enqueue: id=%q created=%v err=%v", id, created, err)
	}

	// Idempotent enqueue with explicit ID.
	_, created, err = q.Enqueue(ctx, []byte("other"), EnqueueOptions{JobID: id})
	if err != nil {
		t.Fatalf("Enqueue dup: %v", err)
	}
	if created {
		t.Fatal("expected created=false for duplicate JobID")
	}

	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job.ID() != id {
		t.Fatalf("claimed %q, want %q", job.ID(), id)
	}
	if job.Attempts() != 1 {
		t.Fatalf("attempts=%d, want 1", job.Attempts())
	}

	rc, err := job.OpenPayload(ctx)
	if err != nil {
		t.Fatalf("OpenPayload: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "hello" {
		t.Fatalf("payload=%q, want hello", data)
	}

	// Nothing else to claim.
	if _, err := q.Claim(ctx, ClaimOptions{}); !errors.Is(err, ErrNoJob) {
		t.Fatalf("Claim empty: %v", err)
	}

	payloadKey := job.meta.PayloadKey
	if err := job.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if _, err := blobs.Stat(ctx, payloadKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("payload should be deleted, stat err=%v", err)
	}
}

func TestEnqueueReader(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 1000)
	id, created, err := q.EnqueueReader(ctx, bytes.NewReader(payload), int64(len(payload)), EnqueueOptions{})
	if err != nil || !created {
		t.Fatalf("EnqueueReader: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	rc, _ := job.OpenPayload(ctx)
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}
	_ = id
}

func TestDelay(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	if _, _, err := q.Enqueue(ctx, []byte("later"), EnqueueOptions{Delay: time.Hour}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Claim(ctx, ClaimOptions{}); !errors.Is(err, ErrNoJob) {
		t.Fatalf("Claim before delay: %v", err)
	}
}

func TestRetryThenDead(t *testing.T) {
	q, _, _ := newTestQueue(t, WithMaxRetries(2))
	ctx := context.Background()
	id, _, err := q.Enqueue(ctx, []byte("work"), EnqueueOptions{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim 1: %v", err)
	}
	dead, err := job.Retry(ctx, 0)
	if err != nil || dead {
		t.Fatalf("Retry 1: dead=%v err=%v", dead, err)
	}

	job, err = q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim 2: %v", err)
	}
	if job.Attempts() != 2 {
		t.Fatalf("attempts=%d, want 2", job.Attempts())
	}
	dead, err = job.Retry(ctx, 0)
	if err != nil || !dead {
		t.Fatalf("Retry 2: dead=%v err=%v", dead, err)
	}

	ids, err := q.ListDead(ctx)
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("dead ids=%v, want [%q]", ids, id)
	}

	if err := q.RequeueDead(ctx, id); err != nil {
		t.Fatalf("RequeueDead: %v", err)
	}
	ids, _ = q.ListDead(ctx)
	if len(ids) != 0 {
		t.Fatalf("dead list not empty after requeue: %v", ids)
	}
	job, err = q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim after requeue: %v", err)
	}
	if job.Attempts() != 1 {
		t.Fatalf("attempts after requeue=%d, want 1", job.Attempts())
	}
}

func TestRenewAndLeaseExpiry(t *testing.T) {
	q, _, _ := newTestQueue(t, WithLeaseDuration(50*time.Millisecond), WithMaintenanceInterval(10*time.Millisecond))
	ctx := context.Background()

	if _, _, err := q.Enqueue(ctx, []byte("a"), EnqueueOptions{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := job.Renew(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	// Old lease expiry must not release a renewed job yet; wait for the
	// renewed lease to lapse, then maintenance should make it claimable.
	time.Sleep(200 * time.Millisecond)
	if _, err := q.Claim(ctx, ClaimOptions{}); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob before maintenance starts, got %v", err)
	}

	q.StartMaintenance(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for {
		job2, err := q.Claim(ctx, ClaimOptions{})
		if err == nil {
			if job2.Attempts() != 2 {
				t.Fatalf("attempts=%d, want 2", job2.Attempts())
			}
			break
		}
		if !errors.Is(err, ErrNoJob) {
			t.Fatalf("Claim: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for lease reaper")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDeadMethod(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	id, _, _ := q.Enqueue(ctx, []byte("x"), EnqueueOptions{})
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := job.Dead(ctx); err != nil {
		t.Fatalf("Dead: %v", err)
	}
	ids, _ := q.ListDead(ctx)
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("dead ids=%v", ids)
	}
	if err := q.RequeueDead(ctx, "missing"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("RequeueDead missing: %v", err)
	}
}

func TestClosedQueue(t *testing.T) {
	kv := newMemKV()
	blobs := newMemBlobs()
	q := New(kv, blobs, "t")
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := q.Enqueue(context.Background(), []byte("x"), EnqueueOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after close: %v", err)
	}
}

func TestExpiredLeaseCannotMutate(t *testing.T) {
	q, _, _ := newTestQueue(t, WithLeaseDuration(5*time.Millisecond))
	ctx := context.Background()
	if _, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := job.Renew(ctx, time.Second); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Renew expired: %v", err)
	}
	if err := job.Complete(ctx); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Complete expired: %v", err)
	}
	if _, err := job.Retry(ctx, 0); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Retry expired: %v", err)
	}
	if err := job.Dead(ctx); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Dead expired: %v", err)
	}
}

func TestConcurrentClaimSingleWinner(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	if _, _, err := q.Enqueue(ctx, []byte("x"), EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	const n = 24
	var wg sync.WaitGroup
	var winners int
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := q.Claim(ctx, ClaimOptions{})
			if err == nil {
				mu.Lock()
				winners++
				mu.Unlock()
				return
			}
			if !errors.Is(err, ErrNoJob) {
				t.Errorf("Claim: %v", err)
			}
		}()
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("winners=%d, want 1", winners)
	}
}

func TestConcurrentIdempotentEnqueueKeepsWinningPayload(t *testing.T) {
	ctx := context.Background()
	kv := storage.NewMemoryKV()
	blobs := storage.NewMemoryBlobStore()
	defer kv.Close()
	defer blobs.Close()
	q := New(kv, blobs, "idem")
	defer q.Close()
	const n = 24
	var wg sync.WaitGroup
	type result struct {
		created bool
		payload string
		err     error
	}
	out := make(chan result, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := fmt.Sprintf("payload-%02d", i)
			_, created, err := q.Enqueue(ctx, []byte(payload), EnqueueOptions{JobID: "one"})
			out <- result{created: created, payload: payload, err: err}
		}(i)
	}
	wg.Wait()
	close(out)
	winner := ""
	created := 0
	for r := range out {
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.created {
			created++
			winner = r.payload
		}
	}
	if created != 1 {
		t.Fatalf("created=%d", created)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rc, err := job.OpenPayload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != winner {
		t.Fatalf("payload=%q winner=%q", got, winner)
	}
}

func TestIdempotencyKeyReturnsCanonicalJob(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	id1, created, err := q.Enqueue(ctx, []byte("first"), EnqueueOptions{IdempotencyKey: "request"})
	if err != nil || !created {
		t.Fatalf("first: %s %v %v", id1, created, err)
	}
	id2, created, err := q.Enqueue(ctx, []byte("second"), EnqueueOptions{IdempotencyKey: "request"})
	if err != nil || created || id2 != id1 {
		t.Fatalf("second: %s %v %v", id2, created, err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rc, err := job.OpenPayload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "first" {
		t.Fatalf("payload=%q", got)
	}
}

func TestOpenPayloadDetectsCorruption(t *testing.T) {
	q, _, blobs := newTestQueue(t)
	ctx := context.Background()
	_, _, err := q.Enqueue(ctx, []byte("original"), EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	blobs.mu.Lock()
	blobs.data[job.meta.PayloadKey] = []byte("tampered")
	blobs.mu.Unlock()
	rc, err := job.OpenPayload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(rc)
	_ = rc.Close()
	if !errors.Is(err, ErrPayloadCorrupt) {
		t.Fatalf("read=%v", err)
	}
}
