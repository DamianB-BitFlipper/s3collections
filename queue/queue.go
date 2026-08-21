// Package queue implements a durable FIFO work queue on top of the
// storage abstraction: JSON metadata in the KV store (mutated inside
// serializable transactions) and payloads in the BlobStore.
package queue

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/damianb/s3collections/storage"
)

// Job states stored in the metadata record.
const (
	statePending   = "pending"
	stateClaimed   = "claimed"
	stateDead      = "dead"
	stateCompleted = "completed"
)

// Errors returned by the queue.
var (
	// ErrNoJob is returned by Claim when no job is currently available.
	ErrNoJob = errors.New("queue: no job available")
	// ErrJobNotFound is returned when a job ID does not exist.
	ErrJobNotFound = errors.New("queue: job not found")
	// ErrNotClaimed is returned when an operation requires an active lease.
	ErrNotClaimed = errors.New("queue: job not claimed")
	// ErrNotDead is returned when RequeueDead targets a non-dead job.
	ErrNotDead = errors.New("queue: job is not dead")
	// ErrPayloadCorrupt is returned when stored payload bytes fail size/hash verification.
	ErrPayloadCorrupt = errors.New("queue: corrupt payload")
	// ErrLeaseExpired is returned when a stale holder acts after expiry.
	ErrLeaseExpired = errors.New("queue: lease expired")
	// ErrClosed is returned when the queue has been shut down.
	ErrClosed = errors.New("queue: closed")
)

// EnqueueOptions controls how a job is enqueued.
type EnqueueOptions struct {
	// JobID, when non-empty, is used as the job identifier. If a job with
	// the same ID already exists, Enqueue returns created=false and no
	// error (idempotent enqueue).
	JobID string
	// IdempotencyKey deterministically identifies a job within this queue.
	// JobID takes precedence when both are set.
	IdempotencyKey string
	// Delay makes the job invisible to Claim until it elapses.
	Delay time.Duration
	// MaxRetries overrides the queue default for this job. <= 0 uses the
	// queue default.
	MaxRetries int
}

// ClaimOptions controls a Claim call.
type ClaimOptions struct {
	// LeaseDuration overrides the queue default lease for this claim.
	// <= 0 uses the queue default.
	LeaseDuration time.Duration
}

// Option configures a Queue at construction.
type Option func(*Queue)

// WithLeaseDuration sets the default claim lease duration.
func WithLeaseDuration(d time.Duration) Option { return func(q *Queue) { q.lease = d } }

// WithMaxRetries sets the default maximum delivery attempts before a job
// is moved to the dead-letter state.
func WithMaxRetries(n int) Option { return func(q *Queue) { q.maxRetries = n } }

// WithMaintenanceInterval sets how often StartMaintenance reaps expired
// leases. <= 0 uses a default of one second.
func WithMaintenanceInterval(d time.Duration) Option {
	return func(q *Queue) { q.maintEvery = d }
}

// Queue is a durable FIFO queue backed by a KV store and a BlobStore.
type Queue struct {
	kv         storage.KV
	blobs      storage.BlobStore
	name       string
	lease      time.Duration
	maxRetries int
	maintEvery time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	closed bool
}

// New creates a queue named name on top of kv and blobs.
func New(kv storage.KV, blobs storage.BlobStore, name string, opts ...Option) *Queue {
	q := &Queue{
		kv:         kv,
		blobs:      blobs,
		name:       name,
		lease:      30 * time.Second,
		maxRetries: 3,
		maintEvery: time.Second,
	}
	for _, o := range opts {
		o(q)
	}
	if q.maintEvery <= 0 {
		q.maintEvery = time.Second
	}
	return q
}

func (q *Queue) namespace() string { return base64.RawURLEncoding.EncodeToString([]byte(q.name)) }
func (q *Queue) metaKey(id string) string {
	return "queue/" + q.namespace() + "/job/" + base64.RawURLEncoding.EncodeToString([]byte(id))
}
func (q *Queue) payloadKey(id string) string {
	return "queue/" + q.namespace() + "/payload/" + base64.RawURLEncoding.EncodeToString([]byte(id))
}
func (q *Queue) prefix() string { return "queue/" + q.namespace() + "/job/" }

// jobMeta is the JSON metadata record stored in the KV store.
type jobMeta struct {
	ID            string    `json:"id"`
	State         string    `json:"state"`
	PayloadKey    string    `json:"payload_key"`
	PayloadSize   int64     `json:"payload_size"`
	PayloadSHA256 string    `json:"payload_sha256"`
	Attempts      int       `json:"attempts"`
	MaxRetries    int       `json:"max_retries"`
	AvailableAt   time.Time `json:"available_at"`
	LeaseUntil    time.Time `json:"lease_until,omitempty"`
	LeaseToken    string    `json:"lease_token,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type countingWriter struct {
	writer io.Writer
	n      int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.n += int64(n)
	return n, err
}

type verifyingPayloadReader struct {
	rc      io.ReadCloser
	h       hash.Hash
	wantN   int64
	wantSum string
	n       int64
}

func (r *verifyingPayloadReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n > 0 {
		r.n += int64(n)
		_, _ = r.h.Write(p[:n])
	}
	if errors.Is(err, io.EOF) {
		if r.n != r.wantN || hex.EncodeToString(r.h.Sum(nil)) != r.wantSum {
			return n, ErrPayloadCorrupt
		}
	}
	return n, err
}
func (r *verifyingPayloadReader) Close() error { return r.rc.Close() }

func (q *Queue) transaction(ctx context.Context, fn func(storage.Tx) error) error {
	const maxAttempts = 16
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := q.kv.Transaction(ctx, fn)
		if !errors.Is(err, storage.ErrConflict) {
			return err
		}
	}
	return storage.ErrConflict
}

func (q *Queue) checkOpen() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	return nil
}

// Enqueue stores payload in the BlobStore and creates a pending job. It
// returns the job ID and whether the job was newly created.
func (q *Queue) Enqueue(ctx context.Context, payload []byte, opts EnqueueOptions) (string, bool, error) {
	return q.EnqueueReader(ctx, bytes.NewReader(payload), int64(len(payload)), opts)
}

// EnqueueReader streams the payload into the BlobStore and creates a
// pending job. size is the exact number of bytes r will yield, or < 0
// when unknown.
func (q *Queue) EnqueueReader(ctx context.Context, r io.Reader, size int64, opts EnqueueOptions) (string, bool, error) {
	if err := q.checkOpen(); err != nil {
		return "", false, err
	}
	id := opts.JobID
	if id == "" && opts.IdempotencyKey != "" {
		sum := sha256.Sum256([]byte(q.name + "\x00" + opts.IdempotencyKey))
		id = "idem-" + hex.EncodeToString(sum[:])
	}
	if id == "" {
		id = newID()
	}
	metaKey := q.metaKey(id)

	// Check for an existing job first for idempotent enqueue.
	if _, err := q.kv.Get(ctx, metaKey); err == nil {
		return id, false, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return "", false, err
	}

	payloadKey := q.payloadKey(id) + "/" + newID()
	digest := sha256.New()
	counter := &countingWriter{writer: digest}
	if err := q.blobs.Put(ctx, payloadKey, io.TeeReader(r, counter), size); err != nil {
		return "", false, fmt.Errorf("queue: put payload: %w", err)
	}

	now := time.Now().UTC()
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = q.maxRetries
	}
	meta := jobMeta{
		ID:            id,
		State:         statePending,
		PayloadKey:    payloadKey,
		PayloadSize:   counter.n,
		PayloadSHA256: hex.EncodeToString(digest.Sum(nil)),
		MaxRetries:    maxRetries,
		AvailableAt:   now.Add(opts.Delay),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return "", false, err
	}
	err = q.transaction(ctx, func(tx storage.Tx) error {
		if _, err := tx.Get(metaKey); err == nil {
			return errJobExists
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return tx.Put(metaKey, data)
	})
	if errors.Is(err, errJobExists) {
		_ = q.blobs.Delete(ctx, payloadKey)
		return id, false, nil
	}
	if err != nil {
		_ = q.blobs.Delete(ctx, payloadKey)
		return "", false, err
	}
	return id, true, nil
}

var errJobExists = errors.New("queue: job exists")

// Job is a claimed unit of work. Its methods act on the queue's storage.
type Job struct {
	q    *Queue
	meta jobMeta
}

// ID returns the job identifier.
func (j *Job) ID() string { return j.meta.ID }

// Attempts returns how many times the job has been claimed.
func (j *Job) Attempts() int { return j.meta.Attempts }

// Claim atomically takes the next available pending job and marks it
// claimed with a lease. It returns ErrNoJob when nothing is available.
func (q *Queue) Claim(ctx context.Context, opts ClaimOptions) (*Job, error) {
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	lease := opts.LeaseDuration
	if lease <= 0 {
		lease = q.lease
	}
	var claimed *jobMeta
	err := q.transaction(ctx, func(tx storage.Tx) error {
		claimed = nil
		now := time.Now().UTC()
		entries, err := tx.Scan(storage.ScanOptions{Prefix: q.prefix()})
		if err != nil {
			return err
		}
		var candidates []jobMeta
		for _, e := range entries {
			var m jobMeta
			if err := json.Unmarshal(e.Value, &m); err != nil {
				continue
			}
			if m.State == statePending && !m.AvailableAt.After(now) {
				candidates = append(candidates, m)
			}
		}
		if len(candidates) == 0 {
			return ErrNoJob
		}
		sort.Slice(candidates, func(i, j int) bool {
			if !candidates[i].AvailableAt.Equal(candidates[j].AvailableAt) {
				return candidates[i].AvailableAt.Before(candidates[j].AvailableAt)
			}
			if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
				return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
			}
			return candidates[i].ID < candidates[j].ID
		})
		m := candidates[0]
		m.State = stateClaimed
		m.Attempts++
		m.LeaseUntil = now.Add(lease)
		m.LeaseToken = newID()
		m.UpdatedAt = now
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if err := tx.Put(q.metaKey(m.ID), data); err != nil {
			return err
		}
		claimed = &m
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Job{q: q, meta: *claimed}, nil
}

// OpenPayload opens the job's payload blob for streaming.
func (j *Job) OpenPayload(ctx context.Context) (io.ReadCloser, error) {
	rc, err := j.q.blobs.Open(ctx, j.meta.PayloadKey)
	if err != nil {
		return nil, err
	}
	return &verifyingPayloadReader{rc: rc, h: sha256.New(), wantN: j.meta.PayloadSize, wantSum: j.meta.PayloadSHA256}, nil
}

// transition reloads the job metadata inside a transaction, checks that
// the job is still claimed by this holder (matching lease), and applies
// fn to the metadata.
func (j *Job) transition(ctx context.Context, fn func(*jobMeta) error) error {
	return j.q.transaction(ctx, func(tx storage.Tx) error {
		now := time.Now().UTC()
		raw, err := tx.Get(j.q.metaKey(j.meta.ID))
		if errors.Is(err, storage.ErrNotFound) {
			return ErrJobNotFound
		}
		if err != nil {
			return err
		}
		var m jobMeta
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if m.State != stateClaimed || m.LeaseToken == "" || m.LeaseToken != j.meta.LeaseToken {
			return ErrNotClaimed
		}
		if !m.LeaseUntil.After(now) {
			return ErrLeaseExpired
		}
		if err := fn(&m); err != nil {
			return err
		}
		m.UpdatedAt = now
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return tx.Put(j.q.metaKey(m.ID), data)
	})
}

// Renew extends the job's lease by d from now.
func (j *Job) Renew(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = j.q.lease
	}
	var newLease time.Time
	err := j.transition(ctx, func(m *jobMeta) error {
		newLease = time.Now().UTC().Add(d)
		m.LeaseUntil = newLease
		return nil
	})
	if err == nil {
		j.meta.LeaseUntil = newLease
	}
	return err
}

// Complete durably marks the job complete before deleting its payload. If the
// blob deletion fails, a retry with the same Job handle resumes cleanup; a
// crash never loses the payload key from metadata.
func (j *Job) Complete(ctx context.Context) error {
	err := j.q.transaction(ctx, func(tx storage.Tx) error {
		now := time.Now().UTC()
		raw, err := tx.Get(j.q.metaKey(j.meta.ID))
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var meta jobMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return err
		}
		if meta.State == stateCompleted && meta.LeaseToken == j.meta.LeaseToken {
			return nil
		}
		if meta.State != stateClaimed || meta.LeaseToken == "" || meta.LeaseToken != j.meta.LeaseToken {
			return ErrNotClaimed
		}
		if !meta.LeaseUntil.After(now) {
			return ErrLeaseExpired
		}
		meta.State = stateCompleted
		meta.LeaseUntil = time.Time{}
		meta.UpdatedAt = now
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return tx.Put(j.q.metaKey(meta.ID), data)
	})
	if err != nil {
		return err
	}
	if err := j.q.blobs.Delete(ctx, j.meta.PayloadKey); err != nil {
		return err
	}
	return j.q.transaction(ctx, func(tx storage.Tx) error {
		raw, err := tx.Get(j.q.metaKey(j.meta.ID))
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var meta jobMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return err
		}
		if meta.State != stateCompleted || meta.LeaseToken != j.meta.LeaseToken {
			return ErrNotClaimed
		}
		return tx.Delete(j.q.metaKey(meta.ID))
	})
}

// Retry returns the job to the pending state, making it available again
// after delay. If the job has exhausted its max retries it is moved to
// the dead-letter state instead and returns true.
func (j *Job) Retry(ctx context.Context, delay time.Duration) (dead bool, err error) {
	err = j.transition(ctx, func(m *jobMeta) error {
		now := time.Now().UTC()
		dead = false
		if m.Attempts >= m.MaxRetries {
			m.State = stateDead
			m.LeaseUntil = time.Time{}
			m.LeaseToken = ""
			dead = true
			return nil
		}
		m.State = statePending
		m.AvailableAt = now.Add(delay)
		m.LeaseUntil = time.Time{}
		m.LeaseToken = ""
		return nil
	})
	return dead, err
}

// Dead moves the job to the dead-letter state immediately.
func (j *Job) Dead(ctx context.Context) error {
	return j.transition(ctx, func(m *jobMeta) error {
		m.State = stateDead
		m.LeaseUntil = time.Time{}
		m.LeaseToken = ""
		return nil
	})
}

// ListDead returns the IDs of all jobs in the dead-letter state.
func (q *Queue) ListDead(ctx context.Context) ([]string, error) {
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	entries, err := q.kv.Scan(ctx, storage.ScanOptions{Prefix: q.prefix()})
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		var m jobMeta
		if err := json.Unmarshal(e.Value, &m); err != nil {
			continue
		}
		if m.State == stateDead {
			ids = append(ids, m.ID)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// RequeueDead moves a dead job back to the pending state with zero
// attempts. It returns ErrJobNotFound if the job does not exist.
func (q *Queue) RequeueDead(ctx context.Context, id string) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	now := time.Now().UTC()
	return q.transaction(ctx, func(tx storage.Tx) error {
		raw, err := tx.Get(q.metaKey(id))
		if errors.Is(err, storage.ErrNotFound) {
			return ErrJobNotFound
		}
		if err != nil {
			return err
		}
		var m jobMeta
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if m.State != stateDead {
			return ErrNotDead
		}
		m.State = statePending
		m.Attempts = 0
		m.AvailableAt = now
		m.LeaseUntil = time.Time{}
		m.UpdatedAt = now
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return tx.Put(q.metaKey(id), data)
	})
}

// StartMaintenance starts a background goroutine that periodically
// returns jobs whose leases have expired to the pending state. It stops
// when ctx is canceled or Close is called.
func (q *Queue) StartMaintenance(ctx context.Context) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	if q.cancel != nil {
		q.mu.Unlock()
		return
	}
	mctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel
	q.done = make(chan struct{})
	done := q.done
	q.mu.Unlock()

	go func() {
		defer close(done)
		t := time.NewTicker(q.maintEvery)
		defer t.Stop()
		for {
			select {
			case <-mctx.Done():
				return
			case <-t.C:
				_ = q.reapExpired(mctx)
			}
		}
	}()
}

func (q *Queue) reapExpired(ctx context.Context) error {
	var completed []jobMeta
	err := q.transaction(ctx, func(tx storage.Tx) error {
		completed = nil
		now := time.Now().UTC()
		entries, err := tx.Scan(storage.ScanOptions{Prefix: q.prefix()})
		if err != nil {
			return err
		}
		for _, e := range entries {
			var m jobMeta
			if err := json.Unmarshal(e.Value, &m); err != nil {
				continue
			}
			if m.State == stateCompleted {
				completed = append(completed, m)
				continue
			}
			if m.State != stateClaimed || m.LeaseUntil.IsZero() || m.LeaseUntil.After(now) {
				continue
			}
			if m.Attempts >= m.MaxRetries {
				m.State = stateDead
			} else {
				m.State = statePending
				m.AvailableAt = now
			}
			m.LeaseUntil = time.Time{}
			m.LeaseToken = ""
			m.UpdatedAt = now
			data, err := json.Marshal(m)
			if err != nil {
				return err
			}
			if err := tx.Put(q.metaKey(m.ID), data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, m := range completed {
		if err := q.blobs.Delete(ctx, m.PayloadKey); err != nil {
			continue
		}
		_ = q.transaction(ctx, func(tx storage.Tx) error {
			raw, err := tx.Get(q.metaKey(m.ID))
			if err != nil {
				return err
			}
			var current jobMeta
			if err := json.Unmarshal(raw, &current); err != nil {
				return err
			}
			if current.State != stateCompleted || current.LeaseToken != m.LeaseToken {
				return nil
			}
			return tx.Delete(q.metaKey(m.ID))
		})
	}
	return nil
}

// Close stops maintenance. It does not close the backing stores.
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	q.closed = true
	cancel := q.cancel
	done := q.done
	q.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	return nil
}
