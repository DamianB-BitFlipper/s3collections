package queue

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
)

// Queue is a durable, S3-backed at-least-once work queue. Canonical job
// state lives in a cas.Store; small marker objects guide LIST-based claim
// and reaper scans. A Queue value is safe for concurrent use.
type Queue struct {
	name   string
	prefix string
	opts   Options
	store  *cas.Store
	be     s3backend.Backend

	maintenanceMu sync.Mutex
	maintenanceOn atomic.Bool

	claimOffset atomic.Uint64
}

// New creates a Queue named name backed by b. The root storage prefix is
// "queue/<name>/". The returned Queue starts no background goroutines;
// callers should run StartMaintenance on each replica.
func New(b s3backend.Backend, name string, opts ...Option) (*Queue, error) {
	if b == nil {
		return nil, errors.New("queue: nil backend")
	}
	if name == "" {
		return nil, errors.New("queue: empty name")
	}
	var o Options
	for _, opt := range opts {
		opt(&o)
	}
	applyDefaults(&o)

	prefix := fmt.Sprintf("queue/%s/", name)
	store, err := cas.New(b, prefix,
		cas.WithWriterID(o.WorkerID),
		cas.WithMaxValueBytes(o.MaxPayloadBytes*2+8192), // envelope overhead
		cas.WithRetry(o.Retry),
		cas.WithMeter(o.Meter),
		cas.WithLogger(o.Logger),
		cas.WithTracer(o.Tracer),
		cas.WithTombstoneRetention(defaultTombstoneRetention),
	)
	if err != nil {
		return nil, fmt.Errorf("queue: create cas store: %w", err)
	}

	q := &Queue{
		name:   name,
		prefix: prefix,
		opts:   o,
		store:  store,
		be:     b,
	}
	return q, nil
}

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// now returns the configured clock.
func (q *Queue) now() time.Time { return q.opts.now() }

// observeLatency records a latency histogram sample for component="queue".
func (q *Queue) observeLatency(ctx context.Context, op, outcome string, d time.Duration) {
	q.opts.Meter.ObserveHistogram(ctx, metricLatency, d.Seconds(),
		s3collections.L("component", "queue"),
		s3collections.L("op", op),
		s3collections.L("outcome", outcome),
	)
}

// recordEvent increments the queue events counter.
func (q *Queue) recordEvent(ctx context.Context, event, outcome string) {
	q.opts.Meter.IncCounter(ctx, metricEvents, 1,
		s3collections.L("queue", q.name),
		s3collections.L("event", event),
		s3collections.L("outcome", outcome),
	)
}

// setDepth updates a queue depth gauge.
func (q *Queue) setDepth(ctx context.Context, kind string, value float64) {
	q.opts.Meter.SetGauge(ctx, metricDepth, value,
		s3collections.L("queue", q.name),
		s3collections.L("kind", kind),
	)
}

// recordReaperRun increments the reaper run counter.
func (q *Queue) recordReaperRun(ctx context.Context) {
	q.opts.Meter.IncCounter(ctx, "s3collections_reaper_runs_total", 1,
		s3collections.L("component", "queue"),
	)
}

// recordReaperDeleted increments the reaper deleted counter.
func (q *Queue) recordReaperDeleted(ctx context.Context, kind string) {
	q.opts.Meter.IncCounter(ctx, "s3collections_reaper_deleted_total", 1,
		s3collections.L("component", "queue"),
		s3collections.L("kind", kind),
	)
}

const (
	metricLatency = "s3collections_latency_seconds"
	metricEvents  = "s3collections_queue_events_total"
	metricDepth   = "s3collections_queue_depth"
)

// nextSequence increments the global sequencer and returns the new value.
// It is used only when Options.SequencerEnabled is true. The single-key
// CAS limits throughput to roughly tens of enqueues per second.
func (q *Queue) nextSequence(ctx context.Context) (uint64, error) {
	b1 := make([]byte, 8)
	binary.BigEndian.PutUint64(b1, 1)
	_, err := q.store.Create(ctx, "sequencer", b1)
	if err == nil {
		return 1, nil
	}
	if !errors.Is(err, cas.ErrAlreadyExists) {
		return 0, err
	}
	var seq uint64
	_, err = q.store.Update(ctx, "sequencer", func(ctx context.Context, cur cas.Record) ([]byte, error) {
		var n uint64
		if len(cur.Value) == 8 {
			n = binary.BigEndian.Uint64(cur.Value)
		}
		n++
		seq = n
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, n)
		return b, nil
	})
	return seq, err
}

// Static prefix templates used as label values for s3collections_list_pages_total.
// They intentionally do not contain queue names, shard values, or job IDs.
const (
	readyPrefixTemplate = "queue/<name>/shard/<hhhh>/ready/"
	leasePrefixTemplate = "queue/<name>/shard/<hhhh>/lease/"
	deadPrefixTemplate  = "queue/<name>/shard/<hhhh>/dead/"
	jobsPrefixTemplate  = "queue/<name>/shard/<hhhh>/jobs/"
)

// recordRetry increments the backend-retry counter.
func (q *Queue) recordRetry(ctx context.Context, op string) {
	q.opts.Meter.IncCounter(ctx, "s3collections_retries_total", 1,
		s3collections.L("component", "queue"),
		s3collections.L("op", op),
		s3collections.L("reason", "backend"),
	)
}

// recordConflict increments the CAS-conflict counter for an operation.
func (q *Queue) recordConflict(ctx context.Context, op string) {
	q.opts.Meter.IncCounter(ctx, "s3collections_conflicts_total", 1,
		s3collections.L("component", "queue"),
		s3collections.L("op", op),
	)
}

// recordListPage increments the list-pages counter.
func (q *Queue) recordListPage(ctx context.Context, op, prefixTemplate string) {
	q.opts.Meter.IncCounter(ctx, "s3collections_list_pages_total", 1,
		s3collections.L("component", "queue"),
		s3collections.L("op", op),
		s3collections.L("prefix", prefixTemplate),
	)
}

// outcome labels.
const (
	outcomeSuccess  = "success"
	outcomeError    = "error"
	outcomeEmpty    = "empty"
	outcomeConflict = "conflict"
	outcomeDead     = "dead"
)
