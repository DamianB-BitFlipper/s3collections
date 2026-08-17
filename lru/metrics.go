package lru

import (
	"context"
	"errors"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/cas"
)

// operation names for metrics and logging.
type opName string

const (
	opGet    opName = "get"
	opSet    opName = "set"
	opTouch  opName = "touch"
	opDelete opName = "delete"
	opLen    opName = "len"
	opStats  opName = "stats"
	opEvict  opName = "evict"
)

// metric and label names.
const (
	metricLatency     = "s3collections_latency_seconds"
	metricConflicts   = "s3collections_conflicts_total"
	metricRetries     = "s3collections_retries_total"
	metricEntries     = "s3collections_lru_entries"
	metricBytes       = "s3collections_lru_bytes"
	metricEvictions   = "s3collections_lru_evictions_total"
	metricTouchWrites = "s3collections_lru_touch_writes_total"
	metricTouchSkips  = "s3collections_lru_touch_skips_total"
	metricListPages   = "s3collections_list_pages_total"
	metricCorrupt     = "s3collections_corrupt_total"

	componentValue = "lru"
	labelKind      = "kind"
	labelReason    = "reason"

	kindLive      = "live"
	kindTombstone = "tombstone"
)

// retryReason values for s3collections_retries_total.
const (
	retryReasonConflict = "conflict"
	retryReasonBackend  = "backend"
)

const (
	outcomeSuccess  = "success"
	outcomeError    = "error"
	outcomeConflict = "conflict"
)

func labels(kv ...string) []s3collections.Label {
	l := make([]s3collections.Label, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		l = append(l, s3collections.Label{Key: kv[i], Value: kv[i+1]})
	}
	return l
}

func (s *Store) observeLatency(ctx context.Context, op opName, start time.Time, outcome string) {
	d := time.Since(start).Seconds()
	s.opts.Meter.ObserveHistogram(ctx, metricLatency, d,
		labels("component", componentValue, "op", string(op), "outcome", outcome)...)
}

func (s *Store) recordConflict(ctx context.Context, op opName) {
	s.opts.Meter.IncCounter(ctx, metricConflicts, 1,
		labels("component", componentValue, "op", string(op))...)
}

func (s *Store) recordRetry(ctx context.Context, op opName, reason string) {
	s.opts.Meter.IncCounter(ctx, metricRetries, 1,
		labels("component", componentValue, "op", string(op), "reason", reason)...)
}

func (s *Store) incCounter(ctx context.Context, name string, delta float64, extra ...string) {
	s.opts.Meter.IncCounter(ctx, name, delta, append(labels("component", componentValue), labels(extra...)...)...)
}

func (s *Store) setGauge(ctx context.Context, name string, value float64, extra ...string) {
	s.opts.Meter.SetGauge(ctx, name, value, append(labels("component", componentValue), labels(extra...)...)...)
}

func outcomeFor(err error) string {
	if err == nil {
		return outcomeSuccess
	}
	if errors.Is(err, cas.ErrConflict) || errors.Is(err, cas.ErrAlreadyExists) {
		return outcomeConflict
	}
	return outcomeError
}
