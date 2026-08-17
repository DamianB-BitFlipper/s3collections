package cas

import (
	"context"
	"errors"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

// operation names for metrics/logging.
type opName string

const (
	opCreate opName = "create"
	opGet    opName = "get"
	opCAS    opName = "cas"
	opUpdate opName = "update"
	opDelete opName = "delete"
	opList   opName = "list"
	opGC     opName = "gc"
)

// Retry applies the given retry policy around op, retrying on transient backend errors.
func Retry(ctx context.Context, policy s3collections.RetryPolicy, op func(context.Context) error) error {
	policy = policy.WithDefaults()
	nextDelay := s3collections.BackoffDelays(policy, nil)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		err := op(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s3backend.IsRetryable(err) {
			return err
		}
		if attempt == policy.MaxAttempts {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextDelay()):
		}
	}
	return nil
}

// classifyError maps an s3backend error to the appropriate cas error.
func classifyError(op opName, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, s3backend.ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, s3backend.ErrPreconditionFailed) {
		if op == opCreate {
			return ErrAlreadyExists
		}
		return ErrConflict
	}
	return err
}

// outcome label values.
const (
	outcomeSuccess  = "success"
	outcomeError    = "error"
	outcomeConflict = "conflict"
)

// metric names.
const (
	metricLatency   = "s3collections_latency_seconds"
	metricAttempts  = "s3collections_cas_attempts"
	metricConflicts = "s3collections_conflicts_total"
	metricRetries   = "s3collections_retries_total"
	metricListPages = "s3collections_list_pages_total"
	metricCorrupt   = "s3collections_corrupt_total"
	componentValue  = "cas"
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
	s.opts.Meter.ObserveHistogram(ctx, metricLatency, d, labels("component", componentValue, "op", string(op), "outcome", outcome)...)
}

func (s *Store) recordAttempts(ctx context.Context, op opName, attempts int) {
	s.opts.Meter.ObserveHistogram(ctx, metricAttempts, float64(attempts), labels("component", componentValue, "op", string(op))...)
}

func (s *Store) recordConflict(ctx context.Context, op opName) {
	s.opts.Meter.IncCounter(ctx, metricConflicts, 1, labels("component", componentValue, "op", string(op))...)
}

func (s *Store) recordRetry(ctx context.Context, op opName, reason string) {
	s.opts.Meter.IncCounter(ctx, metricRetries, 1, labels("component", componentValue, "op", string(op), "reason", reason)...)
}

func (s *Store) recordListPage(ctx context.Context, prefix string) {
	s.opts.Meter.IncCounter(ctx, metricListPages, 1, labels("component", componentValue, "op", string(opList), "prefix", prefix)...)
}

func (s *Store) recordCorrupt(ctx context.Context) {
	s.opts.Meter.IncCounter(ctx, metricCorrupt, 1, labels("component", componentValue)...)
}
