package queue

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/damianb/s3collections"
)

// Option configures a Queue at construction time.
type Option func(*Options)

// Options holds the configuration for a Queue. The zero value is not usable
// directly; New applies defaults before using it.
type Options struct {
	// Shards is the number of queue shards. It must be <= 65536 and defaults
	// to 256. A larger count reduces hot-spotting on the ready/ prefix but
	// increases maintenance scan work.
	Shards uint16

	// DefaultVisibilityTimeout is the duration a claimed job remains leased
	// when Claim is called with a zero VisibilityTimeout. Defaults to 30s.
	DefaultVisibilityTimeout time.Duration

	// ClockSkewTolerance bounds how early or late workers may claim or reap
	// jobs based on wall-clock differences. It affects liveness, not safety.
	// Defaults to 2s.
	ClockSkewTolerance time.Duration

	// ClaimPageSize is the maximum number of ready markers returned per LIST
	// page during a Claim scan. Defaults to 128.
	ClaimPageSize int

	// ClaimMaxPages bounds the number of LIST pages per Claim call per shard.
	// Defaults to 4.
	ClaimMaxPages int

	// ClaimShardProbe is the number of distinct shards examined by a single
	// Claim call when RestrictToShards is empty. Defaults to min(Shards, 8).
	ClaimShardProbe int

	// ReaperInterval is the base period between reaper passes. Passes are
	// jittered to avoid replica stampedes. Defaults to 5s.
	ReaperInterval time.Duration

	// GCInterval is the base period between garbage-collection passes.
	// Passes are jittered. Defaults to 5m.
	GCInterval time.Duration

	// CompletedRetention is how long completed jobs are kept before being
	// tombstoned (and later physically removed by the cas layer). Defaults to
	// 24h.
	CompletedRetention time.Duration

	// DeadRetention is how long dead-lettered jobs are kept before being
	// tombstoned. Defaults to 168h (7d).
	DeadRetention time.Duration

	// MaxAttempts is the maximum number of delivery attempts before Retry
	// dead-letters a job. Zero means unlimited.
	MaxAttempts int

	// ReasonHistory caps the number of recent retry/dead reasons stored in
	// the job envelope. Defaults to 8.
	ReasonHistory int

	// SequencerEnabled enables strict global ordering via a single cas key.
	// Throughput is limited to roughly tens of enqueues per second.
	SequencerEnabled bool

	// WorkerID identifies the caller in job leases. Defaults to a random
	// 16-character hex value.
	WorkerID string

	// Retry is the retry policy passed to the internal cas.Store and used by
	// marker operations. Defaults to s3collections.DefaultRetry().
	Retry s3collections.RetryPolicy

	// Meter receives metrics. A nil value is replaced by a no-op meter.
	Meter s3collections.Meter

	// Logger receives logs. A nil value is replaced by a no-op logger.
	Logger s3collections.Logger

	// Tracer receives traces. A nil value is replaced by a no-op tracer.
	Tracer s3collections.Tracer

	// MaxPayloadBytes caps the payload size accepted by Enqueue. Defaults to
	// 256 KiB.
	MaxPayloadBytes int

	// now, when non-nil, overrides the wall clock. It is used by tests.
	now func() time.Time
}

const (
	defaultShards                    = 256
	defaultVisibilityTimeout         = 30 * time.Second
	defaultClockSkewTolerance        = 2 * time.Second
	defaultClaimPageSize             = 128
	defaultClaimMaxPages             = 4
	defaultReaperInterval            = 5 * time.Second
	defaultGCInterval                = 5 * time.Minute
	defaultCompletedRetention        = 24 * time.Hour
	defaultDeadRetention             = 168 * time.Hour
	defaultReasonHistory             = 8
	defaultMaxPayloadBytes           = 256 * 1024
	defaultTombstoneRetention        = 5 * time.Minute
	maxShards                 uint16 = 65535
)

// applyDefaults fills zero fields in o with package defaults.
func applyDefaults(o *Options) {
	if o.Shards == 0 {
		o.Shards = defaultShards
	}
	if o.Shards > maxShards {
		o.Shards = maxShards
	}
	if o.DefaultVisibilityTimeout <= 0 {
		o.DefaultVisibilityTimeout = defaultVisibilityTimeout
	}
	if o.ClockSkewTolerance <= 0 {
		o.ClockSkewTolerance = defaultClockSkewTolerance
	}
	if o.ClaimPageSize <= 0 {
		o.ClaimPageSize = defaultClaimPageSize
	}
	if o.ClaimMaxPages <= 0 {
		o.ClaimMaxPages = defaultClaimMaxPages
	}
	if o.ClaimShardProbe <= 0 {
		probe := int(o.Shards)
		if probe > 8 {
			probe = 8
		}
		o.ClaimShardProbe = probe
	}
	if o.ReaperInterval <= 0 {
		o.ReaperInterval = defaultReaperInterval
	}
	if o.GCInterval <= 0 {
		o.GCInterval = defaultGCInterval
	}
	if o.CompletedRetention <= 0 {
		o.CompletedRetention = defaultCompletedRetention
	}
	if o.DeadRetention <= 0 {
		o.DeadRetention = defaultDeadRetention
	}
	if o.ReasonHistory <= 0 {
		o.ReasonHistory = defaultReasonHistory
	}
	if o.WorkerID == "" {
		o.WorkerID = randomWorkerID()
	}
	if o.MaxPayloadBytes <= 0 {
		o.MaxPayloadBytes = defaultMaxPayloadBytes
	}
	o.Retry = o.Retry.WithDefaults()
	o.Meter = s3collections.MeterOrNoop(o.Meter)
	o.Logger = s3collections.LoggerOrNoop(o.Logger)
	o.Tracer = s3collections.TracerOrNoop(o.Tracer)
	if o.now == nil {
		o.now = func() time.Time { return time.Now().UTC() }
	}
}

func randomWorkerID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
