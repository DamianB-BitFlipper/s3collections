package lru

import (
	"time"

	"github.com/damianb/s3collections"
)

// RetryPolicy is an alias for the shared retry policy in the root package.
type RetryPolicy = s3collections.RetryPolicy

// TouchPolicy controls how aggressively Touch writes.
type TouchPolicy struct {
	// CoalesceWindow bounds how often we will write touches per key.
	// If LastAccessAt >= now-CoalesceWindow, we skip the write.
	// Zero disables coalescing (not recommended).
	CoalesceWindow time.Duration

	// UpdateAccessCount increments AccessCount on each Touch write when true.
	UpdateAccessCount bool

	// UpdateLastAccess updates LastAccessAt when we do write.
	UpdateLastAccess bool
}

// Options configures a Store.
type Options struct {
	// Prefix is the S3 key prefix for this LRU. It must be unique per logical
	// store. The default is "lru/".
	Prefix string

	// ShardCount is the number of shards. It must be >= 1. The default is 128.
	ShardCount int

	// CapacityBytes is the target total live bytes. Zero disables byte cap.
	CapacityBytes int64

	// CapacityItems is the target total live item count. Zero disables item cap.
	CapacityItems int64

	// EvictorInterval is the periodic tick per worker. The default is 2s.
	EvictorInterval time.Duration

	// EvictorWorkers is the number of concurrent shard workers. The default is
	// min(ShardCount, 4).
	EvictorWorkers int

	// EvictorBatchSize is the maximum entries to process per shard per tick.
	// The default is 512.
	EvictorBatchSize int

	// TouchOnGet, when true, causes Get to call Touch internally.
	TouchOnGet bool

	// TouchPolicy defaults to {CoalesceWindow: 1s, UpdateAccessCount: true,
	// UpdateLastAccess: true}.
	TouchPolicy TouchPolicy

	// ListPageSize is the maximum keys per cas.List page. Zero uses the
	// backend default.
	ListPageSize int

	// Retry controls bounded retries for cas operations. The zero value uses
	// s3collections.DefaultRetry().
	Retry RetryPolicy

	// TombstoneMinAge is the minimum age of a tombstone before physical
	// deletion. The default is 24h. A negative value disables physical GC,
	// allowing tombstones to accumulate.
	//
	// Hazards: a Set that resurrects a tombstoned entry races with the GC
	// worker's verify→DELETE window (roughly one S3 RTT). If the resurrection
	// completes entirely inside that window, the freshly live entry can be
	// physically deleted and will be missing until the next Set. Choose
	// TombstoneMinAge much larger than the expected S3 RTT.
	TombstoneMinAge time.Duration

	// WriterID is persisted in cas envelopes. Defaults to "lru".
	WriterID string

	// Meter receives metrics. Nil means no metrics.
	Meter s3collections.Meter

	// Logger receives logs. Nil means no logging.
	Logger s3collections.Logger

	// Tracer receives traces. Nil means no tracing.
	Tracer s3collections.Tracer
}

func (o *Options) applyDefaults() {
	if o.Prefix == "" {
		o.Prefix = "lru/"
	}
	if o.ShardCount <= 0 {
		o.ShardCount = 128
	}
	if o.EvictorInterval <= 0 {
		o.EvictorInterval = 2 * time.Second
	}
	if o.EvictorWorkers <= 0 {
		if o.ShardCount < 4 {
			o.EvictorWorkers = o.ShardCount
		} else {
			o.EvictorWorkers = 4
		}
	}
	if o.EvictorBatchSize <= 0 {
		o.EvictorBatchSize = 512
	}
	if o.TouchPolicy.CoalesceWindow == 0 {
		o.TouchPolicy.CoalesceWindow = time.Second
	}
	if o.TouchPolicy.CoalesceWindow < 0 {
		o.TouchPolicy.CoalesceWindow = 0
	}
	// The zero TouchPolicy explicitly requests defaults for the booleans.
	if !o.TouchPolicy.UpdateAccessCount && !o.TouchPolicy.UpdateLastAccess {
		o.TouchPolicy.UpdateAccessCount = true
		o.TouchPolicy.UpdateLastAccess = true
	}
	if o.ListPageSize < 0 {
		o.ListPageSize = 0
	}
	o.Retry = o.Retry.WithDefaults()
	if o.TombstoneMinAge == 0 {
		o.TombstoneMinAge = 24 * time.Hour
	}
	if o.WriterID == "" {
		o.WriterID = "lru"
	}
}
