package cas

import (
	"context"
	"time"

	"github.com/damianb/s3collections"
)

// RetryPolicy is an alias for the shared retry policy in the root package.
type RetryPolicy = s3collections.RetryPolicy

// State represents the lifecycle state of a record.
type State int

const (
	// Live is a normal readable record.
	Live State = iota
	// Tombstone is a deleted record that still occupies storage.
	Tombstone
)

// Record is the public view of a stored envelope.
type Record struct {
	Key       string
	Value     []byte
	Revision  uint64
	State     State
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time
	WriterID  string
	ETag      string
}

// UpdateFn is called during Update to produce the next value from the current record.
// It must be pure and side-effect free; it may be invoked multiple times under contention.
type UpdateFn func(ctx context.Context, cur Record) (next []byte, err error)

// KeyCodec encodes application keys to S3 object keys and validates inputs.
type KeyCodec interface {
	// Encode returns the S3 object key under the store's prefix. Must be injective.
	Encode(appKey string) (objectKey string, err error)
	// Decode reverses Encode for listing/GC.
	Decode(objectKey string) (appKey string, err error)
}

// Options configures a Store.
type Options struct {
	WriterID           string
	MaxValueBytes      int
	Retry              RetryPolicy
	Meter              s3collections.Meter
	Logger             s3collections.Logger
	Tracer             s3collections.Tracer
	ClockSkewHint      time.Duration
	KeyCodec           KeyCodec
	TombstoneRetention time.Duration
}

// Option configures Store construction.
type Option func(*Options)

// WriteOptions configures a single mutating call.
type WriteOptions struct {
	Retry            *RetryPolicy
	IncludeTombstone bool
	Resurrect        bool
}

// WriteOption configures a single mutating call.
type WriteOption func(*WriteOptions)

const (
	defaultMaxValueBytes      = 256 * 1024
	defaultClockSkewHint      = 2 * time.Minute
	defaultTombstoneRetention = 5 * time.Minute
	objectSuffix              = ".cas.v1.json"
)

// applyDefaults fills zero fields in o with defaults.
func applyDefaults(o *Options) {
	if o.MaxValueBytes <= 0 {
		o.MaxValueBytes = defaultMaxValueBytes
	}
	if o.ClockSkewHint <= 0 {
		o.ClockSkewHint = defaultClockSkewHint
	}
	if o.TombstoneRetention <= 0 {
		o.TombstoneRetention = defaultTombstoneRetention
	}
	if o.WriterID == "" {
		o.WriterID = "cas"
	}
	o.Retry = o.Retry.WithDefaults()
}

// applyWriteDefaults returns a WriteOptions with defaults applied.
func applyWriteDefaults(opts []WriteOption, store *Options) WriteOptions {
	var w WriteOptions
	for _, opt := range opts {
		opt(&w)
	}
	if w.Retry == nil {
		p := store.Retry
		w.Retry = &p
	}
	return w
}

// WithWriterID sets the writer identity persisted in envelopes.
func WithWriterID(id string) Option {
	return func(o *Options) { o.WriterID = id }
}

// WithMaxValueBytes sets the maximum value size in bytes.
func WithMaxValueBytes(n int) Option {
	return func(o *Options) { o.MaxValueBytes = n }
}

// WithRetry sets the default retry policy for the Store.
func WithRetry(p RetryPolicy) Option {
	return func(o *Options) { o.Retry = p }
}

// WithMeter sets the metric sink.
func WithMeter(m s3collections.Meter) Option {
	return func(o *Options) { o.Meter = m }
}

// WithLogger sets the logger.
func WithLogger(l s3collections.Logger) Option {
	return func(o *Options) { o.Logger = l }
}

// WithTracer sets the tracer.
func WithTracer(t s3collections.Tracer) Option {
	return func(o *Options) { o.Tracer = t }
}

// WithClockSkewHint sets the safety margin used for GC timestamp decisions.
func WithClockSkewHint(d time.Duration) Option {
	return func(o *Options) { o.ClockSkewHint = d }
}

// WithKeyCodec sets the application-key encoder.
func WithKeyCodec(kc KeyCodec) Option {
	return func(o *Options) { o.KeyCodec = kc }
}

// WithTombstoneRetention sets the default retention window for GC.
func WithTombstoneRetention(d time.Duration) Option {
	return func(o *Options) { o.TombstoneRetention = d }
}

// Write option helpers.

// WithRetryPolicy overrides the Store's retry policy for a single call.
func WithRetryPolicy(p RetryPolicy) WriteOption {
	return func(w *WriteOptions) { w.Retry = &p }
}

// WithIncludeTombstone allows Update's fn to observe tombstoned records.
func WithIncludeTombstone() WriteOption {
	return func(w *WriteOptions) { w.IncludeTombstone = true }
}

// WithResurrect allows Update's fn to turn a tombstone back into a live record.
//
// WARNING: physical GC of the tombstone races with resurrection. Only safe when
// tombstones of the affected key family are never deleted, or retention vastly
// exceeds the duration of a single Update.
func WithResurrect() WriteOption {
	return func(w *WriteOptions) { w.Resurrect = true }
}
