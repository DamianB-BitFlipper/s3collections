package tree

import (
	"time"

	"github.com/damianb/s3collections"
)

// Options configures a Store. New begins with documented defaults, then
// applies options, so WithBlobSizeRange(0, max) can explicitly allow small
// test/application blobs.
type Options struct {
	Prefix             string
	MinBlobBytes       int64
	MaxBlobBytes       int64
	MultipartThreshold int64
	MaxBufferedPut     int64
	MaxLineageDepth    uint64
	GCGrace            time.Duration
	ClockSkewHint      time.Duration
	MutationGateTTL    time.Duration
	Retry              s3collections.RetryPolicy
	Meter              s3collections.Meter
	Logger             s3collections.Logger
	Tracer             s3collections.Tracer
	Now                func() time.Time
}

type Option func(*Options)

func WithPrefix(prefix string) Option { return func(o *Options) { o.Prefix = prefix } }
func WithBlobSizeRange(min, max int64) Option {
	return func(o *Options) { o.MinBlobBytes, o.MaxBlobBytes = min, max }
}
func WithMultipartThreshold(n int64) Option        { return func(o *Options) { o.MultipartThreshold = n } }
func WithMaxBufferedPut(n int64) Option            { return func(o *Options) { o.MaxBufferedPut = n } }
func WithMaxLineageDepth(n uint64) Option          { return func(o *Options) { o.MaxLineageDepth = n } }
func WithGCGrace(d time.Duration) Option           { return func(o *Options) { o.GCGrace = d } }
func WithClockSkewHint(d time.Duration) Option     { return func(o *Options) { o.ClockSkewHint = d } }
func WithMutationGateTTL(d time.Duration) Option   { return func(o *Options) { o.MutationGateTTL = d } }
func WithRetry(p s3collections.RetryPolicy) Option { return func(o *Options) { o.Retry = p } }
func WithMeter(m s3collections.Meter) Option       { return func(o *Options) { o.Meter = m } }
func WithLogger(l s3collections.Logger) Option     { return func(o *Options) { o.Logger = l } }
func WithTracer(t s3collections.Tracer) Option     { return func(o *Options) { o.Tracer = t } }
func WithClock(now func() time.Time) Option        { return func(o *Options) { o.Now = now } }

func defaultOptions() Options {
	return Options{
		Prefix: "tree/", MinBlobBytes: 40 << 10, MaxBlobBytes: 500 << 20,
		MultipartThreshold: 100 << 20, MaxBufferedPut: 8 << 20,
		MaxLineageDepth: 1_000_000, GCGrace: 10 * time.Minute,
		ClockSkewHint: 2 * time.Minute, MutationGateTTL: 2 * time.Minute, Retry: s3collections.DefaultRetry(),
		Now: func() time.Time { return time.Now().UTC() },
	}
}
