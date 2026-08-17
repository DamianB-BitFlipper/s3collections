# Observability

`s3collections` emits metrics through the `s3collections.Meter` interface and
logs through the `s3collections.Logger` interface. This document lists the
exact metric names, describes their labels, and provides adapter sketches for
common observability stacks.

## Interfaces

```go
// Meter receives metrics. Implementations must be safe for concurrent use.
type Meter interface {
    IncCounter(ctx context.Context, name string, delta float64, labels ...Label)
    ObserveHistogram(ctx context.Context, name string, value float64, labels ...Label)
    SetGauge(ctx context.Context, name string, value float64, labels ...Label)
}

// Logger receives structured logs.
type Logger interface {
    Debug(msg string, kv ...any)
    Info(msg string, kv ...any)
    Warn(err error, msg string, kv ...any)
    Error(err error, msg string, kv ...any)
    With(kv ...any) Logger
}

// Tracer starts optional spans.
type Tracer interface {
    StartSpan(ctx context.Context, name string, attrs ...Label) (context.Context, Span)
}

type Span interface {
    End(err error)
    AddEvent(name string, attrs ...Label)
}
```

A nil Meter/Logger/Tracer in component options is replaced by a no-op
implementation. Use `s3collections.NoopMeter()`, `NoopLogger()`, and
`NoopTracer()` if you need to construct one explicitly.

## Metric reference

All components emit the shared metrics below. `component` label values are
`"cas"`, `"lru"`, or `"queue"`.

### `s3collections_latency_seconds` (histogram)

* Labels: `component`, `op`, `outcome`.
* Meaning: end-to-end latency of a logical operation, in seconds.
* `op` values per component:
  * `cas`: `create`, `get`, `cas`, `update`, `delete`, `list`, `gc`.
  * `lru`: `get`, `set`, `touch`, `delete`, `len`, `stats`, `evict`.
  * `queue`: `enqueue`, `claim`, `renew`, `complete`, `retry`, `dead`,
    `requeue_dead`, `list_dead`, `reaper`, `gc`.
* `outcome` values: `success`, `error`, `conflict`, `empty` (queue claim),
  `dead` (queue retry→dead).

### `s3collections_cas_attempts` (histogram)

* Labels: `component`, `op`.
* Meaning: number of CAS attempts per logical operation. Values > 1 indicate
  contention or transient failures.

### `s3collections_conflicts_total` (counter)

* Labels: `component`, `op`.
* Meaning: number of `412 Precondition Failed` responses / CAS conflicts.
* **Alert:** sustained spikes indicate hot-key contention.

### `s3collections_retries_total` (counter)

* Labels: `component`, `op`, `reason`.
* Meaning: retry loops entered. `reason` is currently `"backend"` or
  `"conflict"`.
* **Alert:** rapid growth with `reason=backend` indicates S3 throttling or
  transient errors.

### `s3collections_list_pages_total` (counter)

* Labels: `component`, `op`, `prefix`.
* Meaning: number of LIST pages fetched.
* **Important:** the `prefix` label value is a static template such as
  `"cas"` or `"lru/entries/<shard>/"`. It never contains user keys, job IDs,
  or cache keys, keeping cardinality bounded.

### `s3collections_corrupt_total` (counter)

* Labels: `component`.
* Meaning: corrupt envelopes or objects encountered during scans.
* **Alert:** any non-zero value should be investigated.

### `s3collections_reaper_runs_total` (counter)

* Labels: `component`.
* Meaning: number of background reaper/GC passes started.

### `s3collections_reaper_deleted_total` (counter)

* Labels: `component`, `kind`.
* Meaning: objects removed by background passes. `kind` is one of `ready`,
  `lease`, `dead` (queue markers) or the component-specific kind.

## LRU-specific metrics

### `s3collections_lru_entries` (gauge)

* Labels: `component="lru"`, `kind`.
* `kind=live`: approximate live entries.
* `kind=tombstone`: approximate tombstoned entries.

### `s3collections_lru_evictions_total` (counter)

* Labels: `component="lru"`, `reason`.
* `reason` values: `stale` (entry created but never touched), `capacity`.

### `s3collections_lru_bytes` (gauge)

* Labels: `component="lru"`.
* Meaning: approximate live bytes.

### `s3collections_lru_touch_writes_total` (counter)

* Labels: `component="lru"`.
* Meaning: number of `Touch` calls that actually wrote.

### `s3collections_lru_touch_skips_total` (counter)

* Labels: `component="lru"`.
* Meaning: number of `Touch` calls skipped due to the coalesce window.

## Queue-specific metrics

### `s3collections_queue_events_total` (counter)

* Labels: `queue`, `event`, `outcome`.
* `event` values: `enqueue`, `claim`, `renew`, `complete`, `retry`, `dead`,
  `reap`, `gc`, `list_dead`, `requeue_dead`.

### `s3collections_queue_depth` (gauge)

* Labels: `queue`, `kind`.
* `kind=ready`, `leased`, `dead`.
* **Best-effort:** sampled during reaper passes. Do not use for precise
  queue-length assertions.

## Required logging points

Components log at these levels:

* **Info:**
  * Successful start/stop of background loops (`StartEvictor`,
    `StartMaintenance`).
  * Constructor configuration (shard count, prefix, retention values).
  * Periodic reaper/GC summaries (scanned, candidates, deleted).
* **Warn:**
  * CAS hot-spotting or attempt count above half the max.
  * Repeated backend throttling / SlowDown.
  * Marker write/delete failures (queue) and touch-on-get failures (LRU).
  * Partial repair actions (orphan marker cleanup, lease backfill).
* **Error:**
  * Retry budget exhausted.
  * Corrupt or incompatible envelopes.
  * Logical invariant violations.

## Adapter sketches

### Prometheus

```go
type PromMeter struct {
    counters   map[string]*prometheus.CounterVec
    histograms map[string]*prometheus.HistogramVec
    gauges     map[string]*prometheus.GaugeVec
}

func (p *PromMeter) IncCounter(ctx context.Context, name string, delta float64, labels ...s3collections.Label) {
    vec := p.counterVec(name, labels)
    vec.With(toPromLabels(labels)).Add(delta)
}

func (p *PromMeter) ObserveHistogram(ctx context.Context, name string, value float64, labels ...s3collections.Label) {
    vec := p.histogramVec(name, labels)
    vec.With(toPromLabels(labels)).Observe(value)
}

func (p *PromMeter) SetGauge(ctx context.Context, name string, value float64, labels ...s3collections.Label) {
    vec := p.gaugeVec(name, labels)
    vec.With(toPromLabels(labels)).Set(value)
}
```

### log/slog

```go
type SlogLogger struct{ l *slog.Logger }

func (s SlogLogger) Debug(msg string, kv ...any) { s.l.Debug(msg, kv...) }
func (s SlogLogger) Info(msg string, kv ...any)  { s.l.Info(msg, kv...) }
func (s SlogLogger) Warn(err error, msg string, kv ...any) {
    s.l.Warn(msg, append(kv, "err", err)...)
}
func (s SlogLogger) Error(err error, msg string, kv ...any) {
    s.l.Error(msg, append(kv, "err", err)...)
}
func (s SlogLogger) With(kv ...any) s3collections.Logger { return SlogLogger{s.l.With(kv...)} }
```

### OpenTelemetry tracer

```go
type OTelTracer struct{ tracer trace.Tracer }

func (o *OTelTracer) StartSpan(ctx context.Context, name string, attrs ...s3collections.Label) (context.Context, s3collections.Span) {
    ctx, span := o.tracer.Start(ctx, name)
    for _, a := range attrs {
        span.SetAttributes(attribute.String(a.Key, a.Value))
    }
    return ctx, otelSpan{span}
}

type otelSpan struct{ trace.Span }

func (s otelSpan) End(err error) {
    if err != nil {
        s.Span.RecordError(err)
    }
    s.Span.End()
}
func (s otelSpan) AddEvent(name string, attrs ...s3collections.Label) {
    kv := make([]attribute.KeyValue, 0, len(attrs))
    for _, a := range attrs {
        kv = append(kv, attribute.String(a.Key, a.Value))
    }
    s.Span.AddEvent(name, trace.WithAttributes(kv...))
}
```

## CaptureMeter for tests

The root package provides `s3collections.NewCaptureMeter()` for unit tests
and local debugging:

```go
meter := s3collections.NewCaptureMeter()
store, err := cas.New(be, "test/", cas.WithMeter(meter))
// ... do work ...
fmt.Println("conflicts:", meter.Counter("s3collections_conflicts_total",
    s3collections.L("component", "cas"),
    s3collections.L("op", "update"),
))
```

`CaptureMeter` records counters, histograms, and gauges in memory and is safe
for concurrent use.

## Label safety

**Never put user data in label values.** Cache keys, job IDs, queue names,
and shard numbers are safe at bounded cardinality, but per-user or per-object
identifiers must not appear in labels. The library itself uses only static
prefix templates and low-cardinality identifiers as label values.
