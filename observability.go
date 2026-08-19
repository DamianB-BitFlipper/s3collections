package s3collections

import (
	"context"
	"sync"
)

// Label is a single metric/trace dimension. Component docs define the
// allowed label keys; label VALUES must never contain user data (cache keys,
// job IDs) to keep cardinality bounded.
type Label struct {
	Key   string
	Value string
}

// L is a shorthand for constructing a Label.
func L(key, value string) Label { return Label{Key: key, Value: value} }

// Meter receives metrics from all components. Implementations must be safe
// for concurrent use. A nil Meter in component options means no metrics.
//
// The stable metric names emitted by this module are listed in
// docs/reference.md; adapters for Prometheus, OTel, etc.
// are implemented in user code by translating these calls.
type Meter interface {
	IncCounter(ctx context.Context, name string, delta float64, labels ...Label)
	ObserveHistogram(ctx context.Context, name string, value float64, labels ...Label)
	SetGauge(ctx context.Context, name string, value float64, labels ...Label)
}

// Logger receives structured logs from all components. A nil Logger in
// component options means no logging. Adapters for log/slog or other loggers
// are implemented in user code.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(err error, msg string, kv ...any)
	Error(err error, msg string, kv ...any)
	// With returns a Logger that includes kv on every subsequent call.
	With(kv ...any) Logger
}

// Span is a single traced operation.
type Span interface {
	End(err error)
	AddEvent(name string, attrs ...Label)
}

// Tracer starts optional tracing spans. A nil Tracer means no tracing.
// This module ships no OTel client; adapt an existing tracer in user code.
type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs ...Label) (context.Context, Span)
}

// Noop implementations.

type noopMeter struct{}

func (noopMeter) IncCounter(context.Context, string, float64, ...Label)       {}
func (noopMeter) ObserveHistogram(context.Context, string, float64, ...Label) {}
func (noopMeter) SetGauge(context.Context, string, float64, ...Label)         {}

// NoopMeter returns a Meter that discards everything.
func NoopMeter() Meter { return noopMeter{} }

type noopLogger struct{}

func (n noopLogger) Debug(string, ...any)        {}
func (n noopLogger) Info(string, ...any)         {}
func (n noopLogger) Warn(error, string, ...any)  {}
func (n noopLogger) Error(error, string, ...any) {}
func (n noopLogger) With(...any) Logger          { return n }

// NoopLogger returns a Logger that discards everything.
func NoopLogger() Logger { return noopLogger{} }

type noopTracer struct{}
type noopSpan struct{}

func (noopTracer) StartSpan(ctx context.Context, _ string, _ ...Label) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopSpan) End(error)                 {}
func (noopSpan) AddEvent(string, ...Label) {}

// NoopTracer returns a Tracer that starts no spans.
func NoopTracer() Tracer { return noopTracer{} }

// MeterOrNoop returns m or a no-op Meter when nil.
func MeterOrNoop(m Meter) Meter {
	if m == nil {
		return NoopMeter()
	}
	return m
}

// LoggerOrNoop returns l or a no-op Logger when nil.
func LoggerOrNoop(l Logger) Logger {
	if l == nil {
		return NoopLogger()
	}
	return l
}

// TracerOrNoop returns t or a no-op Tracer when nil.
func TracerOrNoop(t Tracer) Tracer {
	if t == nil {
		return NoopTracer()
	}
	return t
}

// CaptureMeter is an in-memory Meter for tests. It records every call and
// offers simple aggregations. Safe for concurrent use.
type CaptureMeter struct {
	mu         sync.Mutex
	Counters   map[string]float64
	Histograms map[string][]float64
	Gauges     map[string]float64
}

// NewCaptureMeter returns an empty CaptureMeter.
func NewCaptureMeter() *CaptureMeter {
	return &CaptureMeter{
		Counters:   make(map[string]float64),
		Histograms: make(map[string][]float64),
		Gauges:     make(map[string]float64),
	}
}

func seriesKey(name string, labels []Label) string {
	k := name
	for _, l := range labels {
		k += "|" + l.Key + "=" + l.Value
	}
	return k
}

func (c *CaptureMeter) IncCounter(_ context.Context, name string, delta float64, labels ...Label) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Counters[seriesKey(name, labels)] += delta
}

func (c *CaptureMeter) ObserveHistogram(_ context.Context, name string, value float64, labels ...Label) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := seriesKey(name, labels)
	c.Histograms[k] = append(c.Histograms[k], value)
}

func (c *CaptureMeter) SetGauge(_ context.Context, name string, value float64, labels ...Label) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Gauges[seriesKey(name, labels)] = value
}

// Counter returns the accumulated value for a name+labels series (0 if absent).
func (c *CaptureMeter) Counter(name string, labels ...Label) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Counters[seriesKey(name, labels)]
}

// CounterSum returns the sum of all counter series whose name matches,
// regardless of labels.
func (c *CaptureMeter) CounterSum(name string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var sum float64
	for k, v := range c.Counters {
		if len(k) >= len(name) && k[:len(name)] == name && (len(k) == len(name) || k[len(name)] == '|') {
			sum += v
		}
	}
	return sum
}

// HistogramCount returns the number of observations for a name+labels series.
func (c *CaptureMeter) HistogramCount(name string, labels ...Label) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.Histograms[seriesKey(name, labels)])
}
