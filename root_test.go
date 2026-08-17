package s3collections

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"
)

func TestRetryPolicyDefaults(t *testing.T) {
	p := (RetryPolicy{}).WithDefaults()
	if p.MaxAttempts != 8 || p.Base != 50*time.Millisecond || p.Max != 2*time.Second || p.Jitter != 1.0 {
		t.Fatalf("bad defaults: %+v", p)
	}
}

func TestBackoffDelaysBounds(t *testing.T) {
	p := RetryPolicy{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond, Jitter: 1.0}
	next := BackoffDelays(p, rand.New(rand.NewSource(42)))
	for i := 0; i < 1000; i++ {
		d := next()
		if d < 10*time.Millisecond || d > 100*time.Millisecond {
			t.Fatalf("delay %v out of [%v,%v]", d, 10*time.Millisecond, 100*time.Millisecond)
		}
	}
}

func TestBackoffDelaysDeterministicWithoutJitter(t *testing.T) {
	p := RetryPolicy{Base: 10 * time.Millisecond, Max: 1 * time.Second, Jitter: -1}
	a := BackoffDelays(p, rand.New(rand.NewSource(1)))
	b := BackoffDelays(p, rand.New(rand.NewSource(9)))
	for i := 0; i < 10; i++ {
		if da, db := a(), b(); da != db {
			t.Fatalf("jitter=0 must be deterministic: %v vs %v", da, db)
		}
	}
}

func TestNoopAdaptersUsable(t *testing.T) {
	ctx := context.Background()
	MeterOrNoop(nil).IncCounter(ctx, "x", 1)
	LoggerOrNoop(nil).Info("hello", "k", "v")
	ctx2, sp := TracerOrNoop(nil).StartSpan(ctx, "op")
	sp.AddEvent("e")
	sp.End(errors.New("boom"))
	_ = ctx2
}

func TestCaptureMeter(t *testing.T) {
	m := NewCaptureMeter()
	ctx := context.Background()
	m.IncCounter(ctx, "c", 1, L("op", "get"))
	m.IncCounter(ctx, "c", 2, L("op", "get"))
	m.IncCounter(ctx, "c", 5, L("op", "put"))
	if got := m.Counter("c", L("op", "get")); got != 3 {
		t.Fatalf("counter = %v, want 3", got)
	}
	if got := m.CounterSum("c"); got != 8 {
		t.Fatalf("counterSum = %v, want 8", got)
	}
	m.ObserveHistogram(ctx, "h", 1.5, L("op", "get"))
	m.ObserveHistogram(ctx, "h", 2.5, L("op", "get"))
	if got := m.HistogramCount("h", L("op", "get")); got != 2 {
		t.Fatalf("histogram count = %v, want 2", got)
	}
	m.SetGauge(ctx, "g", 7)
	if got := m.Gauges["g"]; got != 7 {
		t.Fatalf("gauge = %v, want 7", got)
	}
}
