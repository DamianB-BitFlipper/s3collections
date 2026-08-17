package s3backend

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ChaosConfig controls the fault-injection behavior of a Chaos backend.
type ChaosConfig struct {
	// Rand supplies randomness; provide a seeded rand.Rand for
	// deterministic tests. Nil means rand.New with a fixed seed of 1.
	Rand *rand.Rand
	// ErrorRate is the probability [0,1] that a call fails before reaching
	// the wrapped backend with a retryable 500/503-style error.
	ErrorRate float64
	// AmbiguousWriteRate is the probability [0,1] that a mutating call
	// (Put/Delete) is APPLIED to the wrapped backend but reported as a
	// failure. This models the real S3 failure mode where a write succeeds
	// server-side but the client never learns; retries must therefore be
	// idempotent.
	AmbiguousWriteRate float64
	// DelayRate is the probability [0,1] that a call sleeps Delay before
	// executing, modeling SlowDown/tail latency.
	DelayRate float64
	// Delay is the base injected latency; the actual sleep is uniformly
	// distributed in [Delay/2, Delay).
	Delay time.Duration
}

// Chaos wraps a Backend and injects transient errors, ambiguous write
// outcomes, and latency. It is used by stress tests of every structure in
// this module, and may be reused by consumers for their own chaos testing.
// It is safe for concurrent use; injected randomness is serialized, which
// only affects the statistical pattern, not the failure modes covered.
type Chaos struct {
	backend Backend
	cfg     ChaosConfig
	mu      sync.Mutex // guards rand (rand.Rand is not goroutine-safe)
	rand    *rand.Rand
}

// NewChaos wraps backend with fault injection per cfg.
func NewChaos(backend Backend, cfg ChaosConfig) *Chaos {
	r := cfg.Rand
	if r == nil {
		r = rand.New(rand.NewSource(1))
	}
	return &Chaos{backend: backend, cfg: cfg, rand: r}
}

func (c *Chaos) roll(p float64) bool {
	if p <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rand.Float64() < p
}

func (c *Chaos) maybeDelay(ctx context.Context) error {
	if !c.roll(c.cfg.DelayRate) || c.cfg.Delay <= 0 {
		return nil
	}
	c.mu.Lock()
	jitter := c.rand.Int63n(int64(c.cfg.Delay/2) + 1)
	c.mu.Unlock()
	d := c.cfg.Delay/2 + time.Duration(jitter)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Chaos) injectError(op, key string) error {
	code, status := "InternalError", 500
	c.mu.Lock()
	flip := c.rand.Intn(2)
	c.mu.Unlock()
	if flip == 0 {
		code, status = "SlowDown", 503
	}
	return &Error{Op: op, Key: key, StatusCode: status, Code: code,
		Message: "injected by s3backend.Chaos", Retryable: true}
}

func (c *Chaos) pre(ctx context.Context, op, key string) error {
	if err := c.maybeDelay(ctx); err != nil {
		return err
	}
	if c.roll(c.cfg.ErrorRate) {
		return c.injectError(op, key)
	}
	return nil
}

func (c *Chaos) Get(ctx context.Context, key string) (*Object, error) {
	if err := c.pre(ctx, "Get", key); err != nil {
		return nil, err
	}
	return c.backend.Get(ctx, key)
}

func (c *Chaos) Put(ctx context.Context, key string, body []byte, pre *Preconditions) (string, error) {
	if err := c.pre(ctx, "Put", key); err != nil {
		return "", err
	}
	etag, err := c.backend.Put(ctx, key, body, pre)
	if err == nil && c.roll(c.cfg.AmbiguousWriteRate) {
		return "", fmt.Errorf("put %q applied but reported failed: %w", key, c.injectError("Put", key))
	}
	return etag, err
}

func (c *Chaos) Delete(ctx context.Context, key string) error {
	if err := c.pre(ctx, "Delete", key); err != nil {
		return err
	}
	err := c.backend.Delete(ctx, key)
	if err == nil && c.roll(c.cfg.AmbiguousWriteRate) {
		return fmt.Errorf("delete %q applied but reported failed: %w", key, c.injectError("Delete", key))
	}
	return err
}

func (c *Chaos) List(ctx context.Context, prefix string, opts *ListOptions) (*ListPage, error) {
	if err := c.pre(ctx, "List", prefix); err != nil {
		return nil, err
	}
	return c.backend.List(ctx, prefix, opts)
}
