package s3collections

import (
	"math/rand"
	"sync"
	"time"
)

// RetryPolicy configures bounded retries with decorrelated jittered backoff
// (per AWS "full jitter" practice). The zero value is valid and equivalent
// to DefaultRetry.
type RetryPolicy struct {
	// MaxAttempts is the total number of tries including the first.
	// 0 means the default (8).
	MaxAttempts int
	// Base is the lower bound of retry delays. 0 = 50ms.
	Base time.Duration
	// Max caps any single retry delay. 0 = 2s.
	Max time.Duration
	// Jitter in [0,1] scales randomization: 1.0 = full jitter, 0 =
	// deterministic midpoint of the window. The zero value selects the
	// default (full jitter, 1.0); to request deterministic backoff set
	// any negative value (normalized to 0). Values > 1 are clamped to 1.
	Jitter float64
}

// DefaultRetry returns the module-wide default policy: 8 attempts,
// 50ms base, 2s cap, full jitter.
func DefaultRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 8, Base: 50 * time.Millisecond, Max: 2 * time.Second, Jitter: 1.0}
}

// WithDefaults returns p with invalid/zero fields replaced by defaults.
func (p RetryPolicy) WithDefaults() RetryPolicy {
	d := DefaultRetry()
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = d.MaxAttempts
	}
	if p.Base <= 0 {
		p.Base = d.Base
	}
	if p.Max <= 0 {
		p.Max = d.Max
	}
	switch {
	case p.Jitter == 0:
		p.Jitter = d.Jitter
	case p.Jitter < 0:
		p.Jitter = 0
	case p.Jitter > 1:
		p.Jitter = 1
	}
	return p
}

var globalRand = struct {
	sync.Mutex
	r *rand.Rand
}{r: rand.New(rand.NewSource(time.Now().UnixNano()))}

// BackoffDelays returns a function producing successive retry delays using
// the decorrelated-jitter algorithm:
//
//	delay = Base + f*(min(Max, prev*3) - Base),  f in [0,1)
//
// where f is random scaled by Jitter (f = rnd*Jitter + 0.5*(1-Jitter)).
// The returned function is NOT safe for concurrent use; create one per
// retry loop. rnd may be nil, in which case a locked global source is used.
func BackoffDelays(p RetryPolicy, rnd *rand.Rand) func() time.Duration {
	p = p.WithDefaults()
	float64n := func() float64 {
		if rnd != nil {
			return rnd.Float64()
		}
		globalRand.Lock()
		defer globalRand.Unlock()
		return globalRand.r.Float64()
	}
	prev := float64(p.Base)
	lo := float64(p.Base)
	return func() time.Duration {
		hi := prev * 3
		if hi > float64(p.Max) {
			hi = float64(p.Max)
		}
		f := float64n()*p.Jitter + 0.5*(1-p.Jitter)
		d := lo + f*(hi-lo)
		prev = d
		return time.Duration(d)
	}
}
