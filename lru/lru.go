package lru

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
)

// Store is a distributed LRU metadata store built on the cas package.
// All methods are safe for concurrent use by many replicas. It holds no
// global process state.
type Store struct {
	opts       Options
	backend    s3backend.Backend
	cas        *cas.Store
	shardFmt   string
	shardWidth int
	cursors    []string // per-shard list continuation (object key); owned by one worker

	closed   atomic.Bool
	evMu     sync.Mutex
	evCtx    context.Context
	evCancel context.CancelFunc
	evOnce   sync.Once
	evWG     sync.WaitGroup

	lastEvict atomic.Int64 // unix nano; 0 means never
}

// New constructs a Store backed by b with the given options.
func New(b s3backend.Backend, opts Options) (*Store, error) {
	if b == nil {
		return nil, errors.New("lru: nil backend")
	}
	opts.applyDefaults()
	opts.Meter = s3collections.MeterOrNoop(opts.Meter)
	opts.Logger = s3collections.LoggerOrNoop(opts.Logger)
	opts.Tracer = s3collections.TracerOrNoop(opts.Tracer)
	if opts.EvictorWorkers > opts.ShardCount {
		opts.EvictorWorkers = opts.ShardCount
	}
	width := len(strconv.Itoa(opts.ShardCount - 1))
	if width < 1 {
		width = 1
	}

	// Use a tiny clock-skew hint so that TombstoneMinAge is honored close to
	// the configured value (cas.GC subtracts its own skew margin).
	casStore, err := cas.New(b, opts.Prefix,
		cas.WithWriterID(opts.WriterID),
		cas.WithRetry(opts.Retry),
		cas.WithMeter(opts.Meter),
		cas.WithLogger(opts.Logger),
		cas.WithTracer(opts.Tracer),
		cas.WithClockSkewHint(time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("lru: create cas store: %w", err)
	}

	s := &Store{
		opts:       opts,
		backend:    b,
		cas:        casStore,
		shardFmt:   "%0" + strconv.Itoa(width) + "d",
		shardWidth: width,
		cursors:    make([]string, opts.ShardCount),
	}
	return s, nil
}

// shardFor returns the shard index for key.
func (s *Store) shardFor(key string) int {
	h := fnv.New32a()
	// fnv.Write never returns an error.
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % s.opts.ShardCount
}

// shardStr returns the zero-padded decimal shard string.
func (s *Store) shardStr(shard int) string {
	return fmt.Sprintf(s.shardFmt, shard)
}

// appKey returns the cas application key for a logical key.
func (s *Store) appKey(key string) string {
	shard := s.shardFor(key)
	return fmt.Sprintf("entries/%s/%s", s.shardStr(shard), key)
}

func (s *Store) checkClosed() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return nil
}

// Get returns metadata for key if present and not tombstoned.
func (s *Store) Get(ctx context.Context, key string) (Entry, error) {
	start := time.Now()
	if err := s.checkClosed(); err != nil {
		s.observeLatency(ctx, opGet, start, outcomeError)
		return Entry{}, err
	}
	ent, err := s.getEntry(ctx, key)
	if err != nil {
		s.observeLatency(ctx, opGet, start, outcomeFor(err))
		return Entry{}, err
	}
	if s.opts.TouchOnGet {
		if terr := s.Touch(ctx, key); terr != nil && !errors.Is(terr, ErrNotFound) {
			s.opts.Logger.Warn(terr, "lru: touch-on-get failed", "key", key)
		}
	}
	s.observeLatency(ctx, opGet, start, outcomeSuccess)
	return ent, nil
}

func (s *Store) getEntry(ctx context.Context, key string) (Entry, error) {
	rec, err := s.cas.Get(ctx, s.appKey(key))
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	e, err := parseEntry(rec.Value)
	if err != nil {
		return Entry{}, fmt.Errorf("lru: corrupt entry %q: %w", key, err)
	}
	return entryToPublic(key, rec.Revision, e), nil
}

// Set inserts or updates metadata for key and marks it recently used.
// If the key is tombstoned, it is resurrected with the supplied metadata.
func (s *Store) Set(ctx context.Context, key string, meta EntryMeta) error {
	start := time.Now()
	if err := s.checkClosed(); err != nil {
		s.observeLatency(ctx, opSet, start, outcomeError)
		return err
	}
	appKey := s.appKey(key)
	value, err := entryBytes(key, meta, true, time.Time{})
	if err != nil {
		s.observeLatency(ctx, opSet, start, outcomeError)
		return err
	}

	// Fast path: create if absent.
	_, err = s.cas.Create(ctx, appKey, value)
	if err == nil {
		s.observeLatency(ctx, opSet, start, outcomeSuccess)
		return nil
	}
	if !errors.Is(err, cas.ErrAlreadyExists) {
		s.observeLatency(ctx, opSet, start, outcomeFor(err))
		return err
	}

	// Slow path: update or resurrect. Preserve any existing cleared timestamp
	// so that the evictor's grace window is not reset on a metadata-only Set.
	_, err = s.cas.Update(ctx, appKey, func(_ context.Context, cur cas.Record) ([]byte, error) {
		cleared := time.Time{}
		if cur.State == cas.Live && len(cur.Value) > 0 {
			if e, perr := parseEntry(cur.Value); perr == nil {
				cleared = e.Cleared
			}
		}
		return entryBytes(key, meta, true, cleared)
	}, cas.WithIncludeTombstone(), cas.WithResurrect())
	if err != nil {
		s.observeLatency(ctx, opSet, start, outcomeFor(err))
		return err
	}
	s.observeLatency(ctx, opSet, start, outcomeSuccess)
	return nil
}

// Touch marks key as recently used subject to TouchPolicy.
func (s *Store) Touch(ctx context.Context, key string) error {
	start := time.Now()
	if err := s.checkClosed(); err != nil {
		s.observeLatency(ctx, opTouch, start, outcomeError)
		return err
	}
	appKey := s.appKey(key)
	now := time.Now()
	var wrote bool
	_, err := s.cas.Update(ctx, appKey, func(_ context.Context, cur cas.Record) ([]byte, error) {
		wrote = false
		if cur.State != cas.Live {
			return nil, cas.ErrNotFound
		}
		e, perr := parseEntry(cur.Value)
		if perr != nil {
			return nil, perr
		}
		window := s.opts.TouchPolicy.CoalesceWindow
		if window > 0 && !e.M.LastAccessAt.IsZero() && now.Sub(e.M.LastAccessAt) < window {
			return nil, nil
		}
		e.Access = true
		if s.opts.TouchPolicy.UpdateLastAccess {
			e.M.LastAccessAt = now
		}
		if s.opts.TouchPolicy.UpdateAccessCount {
			e.M.AccessCount++
		}
		wrote = true
		return entryBytes(key, e.M.toPublic(), e.Access, e.Cleared)
	})
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			s.observeLatency(ctx, opTouch, start, outcomeError)
			return ErrNotFound
		}
		s.observeLatency(ctx, opTouch, start, outcomeFor(err))
		return err
	}
	if wrote {
		s.incCounter(ctx, metricTouchWrites, 1)
	} else {
		s.incCounter(ctx, metricTouchSkips, 1)
	}
	s.observeLatency(ctx, opTouch, start, outcomeSuccess)
	return nil
}

// Delete tombstones key via CAS.
func (s *Store) Delete(ctx context.Context, key string) error {
	start := time.Now()
	if err := s.checkClosed(); err != nil {
		s.observeLatency(ctx, opDelete, start, outcomeError)
		return err
	}
	appKey := s.appKey(key)
	err := s.retry(ctx, opDelete, func(ctx context.Context) error {
		rec, err := s.cas.Get(ctx, appKey)
		if err != nil {
			if errors.Is(err, cas.ErrNotFound) {
				return ErrNotFound
			}
			return err
		}
		_, err = s.cas.Delete(ctx, appKey, rec.Revision)
		if err != nil && !errors.Is(err, cas.ErrConflict) && !errors.Is(err, cas.ErrNotFound) {
			return err
		}
		if errors.Is(err, cas.ErrConflict) {
			s.recordConflict(ctx, opDelete)
			return err
		}
		return nil
	})
	s.observeLatency(ctx, opDelete, start, outcomeFor(err))
	return err
}

// Len returns the total number of live entries. It performs a full LIST of
// every shard and is therefore O(entries). Use sparingly.
func (s *Store) Len(ctx context.Context) (int64, error) {
	start := time.Now()
	if err := s.checkClosed(); err != nil {
		s.observeLatency(ctx, opLen, start, outcomeError)
		return 0, err
	}
	var total int64
	token := ""
	for {
		page, err := s.cas.List(ctx, &cas.ListOptions{
			Prefix:            "entries/",
			ContinuationToken: token,
			MaxKeys:           pageSize(s.opts.ListPageSize),
		})
		if err != nil {
			s.observeLatency(ctx, opLen, start, outcomeError)
			return 0, err
		}
		for _, rec := range page.Records {
			if rec.State == cas.Live {
				total++
			}
		}
		if !page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
		if token == "" {
			break
		}
	}
	s.observeLatency(ctx, opLen, start, outcomeSuccess)
	return total, nil
}

// Stats returns a snapshot of store state. Values are approximate under
// concurrency because the store is distributed.
type Stats struct {
	Shards         int
	CapacityBytes  int64
	CapacityItems  int64
	ApproxItems    int64
	ApproxBytes    int64
	Tombstones     int64
	LastEvictionAt time.Time
}

// Stats returns a snapshot of the store.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	start := time.Now()
	if err := s.checkClosed(); err != nil {
		s.observeLatency(ctx, opStats, start, outcomeError)
		return Stats{}, err
	}
	live, tomb, bytes, err := s.countEntries(ctx, "entries/")
	if err != nil {
		s.observeLatency(ctx, opStats, start, outcomeError)
		return Stats{}, err
	}
	s.setGauge(ctx, metricEntries, float64(live), labelKind, kindLive)
	s.setGauge(ctx, metricEntries, float64(tomb), labelKind, kindTombstone)
	s.setGauge(ctx, metricBytes, float64(bytes))
	st := Stats{
		Shards:        s.opts.ShardCount,
		CapacityBytes: s.opts.CapacityBytes,
		CapacityItems: s.opts.CapacityItems,
		ApproxItems:   live,
		ApproxBytes:   bytes,
		Tombstones:    tomb,
	}
	if ns := s.lastEvict.Load(); ns != 0 {
		st.LastEvictionAt = time.Unix(0, ns)
	}
	s.observeLatency(ctx, opStats, start, outcomeSuccess)
	return st, nil
}

// countEntries returns live count, tombstone count, and live bytes for the
// given app-level prefix.
func (s *Store) countEntries(ctx context.Context, prefix string) (int64, int64, int64, error) {
	var live, tomb, bytes int64
	token := ""
	for {
		page, err := s.cas.List(ctx, &cas.ListOptions{
			Prefix:            prefix,
			ContinuationToken: token,
			MaxKeys:           pageSize(s.opts.ListPageSize),
		})
		if err != nil {
			return 0, 0, 0, err
		}
		for _, rec := range page.Records {
			switch rec.State {
			case cas.Live:
				live++
				if e, err := parseEntry(rec.Value); err == nil {
					bytes += e.M.toPublic().SizeBytes
				}
			case cas.Tombstone:
				tomb++
			}
		}
		if !page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
		if token == "" {
			break
		}
	}
	return live, tomb, bytes, nil
}

func pageSize(n int) int {
	if n <= 0 {
		return 1000
	}
	return n
}

// Close stops the evictor and marks the store closed. It is idempotent.
func (s *Store) Close() error {
	s.closed.Store(true)
	s.evMu.Lock()
	if s.evCancel != nil {
		s.evCancel()
	}
	s.evMu.Unlock()
	s.evWG.Wait()
	return nil
}

// retry executes op with the store retry policy, recording retries and conflicts.
func (s *Store) retry(ctx context.Context, op opName, opFn func(context.Context) error) error {
	policy := s.opts.Retry
	nextDelay := s3collections.BackoffDelays(policy, nil)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		err := opFn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, cas.ErrConflict) {
			s.recordConflict(ctx, op)
		}
		if !errors.Is(err, cas.ErrConflict) && !s3backend.IsRetryable(err) {
			return err
		}
		if errors.Is(err, cas.ErrConflict) || s3backend.IsRetryable(err) {
			s.recordRetry(ctx, op, retryReasonConflict)
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

const casObjectSuffix = ".cas.v1.json"

// decodeObjectKey reverses the default cas key codec for an object under this
// store. It is used by the evictor to turn backend object keys back into the
// application keys that cas understands.
func (s *Store) decodeObjectKey(objectKey string) (string, error) {
	if !strings.HasPrefix(objectKey, s.opts.Prefix) {
		return "", fmt.Errorf("lru: object key %q outside store prefix", objectKey)
	}
	encoded := strings.TrimPrefix(objectKey, s.opts.Prefix)
	if !strings.HasSuffix(encoded, casObjectSuffix) {
		return "", fmt.Errorf("lru: object key %q missing cas suffix", objectKey)
	}
	base := strings.TrimSuffix(encoded, casObjectSuffix)
	segments := strings.Split(base, "/")
	out := make([]string, len(segments))
	for i, seg := range segments {
		d, err := url.PathUnescape(seg)
		if err != nil {
			return "", err
		}
		if d == "" {
			return "", fmt.Errorf("lru: empty decoded segment")
		}
		out[i] = d
	}
	return strings.Join(out, "/"), nil
}
