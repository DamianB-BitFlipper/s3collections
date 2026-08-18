package lru

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
)

// StartEvictor starts background eviction workers. Subsequent calls are no-ops.
// Workers run until ctx is done or Close is called.
func (s *Store) StartEvictor(ctx context.Context) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	s.evOnce.Do(func() {
		evCtx, cancel := context.WithCancel(ctx)
		s.evMu.Lock()
		s.evCtx = evCtx
		s.evCancel = cancel
		s.evMu.Unlock()

		for i := 0; i < s.opts.EvictorWorkers; i++ {
			s.evWG.Add(1)
			go s.worker(evCtx, i)
		}
	})
	return nil
}

func (s *Store) worker(ctx context.Context, id int) {
	defer s.evWG.Done()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	// Jittered initial sleep so workers do not stampede S3.
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitteredDelay(rnd, s.opts.EvictorInterval)):
	}

	ticker := time.NewTicker(s.opts.EvictorInterval)
	defer ticker.Stop()

	tick := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for shard := id; shard < s.opts.ShardCount; shard += s.opts.EvictorWorkers {
			if ctx.Err() != nil {
				return
			}
			s.processShard(ctx, shard, tick)
		}
		tick++
	}
}

func jitteredDelay(rnd *rand.Rand, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return time.Duration(rnd.Float64() * float64(interval))
}

// appKeyPrefix returns the cas application-key prefix for a shard.
func (s *Store) appKeyPrefix(shard int) string {
	return fmt.Sprintf("entries/%s/", s.shardStr(shard))
}

// withBackendRetry executes op with bounded retries on transient backend
// errors, emitting s3collections_retries_total{component="lru",op,reason="backend"}.
func (s *Store) withBackendRetry(ctx context.Context, op opName, opFn func(context.Context) error) error {
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
		if !s3backend.IsRetryable(err) {
			return err
		}
		s.recordRetry(ctx, op, retryReasonBackend)
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

// processShard runs one CLOCK eviction pass over a single shard.
func (s *Store) processShard(ctx context.Context, shard, tick int) {
	objectPrefix := s.opts.Prefix + fmt.Sprintf("entries/%s/", s.shardStr(shard))
	now := time.Now()

	// Target capacity per shard. Zero means no cap in that dimension.
	var targetBytes, targetItems int64
	if s.opts.CapacityBytes > 0 {
		targetBytes = int64(math.Floor(float64(s.opts.CapacityBytes) / float64(s.opts.ShardCount) * 0.95))
	}
	if s.opts.CapacityItems > 0 {
		targetItems = int64(math.Floor(float64(s.opts.CapacityItems) / float64(s.opts.ShardCount) * 0.95))
	}

	// countEntries returns (live count, tombstone count, live bytes, error).
	liveItems, tombstones, liveBytes, err := s.shardUsage(ctx, shard)
	if err != nil {
		s.opts.Logger.Warn(err, "lru: shard usage failed", "shard", shard)
		return
	}

	cursor := s.getCursor(shard)
	freedBytes, freedItems := int64(0), int64(0)

	page, err := s.listShard(ctx, shard, cursor)
	if err != nil {
		s.opts.Logger.Warn(err, "lru: shard list failed", "shard", shard)
		return
	}

	grace := s.opts.EvictorInterval / 2
	if grace < 10*time.Millisecond {
		grace = 10 * time.Millisecond
	}

	for _, rec := range page.Records {
		if ctx.Err() != nil {
			return
		}
		if rec.State != cas.Live {
			continue
		}
		e, err := parseEntry(rec.Value)
		if err != nil {
			s.incCounter(ctx, metricCorrupt, 1)
			continue
		}

		// Stale/orphan heuristic: entries that were never accessed and were
		// created well before the evictor interval are assumed to be orphans
		// left by a crashed writer. Wall-clock skew is tolerated because the
		// threshold is several evictor intervals old; a small amount of skew
		// only shifts the cleanup by a single tick.
		stale := e.M.AccessCount == 0 && !e.M.CreatedAt.IsZero() && now.Sub(e.M.CreatedAt) > 2*s.opts.EvictorInterval
		if stale {
			if s.evictRecord(ctx, rec, "stale", false, 0, time.Time{}) {
				freedBytes += e.M.SizeBytes
				freedItems++
			}
			continue
		}

		if e.Access {
			// First pass: clear the access bit.
			if err := s.clearAccess(ctx, rec, e, now); err != nil {
				// Best-effort: log transient backend errors and emit a retry
				// metric, but do not abort the pass.
				if s3backend.IsRetryable(err) {
					s.recordRetry(ctx, opEvict, retryReasonBackend)
				}
				s.opts.Logger.Warn(err, "lru: clear access failed", "shard", shard, "key", rec.Key)
			}
			continue
		}

		// Second pass: evict if the bit has been clear long enough and the
		// shard is still over its proportional target.
		clearedGrace := !e.Cleared.IsZero() && now.Sub(e.Cleared) >= grace
		if clearedGrace {
			overBytes := liveBytes-freedBytes > targetBytes && targetBytes > 0
			overItems := liveItems-freedItems > targetItems && targetItems > 0
			if overBytes || overItems {
				if s.evictRecord(ctx, rec, "capacity", true, grace, now) {
					freedBytes += e.M.SizeBytes
					freedItems++
				}
			}
		}
	}

	// Update cursor for the next tick.
	if page.IsTruncated && page.NextContinuationToken != "" {
		s.setCursor(shard, page.NextContinuationToken)
	} else {
		s.setCursor(shard, "")
	}

	// Update approximate gauges. These are best-effort estimates because
	// concurrent writers may add or remove entries between the usage scan
	// and this point; the tombstone gauge in particular counts freed items
	// that may not yet be physically deleted.
	s.setGauge(ctx, metricEntries, float64(liveItems-freedItems), labelKind, kindLive)
	s.setGauge(ctx, metricEntries, float64(tombstones+freedItems), labelKind, kindTombstone)
	s.setGauge(ctx, metricBytes, float64(liveBytes-freedBytes))

	// Physical tombstone GC, rotated per tick to avoid a full LIST storm.
	if s.opts.TombstoneMinAge >= 0 && tick%s.opts.ShardCount == shard {
		cutoff := now.Add(-s.opts.TombstoneMinAge)
		if _, err := s.cas.GC(ctx, &cas.GCOptions{
			Prefix:    objectPrefix[len(s.opts.Prefix):],
			OlderThan: cutoff,
		}); err != nil {
			s.opts.Logger.Warn(err, "lru: tombstone GC failed", "shard", shard)
		}
	}
}

// listShard pages one shard using cas.Store.List. We use the cas List API
// rather than direct backend paging so that key encoding/decoding stays
// encapsulated in the cas package; the paging cost is equivalent because
// both paths perform one LIST and one GET per object.
func (s *Store) listShard(ctx context.Context, shard int, continuation string) (*cas.ListPage, error) {
	var page *cas.ListPage
	err := s.withBackendRetry(ctx, opEvict, func(ctx context.Context) error {
		var err error
		page, err = s.cas.List(ctx, &cas.ListOptions{
			Prefix:            s.appKeyPrefix(shard),
			ContinuationToken: continuation,
			MaxKeys:           pageSize(s.opts.EvictorBatchSize),
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	s.incCounter(ctx, metricListPages, 1, "op", "list", "prefix", "lru/entries/<shard>/")
	return page, nil
}

// shardUsage returns live count, tombstone count, and live bytes for a shard.
func (s *Store) shardUsage(ctx context.Context, shard int) (int64, int64, int64, error) {
	var live, tomb, bytes int64
	token := ""
	for {
		var page *cas.ListPage
		err := s.withBackendRetry(ctx, opEvict, func(ctx context.Context) error {
			var err error
			page, err = s.cas.List(ctx, &cas.ListOptions{
				Prefix:            s.appKeyPrefix(shard),
				ContinuationToken: token,
				MaxKeys:           pageSize(s.opts.ListPageSize),
			})
			return err
		})
		if err != nil {
			return 0, 0, 0, err
		}
		for _, rec := range page.Records {
			switch rec.State {
			case cas.Live:
				live++
				if e, err := parseEntry(rec.Value); err == nil {
					bytes += e.M.SizeBytes
				} else {
					s.incCounter(ctx, metricCorrupt, 1)
				}
			case cas.Tombstone:
				tomb++
			}
		}
		if !page.IsTruncated {
			break
		}
		if page.NextContinuationToken == "" {
			break
		}
		token = page.NextContinuationToken
	}
	return live, tomb, bytes, nil
}

func (s *Store) getCursor(shard int) string {
	return s.cursors[shard]
}

func (s *Store) setCursor(shard int, cursor string) {
	s.cursors[shard] = cursor
}

// clearAccess writes a=false and cleared=now for a live record. Conflicts are
// expected under concurrent Touch and are ignored.
func (s *Store) clearAccess(ctx context.Context, rec cas.Record, e entry, now time.Time) error {
	if !e.Access {
		return nil
	}
	_, err := s.cas.Update(ctx, rec.Key, func(_ context.Context, cur cas.Record) ([]byte, error) {
		if cur.State != cas.Live {
			return nil, nil
		}
		curE, perr := parseEntry(cur.Value)
		if perr != nil || !curE.Access {
			return nil, nil
		}
		curE.Access = false
		curE.Cleared = now
		return entryBytes(cur.Key, curE.M.toPublic(), false, now)
	})
	return err
}

// evictRecord re-reads the candidate and attempts to tombstone it. It
// returns true if the delete succeeded. Revision mismatches (touch-during-
// eviction races) are treated as failures and not errors.
// When checkAccess is true, an entry whose access bit is currently set or
// that was touched within grace is left alone.
func (s *Store) evictRecord(ctx context.Context, rec cas.Record, reason string, checkAccess bool, grace time.Duration, now time.Time) bool {
	cur, err := s.cas.Get(ctx, rec.Key)
	if err != nil {
		return false
	}
	if cur.State != cas.Live {
		return false
	}
	e, err := parseEntry(cur.Value)
	if err != nil {
		return false
	}
	if checkAccess {
		if e.Access {
			return false
		}
		if grace > 0 && now.Sub(e.M.LastAccessAt) < grace {
			return false
		}
	}
	_, err = s.cas.Delete(ctx, cur.Key, cur.Revision)
	if err == nil {
		s.lastEvict.Store(time.Now().UnixNano())
		s.incCounter(ctx, metricEvictions, 1, labelReason, reason)
		return true
	}
	if errors.Is(err, cas.ErrConflict) {
		s.recordConflict(ctx, opEvict)
	}
	return false
}
