package lru

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

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

// shardPage is the result of listing one page of a shard directly from the
// backend. Using the backend avoids the global-store pagination that
// cas.List performs.
type shardPage struct {
	Records               []cas.Record
	IsTruncated           bool
	NextContinuationToken string
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
			continue
		}

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
			_ = s.clearAccess(ctx, rec, e, now)
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

	// Update approximate gauges.
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

// listShard pages the backend directly for objects under the shard prefix.
// The continuation token is a backend object key, which avoids the global
// pagination issues of cas.List.
func (s *Store) listShard(ctx context.Context, shard int, continuation string) (shardPage, error) {
	prefix := s.opts.Prefix + fmt.Sprintf("entries/%s/", s.shardStr(shard))
	opts := &s3backend.ListOptions{
		ContinuationToken: continuation,
		MaxKeys:           pageSize(s.opts.EvictorBatchSize),
	}
	page, err := s.backend.List(ctx, prefix, opts)
	if err != nil {
		return shardPage{}, err
	}
	s.incCounter(ctx, metricListPages, 1, "op", "list", "prefix", "lru/entries/<shard>/")

	records := make([]cas.Record, 0, len(page.Objects))
	for _, info := range page.Objects {
		appKey, err := s.decodeObjectKey(info.Key)
		if err != nil {
			s.incCounter(ctx, metricCorrupt, 1)
			continue
		}
		rec, err := s.cas.GetMeta(ctx, appKey)
		if err != nil {
			s.incCounter(ctx, metricCorrupt, 1)
			continue
		}
		records = append(records, rec)
	}

	out := shardPage{
		Records:     records,
		IsTruncated: page.IsTruncated,
	}
	if page.IsTruncated && len(page.Objects) > 0 {
		out.NextContinuationToken = page.Objects[len(page.Objects)-1].Key
	}
	return out, nil
}

// shardUsage returns live count, tombstone count, and live bytes for a shard.
func (s *Store) shardUsage(ctx context.Context, shard int) (int64, int64, int64, error) {
	prefix := s.opts.Prefix + fmt.Sprintf("entries/%s/", s.shardStr(shard))
	var live, tomb, bytes int64
	token := ""
	for {
		page, err := s.backend.List(ctx, prefix, &s3backend.ListOptions{
			ContinuationToken: token,
			MaxKeys:           pageSize(s.opts.ListPageSize),
		})
		s.incCounter(ctx, metricListPages, 1, "op", "list", "prefix", "lru/entries/<shard>/")
		if err != nil {
			return 0, 0, 0, err
		}
		for _, info := range page.Objects {
			appKey, err := s.decodeObjectKey(info.Key)
			if err != nil {
				continue
			}
			rec, err := s.cas.GetMeta(ctx, appKey)
			if err != nil {
				continue
			}
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
