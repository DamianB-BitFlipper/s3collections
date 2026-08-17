package queue

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
)

// markerCounts holds depth and deletion counts for a maintenance pass.
type markerCounts struct {
	ready, leased, dead int
	deletedLease        int
	deletedReady        int
	deletedDead         int
}

// StartMaintenance starts a single background goroutine that runs reaper and
// GC passes until ctx is done. It is idempotent: repeated calls are no-ops
// while the loop is already running. Every queue replica should run it.
func (q *Queue) StartMaintenance(ctx context.Context) {
	if !q.maintenanceOn.CompareAndSwap(false, true) {
		return
	}
	go q.maintenanceLoop(ctx)
}

func (q *Queue) maintenanceLoop(ctx context.Context) {
	defer q.maintenanceOn.Store(false)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	jitter := func(d time.Duration) time.Duration {
		return d + time.Duration(rnd.Float64()*float64(d))
	}
	now := time.Now().UTC()
	nextReaper := now.Add(jitter(q.opts.ReaperInterval))
	nextGC := now.Add(jitter(q.opts.GCInterval))
	timer := time.NewTimer(time.Until(nextReaper))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		now = time.Now().UTC()
		if !now.Before(nextReaper) {
			q.reaperPass(ctx)
			nextReaper = now.Add(jitter(q.opts.ReaperInterval))
		}
		if !now.Before(nextGC) {
			q.gcPass(ctx)
			nextGC = now.Add(jitter(q.opts.GCInterval))
		}
		next := nextReaper
		if nextGC.Before(next) {
			next = nextGC
		}
		timer.Reset(time.Until(next))
	}
}

// reaperPass reclaims expired leases and backfills missing markers.
func (q *Queue) reaperPass(ctx context.Context) {
	start := time.Now()
	q.recordReaperRun(ctx)
	var readyTotal, leasedTotal, deadTotal atomic.Int64
	var deletedLease, deletedReady, deletedDead atomic.Int64
	for s := uint16(0); s < q.opts.Shards; s++ {
		c := q.reapShard(ctx, s)
		readyTotal.Add(int64(c.ready))
		leasedTotal.Add(int64(c.leased))
		deadTotal.Add(int64(c.dead))
		deletedLease.Add(int64(c.deletedLease))
		deletedReady.Add(int64(c.deletedReady))
		deletedDead.Add(int64(c.deletedDead))
	}
	q.setDepth(ctx, "ready", float64(readyTotal.Load()))
	q.setDepth(ctx, "leased", float64(leasedTotal.Load()))
	q.setDepth(ctx, "dead", float64(deadTotal.Load()))
	for i := int64(0); i < deletedLease.Load(); i++ {
		q.recordReaperDeleted(ctx, "lease")
	}
	for i := int64(0); i < deletedReady.Load(); i++ {
		q.recordReaperDeleted(ctx, "ready")
	}
	for i := int64(0); i < deletedDead.Load(); i++ {
		q.recordReaperDeleted(ctx, "dead")
	}
	q.observeLatency(ctx, "reaper", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "reap", outcomeSuccess)
}

// reapShard processes one shard. It returns approximate ready, leased, and
// dead marker counts plus deleted marker counts.
func (q *Queue) reapShard(ctx context.Context, shard uint16) markerCounts {
	var c markerCounts
	now := q.now()
	deadline := now.Add(q.opts.ClockSkewTolerance)

	// Expired leases.
	leasePrefix := q.prefix + "shard/" + shardHex(shard) + "/lease/"
	startAfter := ""
	for {
		page, err := q.be.List(ctx, leasePrefix, &s3backend.ListOptions{
			StartAfter: startAfter,
			MaxKeys:    1000,
		})
		if err != nil {
			q.opts.Logger.Warn(err, "queue: reaper lease list failed", "shard", shard)
			break
		}
		for _, info := range page.Objects {
			_, kind, expiryTS, jobID, ok := q.parseMarker(info.Key)
			if !ok || kind != "lease" {
				continue
			}
			if expiryTS.After(deadline) {
				// No more expired markers in sorted order.
				page.IsTruncated = false
				break
			}
			q.reapExpiredLease(ctx, shard, jobID, now, &c)
		}
		if !page.IsTruncated {
			break
		}
		startAfter = page.NextContinuationToken
		if startAfter == "" {
			break
		}
	}

	// Backfill missing ready markers for pending jobs.
	q.backfillReadyMarkers(ctx, shard)

	// Best-effort depth counts.
	c.ready = q.countMarkers(ctx, shard, "ready")
	c.leased = q.countMarkers(ctx, shard, "lease")
	c.dead = q.countMarkers(ctx, shard, "dead")
	return c
}

// reapExpiredLease reclaims one expired lease, recreating the ready marker.
func (q *Queue) reapExpiredLease(ctx context.Context, shard uint16, jobID string, now time.Time, c *markerCounts) {
	appKey := jobAppKey(shard, jobID)
	rec, err := q.store.Get(ctx, appKey)
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			// Job gone; marker is orphan and will be cleaned by GC.
		}
		return
	}
	env, err := decodeJob(rec.Value)
	if err != nil {
		return
	}
	if env.State != stateClaimed || env.Lease == nil {
		return
	}
	deadline := now.Add(q.opts.ClockSkewTolerance)
	if env.Lease.Expiry.After(deadline) {
		// Lease has been renewed; stale marker.
		q.deleteMarker(ctx, q.leaseMarkerKey(shard, env.Lease.Expiry, jobID))
		c.deletedLease++
		return
	}

	newNotBefore := env.NotBefore
	if now.After(newNotBefore) {
		newNotBefore = now
	}
	_, err = q.store.Update(ctx, appKey, func(ctx context.Context, cur cas.Record) ([]byte, error) {
		curEnv, err := decodeJob(cur.Value)
		if err != nil {
			return nil, err
		}
		if curEnv.State != stateClaimed || curEnv.Lease == nil {
			return cur.Value, nil
		}
		if curEnv.Lease.Expiry.After(deadline) {
			return cur.Value, nil
		}
		next := *curEnv
		next.State = statePending
		next.Lease = nil
		next.NotBefore = curEnv.NotBefore
		if q.now().After(next.NotBefore) {
			next.NotBefore = q.now()
		}
		return encodeJob(&next)
	})
	if err != nil {
		q.opts.Logger.Warn(err, "queue: reaper CAS failed", "job", jobID)
		return
	}
	if err := q.putMarker(ctx, q.readyMarkerKey(shard, newNotBefore, jobID), nil); err != nil {
		q.opts.Logger.Warn(err, "queue: reaper ready marker failed", "job", jobID)
	}
	q.deleteMarker(ctx, q.leaseMarkerKey(shard, env.Lease.Expiry, jobID))
	c.deletedLease++
}

// backfillReadyMarkers lists canonical jobs and ensures pending jobs have a
// ready marker.
func (q *Queue) backfillReadyMarkers(ctx context.Context, shard uint16) {
	prefix := "shard/" + shardHex(shard) + "/jobs/"
	page, err := q.store.List(ctx, &cas.ListOptions{Prefix: prefix, MaxKeys: 500})
	if err != nil {
		q.opts.Logger.Warn(err, "queue: reaper backfill list failed", "shard", shard)
		return
	}
	for _, rec := range page.Records {
		if rec.State != cas.Live {
			continue
		}
		env, err := decodeJob(rec.Value)
		if err != nil || env.State != statePending {
			continue
		}
		jobID := env.ID
		if err := q.putMarker(ctx, q.readyMarkerKey(shard, env.NotBefore, jobID), nil); err != nil {
			q.opts.Logger.Warn(err, "queue: backfill ready marker failed", "job", jobID)
		}
	}
}

// countMarkers returns the number of raw marker objects of kind.
func (q *Queue) countMarkers(ctx context.Context, shard uint16, kind string) int {
	prefix := q.prefix + "shard/" + shardHex(shard) + "/" + kind + "/"
	page, err := q.be.List(ctx, prefix, &s3backend.ListOptions{MaxKeys: 1000})
	if err != nil {
		return 0
	}
	return len(page.Objects)
}

// gcPass removes completed/dead jobs past retention and cleans orphan markers.
func (q *Queue) gcPass(ctx context.Context) {
	start := time.Now()
	q.recordReaperRun(ctx)
	var deletedLease, deletedReady, deletedDead atomic.Int64
	for s := uint16(0); s < q.opts.Shards; s++ {
		c := q.gcShard(ctx, s)
		deletedLease.Add(int64(c.deletedLease))
		deletedReady.Add(int64(c.deletedReady))
		deletedDead.Add(int64(c.deletedDead))
	}
	for i := int64(0); i < deletedLease.Load(); i++ {
		q.recordReaperDeleted(ctx, "lease")
	}
	for i := int64(0); i < deletedReady.Load(); i++ {
		q.recordReaperDeleted(ctx, "ready")
	}
	for i := int64(0); i < deletedDead.Load(); i++ {
		q.recordReaperDeleted(ctx, "dead")
	}
	q.observeLatency(ctx, "gc", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "gc", outcomeSuccess)
}

// gcShard processes one shard for GC.
func (q *Queue) gcShard(ctx context.Context, shard uint16) markerCounts {
	var c markerCounts
	now := q.now()
	prefix := "shard/" + shardHex(shard) + "/jobs/"
	startAfter := ""
	for {
		page, err := q.store.List(ctx, &cas.ListOptions{
			Prefix:     prefix,
			StartAfter: startAfter,
			MaxKeys:    500,
		})
		if err != nil {
			q.opts.Logger.Warn(err, "queue: gc list failed", "shard", shard)
			return c
		}
		for _, rec := range page.Records {
			jobID, ok := jobIDFromAppKey(rec.Key)
			if !ok {
				continue
			}
			if rec.State == cas.Tombstone {
				// Physical deletion is handled by the cas layer's GC.
				continue
			}
			env, err := decodeJob(rec.Value)
			if err != nil {
				continue
			}
			switch env.State {
			case stateCompleted:
				if env.CompletedAt != nil && now.Sub(*env.CompletedAt) >= q.opts.CompletedRetention {
					if _, err := q.store.Delete(ctx, rec.Key, rec.Revision); err != nil {
						q.opts.Logger.Warn(err, "queue: gc delete completed failed", "job", jobID)
						continue
					}
					q.cleanMarkers(ctx, shard, jobID, env, &c)
				}
			case stateDead:
				if now.Sub(env.Dead.At) >= q.opts.DeadRetention {
					if _, err := q.store.Delete(ctx, rec.Key, rec.Revision); err != nil {
						q.opts.Logger.Warn(err, "queue: gc delete dead failed", "job", jobID)
						continue
					}
					q.cleanMarkers(ctx, shard, jobID, env, &c)
				}
			}
		}
		if !page.IsTruncated {
			break
		}
		startAfter = page.NextContinuationToken
		if startAfter == "" {
			break
		}
	}

	// Orphan marker cleanup.
	for _, kind := range []string{"ready", "lease", "dead"} {
		q.cleanOrphanMarkers(ctx, shard, kind, &c)
	}
	return c
}

// cleanMarkers removes ready/lease markers for a job that is being deleted.
func (q *Queue) cleanMarkers(ctx context.Context, shard uint16, jobID string, env *jobEnvelope, c *markerCounts) {
	q.deleteMarker(ctx, q.readyMarkerKey(shard, env.NotBefore, jobID))
	c.deletedReady++
	if env.Lease != nil {
		q.deleteMarker(ctx, q.leaseMarkerKey(shard, env.Lease.Expiry, jobID))
		c.deletedLease++
	}
	if env.Dead != nil {
		q.deleteMarker(ctx, q.deadMarkerKey(shard, env.Dead.At, jobID))
		c.deletedDead++
	}
}

// cleanOrphanMarkers removes markers whose canonical job is missing or in an
// inconsistent state.
func (q *Queue) cleanOrphanMarkers(ctx context.Context, shard uint16, kind string, c *markerCounts) {
	prefix := q.prefix + "shard/" + shardHex(shard) + "/" + kind + "/"
	page, err := q.be.List(ctx, prefix, &s3backend.ListOptions{MaxKeys: 1000})
	if err != nil {
		return
	}
	for _, info := range page.Objects {
		_, mk, _, jobID, ok := q.parseMarker(info.Key)
		if !ok {
			continue
		}
		appKey := jobAppKey(shard, jobID)
		rec, err := q.store.Get(ctx, appKey)
		if err != nil {
			if errors.Is(err, cas.ErrNotFound) {
				q.deleteMarker(ctx, info.Key)
			}
			continue
		}
		env, err := decodeJob(rec.Value)
		if err != nil {
			q.deleteMarker(ctx, info.Key)
			continue
		}
		switch mk {
		case "ready":
			if env.State != statePending {
				q.deleteMarker(ctx, info.Key)
				c.deletedReady++
			}
		case "lease":
			if env.State != stateClaimed || env.Lease == nil {
				q.deleteMarker(ctx, info.Key)
				c.deletedLease++
			}
		case "dead":
			if env.State != stateDead {
				q.deleteMarker(ctx, info.Key)
				c.deletedDead++
			}
		}
	}
}

// jobIDFromAppKey extracts the job id from a cas application key.
func jobIDFromAppKey(appKey string) (string, bool) {
	parts := strings.Split(appKey, "/")
	if len(parts) != 4 || parts[0] != "shard" || parts[2] != "jobs" {
		return "", false
	}
	return parts[3], true
}

// parseShardAppKey returns the shard encoded in a cas app key.
func parseShardAppKey(appKey string) (uint16, bool) {
	parts := strings.Split(appKey, "/")
	if len(parts) < 2 || parts[0] != "shard" {
		return 0, false
	}
	n, err := strconv.ParseUint(parts[1], 16, 16)
	if err != nil {
		return 0, false
	}
	return uint16(n), true
}
