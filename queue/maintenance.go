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

// listCAS wraps cas.Store.List with queue-level list-page metrics.
func (q *Queue) listCAS(ctx context.Context, op, prefixTemplate string, opts *cas.ListOptions) (*cas.ListPage, error) {
	page, err := q.store.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	q.recordListPage(ctx, op, prefixTemplate)
	return page, nil
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
	contToken := ""
	for {
		opts := &s3backend.ListOptions{MaxKeys: 1000}
		if contToken != "" {
			opts.ContinuationToken = contToken
		}
		page, err := q.listWithRetry(ctx, "reaper", leasePrefixTemplate, leasePrefix, opts)
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
			q.reapExpiredLease(ctx, shard, jobID, expiryTS, now, &c)
		}
		if !page.IsTruncated {
			break
		}
		contToken = page.NextContinuationToken
		if contToken == "" {
			break
		}
	}

	// Backfill missing ready markers for pending jobs.
	q.backfillReadyMarkers(ctx, shard)

	// Backfill missing dead markers for dead jobs.
	q.backfillDeadMarkers(ctx, shard)

	// Best-effort depth counts.
	c.ready = q.countMarkers(ctx, shard, "ready")
	c.leased = q.countMarkers(ctx, shard, "lease")
	c.dead = q.countMarkers(ctx, shard, "dead")
	return c
}

// reapExpiredLease reclaims one expired lease, recreating the ready marker.
func (q *Queue) reapExpiredLease(ctx context.Context, shard uint16, jobID string, expiryTS time.Time, now time.Time, c *markerCounts) {
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
	// A marker whose timestamp does not match the canonical lease is stale
	// (e.g. the original marker left behind after a renew). Delete it without
	// reclaiming. If the canonical lease itself is still in the future, leave
	// the matching marker alone.
	if !expiryTS.Equal(env.Lease.Expiry) {
		q.deleteMarker(ctx, q.leaseMarkerKey(shard, expiryTS, jobID))
		c.deletedLease++
		return
	}
	if env.Lease.Expiry.After(deadline) {
		return
	}

	leaseToken := env.Lease.Token
	leaseExpiry := env.Lease.Expiry
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
		if curEnv.Lease.Token != leaseToken || !curEnv.Lease.Expiry.Equal(leaseExpiry) {
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
	if err := q.createMarker(ctx, q.readyMarkerKey(shard, newNotBefore, jobID)); err != nil {
		q.opts.Logger.Warn(err, "queue: reaper ready marker failed", "job", jobID)
	}
	q.deleteMarker(ctx, q.leaseMarkerKey(shard, env.Lease.Expiry, jobID))
	c.deletedLease++
}

// backfillReadyMarkers lists canonical jobs and ensures pending jobs have a
// ready marker.
func (q *Queue) backfillReadyMarkers(ctx context.Context, shard uint16) {
	prefix := "shard/" + shardHex(shard) + "/jobs/"
	startAfter := ""
	for {
		page, err := q.listCAS(ctx, "backfill", jobsPrefixTemplate, &cas.ListOptions{
			Prefix:     prefix,
			StartAfter: startAfter,
			MaxKeys:    500,
		})
		if err != nil {
			q.opts.Logger.Warn(err, "queue: reaper backfill list failed", "shard", shard)
			return
		}
		for _, rec := range page.Records {
			if rec.State != cas.Live {
				continue
			}
			env, err := decodeJob(rec.Value)
			if err != nil {
				continue
			}
			if env.State == statePublishing {
				// Resume a stale publishing job from a crashed prior attempt.
				q.resumePublishing(ctx, shard, env.ID, env)
				continue
			}
			if env.State != statePending {
				continue
			}
			jobID := env.ID
			if err := q.createMarker(ctx, q.readyMarkerKey(shard, env.NotBefore, jobID)); err != nil {
				q.opts.Logger.Warn(err, "queue: backfill ready marker failed", "job", jobID)
			}
		}
		if !page.IsTruncated {
			break
		}
		if len(page.Records) == 0 {
			break
		}
		startAfter = page.Records[len(page.Records)-1].Key
	}
}

// backfillDeadMarkers lists canonical jobs and ensures dead jobs have a dead
// marker.
func (q *Queue) backfillDeadMarkers(ctx context.Context, shard uint16) {
	prefix := "shard/" + shardHex(shard) + "/jobs/"
	startAfter := ""
	for {
		page, err := q.listCAS(ctx, "backfill", jobsPrefixTemplate, &cas.ListOptions{
			Prefix:     prefix,
			StartAfter: startAfter,
			MaxKeys:    500,
		})
		if err != nil {
			q.opts.Logger.Warn(err, "queue: reaper dead backfill list failed", "shard", shard)
			return
		}
		for _, rec := range page.Records {
			if rec.State != cas.Live {
				continue
			}
			env, err := decodeJob(rec.Value)
			if err != nil || env.State != stateDead || env.Dead == nil {
				continue
			}
			jobID := env.ID
			if err := q.createMarker(ctx, q.deadMarkerKey(shard, env.Dead.At, jobID)); err != nil {
				// A precondition failure means the marker already exists.
				if !errors.Is(err, s3backend.ErrPreconditionFailed) {
					q.opts.Logger.Warn(err, "queue: backfill dead marker failed", "job", jobID)
				}
			}
		}
		if !page.IsTruncated {
			break
		}
		if len(page.Records) == 0 {
			break
		}
		startAfter = page.Records[len(page.Records)-1].Key
	}
}

// countMarkers returns the number of raw marker objects of kind.
func (q *Queue) countMarkers(ctx context.Context, shard uint16, kind string) int {
	prefix := q.prefix + "shard/" + shardHex(shard) + "/" + kind + "/"
	prefixTemplate := markerKindPrefixTemplate(kind)
	page, err := q.listWithRetry(ctx, "reaper", prefixTemplate, prefix, &s3backend.ListOptions{MaxKeys: 1000})
	if err != nil {
		return 0
	}
	return len(page.Objects)
}

// markerKindPrefixTemplate returns the static metric prefix template for a
// raw marker kind.
func markerKindPrefixTemplate(kind string) string {
	switch kind {
	case "ready":
		return readyPrefixTemplate
	case "lease":
		return leasePrefixTemplate
	case "dead":
		return deadPrefixTemplate
	}
	return "queue/<name>/shard/<hhhh>/" + kind + "/"
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
	q.runPayloadGC(ctx)
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
		page, err := q.listCAS(ctx, "gc", jobsPrefixTemplate, &cas.ListOptions{
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
			case statePublishing:
				last := env.CreatedAt
				if env.PublishingAt != nil {
					last = *env.PublishingAt
				}
				if now.Sub(last) >= q.opts.PreparationTimeout {
					if err := q.purgeExternalJob(ctx, shard, jobID, rec, env); err != nil {
						q.opts.Logger.Warn(err, "queue: gc purge stale publishing failed", "job", jobID)
					}
					q.cleanMarkers(ctx, shard, jobID, env, &c)
				} else {
					q.resumePublishing(ctx, shard, jobID, env)
				}
			case statePurging:
				// Resume purge: remove ref then tombstone.
				if err := q.resumePurging(ctx, shard, jobID, rec, env); err != nil {
					q.opts.Logger.Warn(err, "queue: gc resume purging failed", "job", jobID)
				}
				q.cleanMarkers(ctx, shard, jobID, env, &c)
			case stateCompleted:
				if env.CompletedAt != nil && now.Sub(*env.CompletedAt) >= q.opts.CompletedRetention {
					if env.PayloadRef != nil {
						if err := q.purgeExternalJob(ctx, shard, jobID, rec, env); err != nil {
							q.opts.Logger.Warn(err, "queue: gc purge external completed failed", "job", jobID)
							continue
						}
					} else {
						if _, err := q.store.Delete(ctx, rec.Key, rec.Revision); err != nil {
							q.opts.Logger.Warn(err, "queue: gc delete completed failed", "job", jobID)
							continue
						}
					}
					q.cleanMarkers(ctx, shard, jobID, env, &c)
				}
			case stateDead:
				if env.Dead != nil && now.Sub(env.Dead.At) >= q.opts.DeadRetention {
					if env.PayloadRef != nil {
						if err := q.purgeExternalJob(ctx, shard, jobID, rec, env); err != nil {
							q.opts.Logger.Warn(err, "queue: gc purge external dead failed", "job", jobID)
							continue
						}
					} else {
						if _, err := q.store.Delete(ctx, rec.Key, rec.Revision); err != nil {
							q.opts.Logger.Warn(err, "queue: gc delete dead failed", "job", jobID)
							continue
						}
					}
					q.cleanMarkers(ctx, shard, jobID, env, &c)
				}
			}
		}
		if !page.IsTruncated {
			break
		}
		if len(page.Records) == 0 {
			break
		}
		startAfter = page.Records[len(page.Records)-1].Key
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
	prefixTemplate := markerKindPrefixTemplate(kind)
	contToken := ""
	for {
		opts := &s3backend.ListOptions{MaxKeys: 1000}
		if contToken != "" {
			opts.ContinuationToken = contToken
		}
		page, err := q.listWithRetry(ctx, "reaper", prefixTemplate, prefix, opts)
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
		if !page.IsTruncated {
			break
		}
		contToken = page.NextContinuationToken
		if contToken == "" {
			break
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
