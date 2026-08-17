package queue

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
)

// ListDeadOptions configures ListDead.
type ListDeadOptions struct {
	// StartAfter is an opaque cursor. When Shards contains exactly one
	// element, it refers to a marker suffix "dead/<ts>/<jobID>". When Shards
	// is empty it has the form "<shard>/<ts>/<jobID>".
	StartAfter string

	// Limit caps the number of items returned. Defaults to 1000.
	Limit int

	// Shards restricts the scan. Empty scans all shards; exactly one shard
	// enables efficient resumption with StartAfter.
	Shards []uint16
}

// DeadItem is a dead-lettered job returned by ListDead.
type DeadItem struct {
	// ID is the job identifier.
	ID string
	// Shard is the shard containing the job.
	Shard uint16
	// When is the dead-letter timestamp.
	When time.Time
	// Attempts is the number of delivery attempts before dead-lettering.
	Attempts int
	// Reason is the last recorded reason.
	Reason string
}

// ListDead returns dead-lettered jobs in ascending dead timestamp order.
// The returned next cursor should be passed as StartAfter on the next call.
func (q *Queue) ListDead(ctx context.Context, opts ListDeadOptions) ([]DeadItem, string, error) {
	start := time.Now()
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}
	now := q.now()

	var items []DeadItem
	var next string
	var err error
	if len(opts.Shards) == 1 {
		items, next, err = q.listDeadSingleShard(ctx, opts.Shards[0], opts.StartAfter, limit, now)
	} else {
		items, next, err = q.listDeadAllShards(ctx, opts.StartAfter, limit, now)
	}
	if err != nil {
		q.observeLatency(ctx, "list_dead", outcomeError, time.Since(start))
		q.recordEvent(ctx, "list_dead", outcomeError)
		return nil, "", err
	}
	q.observeLatency(ctx, "list_dead", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "list_dead", outcomeSuccess)
	return items, next, nil
}

func (q *Queue) listDeadSingleShard(ctx context.Context, shard uint16, startAfterSuffix string, limit int, now time.Time) ([]DeadItem, string, error) {
	prefix := q.prefix + "shard/" + shardHex(shard) + "/dead/"
	startAfter := ""
	if startAfterSuffix != "" {
		startAfter = prefix + startAfterSuffix
	}
	page, err := q.be.List(ctx, prefix, &s3backend.ListOptions{
		StartAfter: startAfter,
		MaxKeys:    limit,
	})
	if err != nil {
		return nil, "", err
	}
	items, err := q.deadMarkersToItems(ctx, page.Objects, shard, now)
	if err != nil {
		return nil, "", err
	}
	var next string
	if page.IsTruncated && len(page.Objects) > 0 {
		last := page.Objects[len(page.Objects)-1].Key
		if _, _, _, _, ok := q.parseMarker(last); ok {
			// Suffix after "dead/".
			rest := strings.TrimPrefix(last, prefix)
			next = rest
		}
	}
	return items, next, nil
}

func (q *Queue) listDeadAllShards(ctx context.Context, startAfterCursor string, limit int, now time.Time) ([]DeadItem, string, error) {
	startShard := uint16(0)
	startAfter := ""
	if startAfterCursor != "" {
		parts := strings.SplitN(startAfterCursor, "/", 3)
		if len(parts) == 3 {
			n, err := strconv.ParseUint(parts[0], 16, 16)
			if err == nil {
				startShard = uint16(n)
				startAfter = "dead/" + parts[1] + "/" + parts[2]
			}
		}
	}

	items := make([]DeadItem, 0, limit)
	lastCursor := ""
	for s := startShard; s < q.opts.Shards; s++ {
		prefix := q.prefix + "shard/" + shardHex(s) + "/dead/"
		sa := ""
		if startAfter != "" {
			sa = prefix + startAfter
			startAfter = ""
		}
		page, err := q.be.List(ctx, prefix, &s3backend.ListOptions{
			StartAfter: sa,
			MaxKeys:    limit - len(items),
		})
		if err != nil {
			return nil, "", err
		}
		chunk, err := q.deadMarkersToItems(ctx, page.Objects, s, now)
		if err != nil {
			return nil, "", err
		}
		items = append(items, chunk...)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].When.Equal(items[j].When) {
			return items[i].ID < items[j].ID
		}
		return items[i].When.Before(items[j].When)
	})
	if len(items) > limit {
		if limit > 0 {
			last := items[limit-1]
			lastCursor = shardHex(last.Shard) + "/" + ts20(last.When) + "/" + last.ID
		}
		items = items[:limit]
	}
	return items, lastCursor, nil
}

func (q *Queue) deadMarkersToItems(ctx context.Context, objects []s3backend.ObjectInfo, shard uint16, now time.Time) ([]DeadItem, error) {
	items := make([]DeadItem, 0, len(objects))
	for _, info := range objects {
		_, kind, when, jobID, ok := q.parseMarker(info.Key)
		if !ok || kind != "dead" {
			continue
		}
		appKey := jobAppKey(shard, jobID)
		rec, err := q.store.Get(ctx, appKey)
		if err != nil {
			if errors.Is(err, cas.ErrNotFound) {
				// Orphan marker; skip.
				continue
			}
			return nil, err
		}
		env, err := decodeJob(rec.Value)
		if err != nil {
			return nil, err
		}
		reason := ""
		if env.Dead != nil {
			reason = env.Dead.Reason
		}
		items = append(items, DeadItem{
			ID:       jobID,
			Shard:    shard,
			When:     when,
			Attempts: env.Attempts,
			Reason:   reason,
		})
	}
	return items, nil
}

// RequeueDead moves a dead-lettered job back to pending, recreates its ready
// marker, and deletes the dead marker. If the job is already pending or
// completed, the dead marker is removed and the call returns nil.
func (q *Queue) RequeueDead(ctx context.Context, jobID string, shard uint16) error {
	start := time.Now()
	appKey := jobAppKey(shard, jobID)
	rec, err := q.store.Get(ctx, appKey)
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			// Job gone; delete any dead marker best-effort.
			q.deleteDeadMarkerForJob(ctx, shard, jobID)
			q.observeLatency(ctx, "requeue_dead", outcomeSuccess, time.Since(start))
			q.recordEvent(ctx, "requeue_dead", outcomeSuccess)
			return nil
		}
		q.observeLatency(ctx, "requeue_dead", outcomeError, time.Since(start))
		q.recordEvent(ctx, "requeue_dead", outcomeError)
		return err
	}
	env, err := decodeJob(rec.Value)
	if err != nil {
		q.observeLatency(ctx, "requeue_dead", outcomeError, time.Since(start))
		q.recordEvent(ctx, "requeue_dead", outcomeError)
		return err
	}

	switch env.State {
	case stateDead:
		now := q.now()
		rec2, err := q.store.Update(ctx, appKey, func(ctx context.Context, cur cas.Record) ([]byte, error) {
			curEnv, err := decodeJob(cur.Value)
			if err != nil {
				return nil, err
			}
			if curEnv.State != stateDead {
				return cur.Value, nil
			}
			next := *curEnv
			next.State = statePending
			next.Lease = nil
			next.Dead = nil
			next.NotBefore = now
			next.Reasons = append(next.Reasons, reasonEnvelope{At: now, Reason: "requeued"})
			if len(next.Reasons) > q.opts.ReasonHistory {
				next.Reasons = next.Reasons[len(next.Reasons)-q.opts.ReasonHistory:]
			}
			return encodeJob(&next)
		})
		if err != nil {
			q.observeLatency(ctx, "requeue_dead", outcomeError, time.Since(start))
			q.recordEvent(ctx, "requeue_dead", outcomeError)
			return err
		}
		_ = rec2
		if err := q.putMarker(ctx, q.readyMarkerKey(shard, now, jobID), nil); err != nil {
			q.opts.Logger.Warn(err, "queue: requeue dead ready marker failed", "job", jobID)
		}
		q.deleteDeadMarkerForJob(ctx, shard, jobID)
	case statePending, stateClaimed:
		// Already active; just remove stale dead marker.
		q.deleteDeadMarkerForJob(ctx, shard, jobID)
	case stateCompleted:
		// Do not resurrect completed jobs; just clean marker.
		q.deleteDeadMarkerForJob(ctx, shard, jobID)
	}
	_ = rec
	q.observeLatency(ctx, "requeue_dead", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "requeue_dead", outcomeSuccess)
	return nil
}

// deleteDeadMarkerForJob best-effort deletes any dead marker for jobID in shard.
func (q *Queue) deleteDeadMarkerForJob(ctx context.Context, shard uint16, jobID string) {
	prefix := q.prefix + "shard/" + shardHex(shard) + "/dead/"
	page, err := q.be.List(ctx, prefix, &s3backend.ListOptions{MaxKeys: 100})
	if err != nil {
		return
	}
	for _, info := range page.Objects {
		_, _, _, id, ok := q.parseMarker(info.Key)
		if ok && id == jobID {
			q.deleteMarker(ctx, info.Key)
		}
	}
}
