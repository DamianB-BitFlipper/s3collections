package queue

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
)

// ListDeadOptions configures ListDead.
type ListDeadOptions struct {
	// StartAfter is an opaque cursor. When Shards contains exactly one
	// element, it refers to a marker suffix "<ts>/<jobID>" relative to the
	// shard's dead/ prefix. When Shards is empty, StartAfter is an opaque
	// JSON cursor produced by a previous all-shard ListDead call and must
	// not be synthesized by callers.
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
//
// Pagination protocol: pass the returned cursor as StartAfter on the next
// call. A non-empty page always comes with a non-empty cursor; the walk is
// complete when a call returns an empty page (the cursor is then "").
// An empty StartAfter begins a fresh walk. Listing never removes markers;
// a fresh walk over unchanged state returns the same items again.
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
	} else if len(opts.Shards) == 0 {
		items, next, err = q.listDeadAllShards(ctx, opts.StartAfter, limit, now)
	} else {
		err = errors.New("queue: ListDead supports exactly one shard or all shards")
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
	items := make([]DeadItem, 0, limit)
	var next string
	var lastSuffix string // suffix of the last marker page consumed
	opts := &s3backend.ListOptions{
		StartAfter: startAfter,
		MaxKeys:    limit,
	}
	if opts.MaxKeys < 1 {
		opts.MaxKeys = 1
	}
	for len(items) < limit {
		page, err := q.listWithRetry(ctx, "list_dead", deadPrefixTemplate, prefix, opts)
		if err != nil {
			return nil, "", err
		}
		chunk, err := q.deadMarkersToItems(ctx, page.Objects, shard, now)
		if err != nil {
			return nil, "", err
		}
		items = append(items, chunk...)
		if len(page.Objects) > 0 {
			lastSuffix = strings.TrimPrefix(page.Objects[len(page.Objects)-1].Key, prefix)
		}
		if !page.IsTruncated {
			break
		}
		if len(items) >= limit {
			// Enough items; cursor resumes after the last object in this page.
			next = lastSuffix
			break
		}
		// Continue with the opaque continuation token issued by the backend.
		opts.StartAfter = ""
		opts.ContinuationToken = page.NextContinuationToken
		opts.MaxKeys = limit - len(items)
		if opts.MaxKeys < 1 {
			opts.MaxKeys = 1
		}
	}
	if len(items) > limit {
		items = items[:limit]
	}
	if next == "" && len(items) > 0 {
		// Cursor protocol (see encodeDeadCursor): a non-empty page always
		// yields a non-empty cursor. The walk drained exactly at the end of
		// this page, so resume after the last consumed marker; replaying
		// that cursor returns an empty page with an empty cursor.
		next = lastSuffix
	}
	return items, next, nil
}

// deadCursorV1 is the JSON form of the opaque all-shards ListDead cursor.
// It records, for every shard that has been touched, the suffix of the last
// returned marker and (if already fetched but not yet returned) the next
// pending candidate. A shard marked done has no further markers. Shards
// absent from the map are started from the beginning.
type deadCursorV1 struct {
	Version int                        `json:"v"`
	Shards  map[string]deadCursorShard `json:"s,omitempty"`
}

type deadCursorShard struct {
	Done   bool            `json:"done,omitempty"`
	Suffix string          `json:"suffix,omitempty"`
	Next   *deadCursorNext `json:"next,omitempty"`
}

type deadCursorNext struct {
	When int64  `json:"t"`
	ID   string `json:"id"`
}

// deadShardIter pages one shard's dead/ prefix and yields DeadItem values
// in marker-key order. It never synthesizes backend continuation tokens.
type deadShardIter struct {
	q        *Queue
	ctx      context.Context
	shard    uint16
	prefix   string
	pageSize int

	startAfter string // used for the first page only
	contToken  string // opaque backend continuation token; empty means no further pages
	started    bool   // true once the first page has been fetched

	page       []s3backend.ObjectInfo
	pos        int
	lastSuffix string // suffix of the last consumed marker, empty if none
}

// fill fetches the next page if the current buffer is empty. It never
// synthesizes continuation tokens.
func (it *deadShardIter) fill() error {
	if it.pos < len(it.page) {
		return nil
	}
	if it.started && it.contToken == "" {
		return nil
	}
	opts := &s3backend.ListOptions{MaxKeys: it.pageSize}
	if it.contToken != "" {
		opts.ContinuationToken = it.contToken
	} else if it.startAfter != "" {
		opts.StartAfter = it.startAfter
	}
	page, err := it.q.listWithRetry(it.ctx, "list_dead", deadPrefixTemplate, it.prefix, opts)
	if err != nil {
		return err
	}
	it.started = true
	it.startAfter = ""
	it.page = page.Objects
	it.pos = 0
	if page.IsTruncated {
		it.contToken = page.NextContinuationToken
	} else {
		it.contToken = ""
	}
	return nil
}

// done reports whether this shard has no more candidates. An iterator with
// an unconsumed item in its current page is not done.
func (it *deadShardIter) done() bool {
	return it.started && it.pos >= len(it.page) && it.contToken == ""
}

// next returns the next valid dead item from this shard, skipping orphan or
// malformed markers. The iterator's lastSuffix is advanced past skipped keys
// so that resume points never revisit them.
func (it *deadShardIter) next() (DeadItem, bool, error) {
	for {
		if err := it.fill(); err != nil {
			return DeadItem{}, false, err
		}
		if it.pos >= len(it.page) {
			return DeadItem{}, false, nil
		}
		info := it.page[it.pos]
		it.pos++
		suffix := strings.TrimPrefix(info.Key, it.prefix)
		if suffix == info.Key {
			// Should not happen: key was returned under prefix.
			it.lastSuffix = ""
			continue
		}
		_, kind, when, jobID, ok := it.q.parseMarker(info.Key)
		if !ok || kind != "dead" {
			it.lastSuffix = suffix
			continue
		}
		appKey := jobAppKey(it.shard, jobID)
		rec, err := it.q.store.Get(it.ctx, appKey)
		if err != nil {
			if errors.Is(err, cas.ErrNotFound) {
				// Orphan marker; skip and advance past it.
				it.lastSuffix = suffix
				continue
			}
			return DeadItem{}, false, err
		}
		env, err := decodeJob(rec.Value)
		if err != nil {
			return DeadItem{}, false, err
		}
		reason := ""
		if env.Dead != nil {
			reason = env.Dead.Reason
		}
		it.lastSuffix = suffix
		return DeadItem{
			ID:       jobID,
			Shard:    it.shard,
			When:     when,
			Attempts: env.Attempts,
			Reason:   reason,
		}, true, nil
	}
}

// deadHeapNode is one candidate in the k-way merge.
type deadHeapNode struct {
	item DeadItem
	it   *deadShardIter
}

type deadMergeHeap []deadHeapNode

func (h deadMergeHeap) Len() int { return len(h) }
func (h deadMergeHeap) Less(i, j int) bool {
	if h[i].item.When.Equal(h[j].item.When) {
		return h[i].item.ID < h[j].item.ID
	}
	return h[i].item.When.Before(h[j].item.When)
}
func (h deadMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *deadMergeHeap) Push(x any)   { *h = append(*h, x.(deadHeapNode)) }
func (h *deadMergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (q *Queue) listDeadAllShards(ctx context.Context, cursorStr string, limit int, now time.Time) ([]DeadItem, string, error) {
	shardCursors, err := q.parseDeadCursor(cursorStr)
	if err != nil {
		return nil, "", err
	}
	// One item per page keeps the per-shard iterator state simple: the
	// cursor can record a single pending candidate per shard without also
	// having to snapshot the rest of a partially consumed page.
	const pageSize = 1

	iters := make([]*deadShardIter, q.opts.Shards)
	for s := uint16(0); s < q.opts.Shards; s++ {
		sc := shardCursors[s]
		prefix := q.prefix + "shard/" + shardHex(s) + "/dead/"
		var startAfter string
		if sc.suffix != "" {
			startAfter = prefix + sc.suffix
		}
		iters[s] = &deadShardIter{
			q:          q,
			ctx:        ctx,
			shard:      s,
			prefix:     prefix,
			pageSize:   pageSize,
			startAfter: startAfter,
		}
		if sc.done {
			iters[s].started = true
			iters[s].contToken = ""
			iters[s].page = nil
			iters[s].pos = 0
		}
	}

	h := &deadMergeHeap{}
	heap.Init(h)
	for s, it := range iters {
		sc := shardCursors[s]
		if sc.pending != nil && !sc.done {
			// The pending candidate was fetched from S3 by the previous
			// call but not returned (a heap node surviving the limit cut).
			// Position the iterator past it so the next fill() resumes
			// after the pending key rather than re-fetching it.
			pendSuffix := ts20(sc.pending.When) + "/" + sc.pending.ID
			it.startAfter = it.prefix + pendSuffix
			it.lastSuffix = pendSuffix
			heap.Push(h, deadHeapNode{
				item: *sc.pending,
				it:   it,
			})
			continue
		}
		if it.done() {
			continue
		}
		item, ok, err := it.next()
		if err != nil {
			return nil, "", err
		}
		if ok {
			heap.Push(h, deadHeapNode{item: item, it: it})
		}
	}

	items := make([]DeadItem, 0, limit)
	for h.Len() > 0 && len(items) < limit {
		node := heap.Pop(h).(deadHeapNode)
		items = append(items, node.item)
		nextItem, ok, err := node.it.next()
		if err != nil {
			return nil, "", err
		}
		if ok {
			heap.Push(h, deadHeapNode{item: nextItem, it: node.it})
		}
	}

	var nextCursor string
	if len(items) > 0 {
		nextCursor, err = q.encodeDeadCursor(iters, h)
		if err != nil {
			return nil, "", err
		}
	}
	return items, nextCursor, nil
}

type shardCursor struct {
	suffix  string
	done    bool
	pending *DeadItem
}

// parseDeadCursor decodes an all-shards ListDead cursor. An empty cursor
// means "start from the beginning".
func (q *Queue) parseDeadCursor(cursorStr string) ([]shardCursor, error) {
	out := make([]shardCursor, q.opts.Shards)
	if cursorStr == "" {
		return out, nil
	}
	var c deadCursorV1
	if err := json.Unmarshal([]byte(cursorStr), &c); err != nil {
		return nil, fmt.Errorf("queue: invalid ListDead cursor: %w", err)
	}
	if c.Version != 1 {
		return nil, fmt.Errorf("queue: unsupported ListDead cursor version %d", c.Version)
	}
	for shardStr, sc := range c.Shards {
		n, err := strconv.ParseUint(shardStr, 16, 16)
		if err != nil || n >= uint64(q.opts.Shards) {
			return nil, fmt.Errorf("queue: invalid shard in ListDead cursor: %q", shardStr)
		}
		if sc.Suffix != "" {
			parts := strings.Split(sc.Suffix, "/")
			if len(parts) != 2 || len(parts[0]) != 20 || parts[1] == "" {
				return nil, fmt.Errorf("queue: invalid suffix in ListDead cursor: %q", sc.Suffix)
			}
			if _, err := strconv.ParseInt(parts[0], 10, 64); err != nil {
				return nil, fmt.Errorf("queue: invalid suffix in ListDead cursor: %q", sc.Suffix)
			}
		}
		if sc.Done && sc.Next != nil {
			return nil, fmt.Errorf("queue: invalid ListDead cursor: shard %q both done and pending", shardStr)
		}
		var pending *DeadItem
		if sc.Next != nil {
			if sc.Next.ID == "" || sc.Next.When < 0 {
				return nil, fmt.Errorf("queue: invalid next candidate in ListDead cursor")
			}
			pending = &DeadItem{
				ID:    sc.Next.ID,
				Shard: uint16(n),
				When:  time.UnixMicro(sc.Next.When).UTC(),
			}
		}
		out[n] = shardCursor{suffix: sc.Suffix, done: sc.Done, pending: pending}
	}
	return out, nil
}

// encodeDeadCursor builds the opaque cursor that records how far each shard
// advanced, including shards whose next candidate was fetched but not yet
// returned. A non-empty page always produces a non-empty cursor, even when
// every touched shard is fully drained; replaying that terminal cursor yields
// an empty page with an empty cursor. An empty cursor is returned only when
// the page itself contained no items at all.
func (q *Queue) encodeDeadCursor(iters []*deadShardIter, h *deadMergeHeap) (string, error) {
	pending := make(map[uint16]DeadItem, h.Len())
	for _, node := range *h {
		pending[node.it.shard] = node.item
	}
	m := make(map[string]deadCursorShard, len(iters))
	for _, it := range iters {
		key := shardHex(it.shard)
		pend, hasPending := pending[it.shard]
		touched := it.started || hasPending || it.lastSuffix != ""
		if !touched {
			continue
		}
		if it.done() && !hasPending {
			m[key] = deadCursorShard{Done: true}
			continue
		}
		sc := deadCursorShard{Suffix: it.lastSuffix}
		if hasPending {
			sc.Next = &deadCursorNext{When: pend.When.UnixMicro(), ID: pend.ID}
		}
		m[key] = sc
	}
	if len(m) == 0 {
		return "", nil
	}
	c := deadCursorV1{Version: 1, Shards: m}
	b, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("queue: encode ListDead cursor: %w", err)
	}
	return string(b), nil
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
		if err := q.createMarker(ctx, q.readyMarkerKey(shard, now, jobID)); err != nil {
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
	token := ""
	for {
		page, err := q.listWithRetry(ctx, "list_dead", deadPrefixTemplate, prefix, &s3backend.ListOptions{
			ContinuationToken: token,
			MaxKeys:           1000,
		})
		if err != nil {
			return
		}
		for _, info := range page.Objects {
			_, _, _, id, ok := q.parseMarker(info.Key)
			if ok && id == jobID {
				q.deleteMarker(ctx, info.Key)
			}
		}
		if !page.IsTruncated {
			return
		}
		token = page.NextContinuationToken
		if token == "" {
			return
		}
	}
}
