package queue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
	"github.com/damianb/s3collections/tree"
)

// EnqueueOptions configures a single Enqueue call.
type EnqueueOptions struct {
	// IdempotencyKey, when non-empty, produces a deterministic job id. A
	// repeated enqueue with the same key returns existed=true while the
	// canonical object exists (including as a tombstone within retention).
	IdempotencyKey string

	// Shard pins the job to a specific shard. If nil, the shard is derived
	// from the job id.
	Shard *uint16

	// Delay defers claimability until now+Delay.
	Delay time.Duration
}

// Enqueue stores payload as a new pending job. With IdempotencyKey it returns
// existed=true if the canonical job object already exists, without creating a
// duplicate. The returned job id is deterministic for idempotent enqueues.
//
// Payloads larger than InlinePayloadBytes are automatically offloaded to the
// payload tree; the canonical job envelope stores only a small descriptor.
//
// In sequencer mode the job id is "<seq20>-<rand16hex>" (or
// "<seq20>-idem-<hash>" for idempotent jobs) so that ids remain lexicographically
// ordered by sequence number.
func (q *Queue) Enqueue(ctx context.Context, payload []byte, opts EnqueueOptions) (jobID string, existed bool, err error) {
	if len(payload) > q.opts.MaxPayloadBytes {
		return "", false, fmt.Errorf("%w: %w", ErrPayloadTooLarge, cas.ErrTooLarge)
	}

	// For inline-sized payloads, store directly. For larger ones, offload
	// via the external path using the payload hash.
	if len(payload) <= q.opts.InlinePayloadBytes {
		return q.enqueueInline(ctx, payload, opts)
	}

	// Compute the payload hash and delegate to the external path.
	sum := sha256.Sum256(payload)
	payloadID := hex.EncodeToString(sum[:])
	return q.enqueueExternal(ctx, bytes.NewReader(payload), int64(len(payload)), payloadID, opts)
}

// EnqueueReader stores a payload read from r as a new pending job. The payload
// is always offloaded to the payload tree; the canonical job envelope is
// metadata-only. size is the exact number of bytes r will yield; r is never
// closed by this method. The queue bounded-spools and hashes the payload
// before uploading it to the payload tree.
//
// The returned Job.Payload is nil for externally stored jobs; use OpenPayload
// to stream the payload on demand.
func (q *Queue) EnqueueReader(ctx context.Context, r io.Reader, size int64, opts EnqueueOptions) (jobID string, existed bool, err error) {
	start := time.Now()
	defer func() {
		out := outcomeSuccess
		if err != nil {
			out = outcomeError
		}
		q.observeLatency(ctx, "enqueue_reader", out, time.Since(start))
		q.recordEvent(ctx, "enqueue_reader", out)
	}()
	if r == nil {
		return "", false, fmt.Errorf("queue: nil reader")
	}
	if size < 0 || size > int64(q.opts.MaxPayloadBytes) {
		return "", false, fmt.Errorf("%w: %w: size %d", ErrPayloadTooLarge, cas.ErrTooLarge, size)
	}
	if !q.backendSupportsStreaming() {
		return "", false, ErrBackendCapability
	}

	digest, n, f, cleanup, e := spoolAndHash(ctx, r, size)
	if e != nil {
		return "", false, e
	}
	defer cleanup()
	if n != size {
		return "", false, fmt.Errorf("queue: payload size: got %d want %d", n, size)
	}
	payloadID := PayloadID(digest)

	jobID, existed, err = q.enqueueExternal(ctx, f, size, payloadID, opts)
	return jobID, existed, err
}

// enqueueInline creates a job with inline (base64) payload stored in CAS.
func (q *Queue) enqueueInline(ctx context.Context, payload []byte, opts EnqueueOptions) (jobID string, existed bool, err error) {
	start := time.Now()
	if len(payload) > q.opts.InlinePayloadBytes {
		return "", false, ErrPayloadTooLarge
	}

	jobID, err = q.newJobID(ctx, opts.IdempotencyKey)
	if err != nil {
		q.observeLatency(ctx, "enqueue", outcomeError, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeError)
		return "", false, fmt.Errorf("queue: job id generation failed: %w", err)
	}

	shard := q.resolveShard(jobID, opts.Shard)
	appKey := jobAppKey(shard, jobID)
	now := q.now()
	notBefore := now.Add(opts.Delay)
	env := newInlineJobEnvelope(jobID, q.name, shard, payload, now, notBefore)
	body, err := encodeJob(env)
	if err != nil {
		q.observeLatency(ctx, "enqueue", outcomeError, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeError)
		return "", false, fmt.Errorf("queue: encode job: %w", err)
	}

	_, err = q.store.Create(ctx, appKey, body)
	if err == nil {
		if err := q.createMarker(ctx, q.readyMarkerKey(shard, notBefore, jobID)); err != nil {
			q.opts.Logger.Warn(err, "queue: ready marker create failed", "job", jobID)
		}
		q.observeLatency(ctx, "enqueue", outcomeSuccess, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeSuccess)
		return jobID, false, nil
	}
	if errors.Is(err, cas.ErrAlreadyExists) {
		q.observeLatency(ctx, "enqueue", outcomeSuccess, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeSuccess)
		return jobID, true, nil
	}
	q.observeLatency(ctx, "enqueue", outcomeError, time.Since(start))
	q.recordEvent(ctx, "enqueue", outcomeError)
	return "", false, err
}

// enqueueExternal creates a job with an external payload stored in the
// payload tree. It follows the durable publication protocol:
//  1. Create a publishing-state canonical record (establishes idempotent ownership)
//  2. PutBlob (hash/spool/upload outside gate)
//  3. CommitRoot containing the BlobRef
//  4. CreateRef with deterministic name to the root (durable GC root)
//  5. CAS publishing -> pending with payload descriptor
//  6. Create ready marker
//
// A crash before step 5 leaves a discoverable publishing record, never an
// unrooted published job. On error after step 1, we best-effort drive the
// publishing record toward purging immediately.
func (q *Queue) enqueueExternal(ctx context.Context, r io.Reader, size int64, expectedID PayloadID, opts EnqueueOptions) (jobID string, existed bool, err error) {
	start := time.Now()
	prepToken := randomToken()

	jobID, err = q.newJobID(ctx, opts.IdempotencyKey)
	if err != nil {
		return "", false, fmt.Errorf("queue: job id generation failed: %w", err)
	}

	shard := q.resolveShard(jobID, opts.Shard)
	appKey := jobAppKey(shard, jobID)
	now := q.now()
	notBefore := now.Add(opts.Delay)
	publicationCreated := now

	// Step 1: Create the publishing record.
	pubEnv := newPublishingJobEnvelope(jobID, q.name, shard, prepToken, PayloadInfo{ID: expectedID, Size: size}, now, notBefore)
	pubBody, err := encodeJob(pubEnv)
	if err != nil {
		return "", false, fmt.Errorf("queue: encode publishing job: %w", err)
	}
	_, createErr := q.store.Create(ctx, appKey, pubBody)
	if createErr != nil {
		if !errors.Is(createErr, cas.ErrAlreadyExists) {
			return "", false, fmt.Errorf("queue: create publishing job: %w", createErr)
		}
		rec, getErr := q.store.Get(ctx, appKey)
		if errors.Is(getErr, cas.ErrNotFound) {
			return jobID, true, nil
		}
		if getErr != nil {
			return "", false, fmt.Errorf("queue: idempotent enqueue readback failed: %w", getErr)
		}
		existing, decErr := decodeJob(rec.Value)
		if decErr != nil {
			return "", false, decErr
		}
		if existing.State != statePublishing || existing.PayloadInfo.ID != expectedID || existing.PayloadInfo.Size != size {
			return jobID, true, nil
		}
		last := existing.CreatedAt
		if existing.PublishingAt != nil {
			last = *existing.PublishingAt
		}
		if q.now().Sub(last) < q.opts.PreparationTimeout {
			return jobID, true, nil
		}
		publicationCreated = existing.CreatedAt
		notBefore = existing.NotBefore
		oldRefName := ownerRefName(shard, jobID, existing.PrepToken)
		// Fence the old generation's ref name before forgetting its token. An
		// absent ref is materialized as a tombstoned sentinel, so a late CreateRef cannot leak.
		if e := q.releaseOwnerRef(ctx, oldRefName, 0); e != nil {
			return "", false, fmt.Errorf("queue: fence stale publication ref: %w", e)
		}
		takeover := *existing
		takeover.PrepToken = prepToken
		nowTouch := q.now()
		takeover.PublishingAt = &nowTouch
		takeBody, _ := encodeJob(&takeover)
		if _, e := q.store.CompareAndSwap(ctx, appKey, rec.Revision, takeBody); e != nil {
			return jobID, true, nil
		}
	}
	stopHeartbeat := q.startPublishingHeartbeat(ctx, appKey, prepToken)
	defer func() { _ = stopHeartbeat() }()
	// Step 2: PutBlob - hash and upload the payload to the payload tree.
	// This happens outside the tree mutation gate (refactored tree.PutBlob).
	blobRef, err := q.payloads.PutBlob(ctx, expectedID, r,
		tree.WithExpectedBlobSize(size),
	)
	if err != nil {
		q.cleanupPublishingOnFailure(ctx, shard, jobID, appKey, prepToken)
		if errors.Is(err, tree.ErrBackendCapability) {
			return "", false, fmt.Errorf("%w: %v", ErrBackendCapability, err)
		}
		return "", false, fmt.Errorf("queue: put payload blob: %w", err)
	}

	// Step 3: CommitRoot containing the BlobRef.
	nodeID, err := q.payloads.CommitRoot(ctx, []tree.BlobRef{blobRef}, nil)
	if err != nil {
		q.cleanupPublishingOnFailure(ctx, shard, jobID, appKey, prepToken)
		return "", false, fmt.Errorf("queue: commit payload root: %w", err)
	}

	if e := stopHeartbeat(); e != nil {
		return "", false, fmt.Errorf("queue: publication heartbeat: %w", e)
	}
	if err = q.touchPublishing(ctx, appKey, prepToken); err != nil {
		return "", false, fmt.Errorf("queue: publication ownership lost: %w", err)
	}
	refHeartbeat := q.startPublishingHeartbeat(ctx, appKey, prepToken)
	// Step 4: CreateRef with deterministic name to the root.
	refName := ownerRefName(shard, jobID, prepToken)
	ownerRef, refErr := q.payloads.CreateRef(ctx, refName, nodeID)
	if refErr != nil {
		if errors.Is(refErr, tree.ErrAlreadyExists) {
			// A previous attempt may have created the ref. Get it.
			ownerRef, refErr = q.payloads.GetRef(ctx, refName)
			if refErr != nil {
				q.cleanupPublishingOnFailure(ctx, shard, jobID, appKey, prepToken)
				return "", false, fmt.Errorf("queue: get existing payload ref: %w", refErr)
			}
		} else {
			q.cleanupPublishingOnFailure(ctx, shard, jobID, appKey, prepToken)
			return "", false, fmt.Errorf("queue: create payload ref: %w", refErr)
		}
	}

	if ownerRef.NodeID != nodeID {
		q.cleanupPublishingOnFailure(ctx, shard, jobID, appKey, prepToken)
		return "", false, fmt.Errorf("queue: payload owner ref conflict")
	}

	if e := refHeartbeat(); e != nil {
		return "", false, fmt.Errorf("queue: publication heartbeat: %w", e)
	}
	if e := q.touchPublishing(ctx, appKey, prepToken); e != nil {
		return "", false, fmt.Errorf("queue: publication ownership lost: %w", e)
	}
	// Step 5: CAS publishing -> pending with payload descriptor.
	descriptor := &payloadRefDescriptor{
		BlobRef:          blobRef,
		NodeID:           nodeID,
		OwnerRefName:     refName,
		OwnerRefRevision: ownerRef.Revision,
	}
	payloadInfo := PayloadInfo{ID: expectedID, Size: size}
	pendingEnv := newExternalJobEnvelope(jobID, q.name, shard, descriptor, payloadInfo, publicationCreated, notBefore)
	pendingEnv.PrepToken = prepToken
	pendingBody, err := encodeJob(pendingEnv)
	if err != nil {
		q.cleanupPublishingOnFailure(ctx, shard, jobID, appKey, prepToken)
		return "", false, fmt.Errorf("queue: encode pending job: %w", err)
	}

	current, e := q.store.Get(ctx, appKey)
	if e != nil {
		return "", false, e
	}
	currentEnv, e := decodeJob(current.Value)
	if e != nil || currentEnv.State != statePublishing || currentEnv.PrepToken != prepToken {
		return "", false, fmt.Errorf("queue: publication ownership lost")
	}
	_, err = q.store.CompareAndSwap(ctx, appKey, current.Revision, pendingBody)
	if err != nil {
		if errors.Is(err, cas.ErrConflict) || errors.Is(err, cas.ErrNotFound) {
			// Ambiguous: may have applied. Read back.
			rec2, getErr := q.store.Get(ctx, appKey)
			if getErr == nil {
				existing, decErr := decodeJob(rec2.Value)
				if decErr == nil && existing.State == statePending && existing.PrepToken == prepToken {
					// Our CAS applied despite the error; proceed to step 6.
					goto createReadyMarker
				}
			}
		}
		q.cleanupPublishingOnFailure(ctx, shard, jobID, appKey, prepToken)
		return "", false, fmt.Errorf("queue: transition to pending: %w", err)
	}

createReadyMarker:
	// Step 6: Create ready marker.
	if err := q.createMarker(ctx, q.readyMarkerKey(shard, notBefore, jobID)); err != nil {
		q.opts.Logger.Warn(err, "queue: ready marker create failed", "job", jobID)
	}
	q.observeLatency(ctx, "enqueue", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "enqueue", outcomeSuccess)
	return jobID, false, nil
}

// cleanupPublishingOnFailure best-effort transitions a publishing record
// toward purging so an idempotency key is not wedged.
func (q *Queue) cleanupPublishingOnFailure(ctx context.Context, shard uint16, jobID, appKey, prepToken string) {
	rec, err := q.store.Get(ctx, appKey)
	if err != nil {
		return
	}
	env, err := decodeJob(rec.Value)
	if err != nil || env.PrepToken != prepToken || env.State != statePublishing {
		return
	}
	_, _ = q.store.CompareAndSwap(ctx, appKey, rec.Revision, mustEncodeJob(&jobEnvelope{
		ID:        jobID,
		Queue:     q.name,
		Shard:     shard,
		State:     statePurging,
		Attempts:  0,
		CreatedAt: env.CreatedAt,
		NotBefore: env.NotBefore,
		PrepToken: prepToken,
		PurgedAt:  ptrTime(q.now()),
	}))
}

func mustEncodeJob(env *jobEnvelope) []byte {
	b, _ := encodeJob(env)
	return b
}

func ptrTime(t time.Time) *time.Time { return &t }

// backendSupportsStreaming reports whether the backend can accept
// streaming/multipart PUT for external payloads.
func (q *Queue) backendSupportsStreaming() bool { _, ok := q.be.(s3backend.StreamBackend); return ok }

// newJobID returns a job id appropriate for the queue configuration.
func (q *Queue) newJobID(ctx context.Context, idempotencyKey string) (string, error) {
	if q.opts.SequencerEnabled {
		seq, err := q.nextSequence(ctx)
		if err != nil {
			return "", err
		}
		if idempotencyKey != "" {
			return fmt.Sprintf("%020d-idem-%s", seq, idempotencyKeyHash(q.name, idempotencyKey)), nil
		}
		return fmt.Sprintf("%020d-%s", seq, randomSuffix()), nil
	}
	if idempotencyKey != "" {
		return "idem-" + idempotencyKeyHash(q.name, idempotencyKey), nil
	}
	return fmt.Sprintf("%020d-%s", q.now().UnixMicro(), randomSuffix()), nil
}

// resolveShard returns the shard for a job, honoring an explicit pin.
// When the sequencer is enabled, strict ordering requires a single shard.
func (q *Queue) resolveShard(jobID string, pin *uint16) uint16 {
	if q.opts.SequencerEnabled {
		return 0
	}
	if pin != nil {
		return *pin % q.opts.Shards
	}
	return shardForJob(jobID, q.opts.Shards)
}

// createMarker writes a zero-byte raw marker object with If-None-Match:*,
// retrying transient errors.
func (q *Queue) createMarker(ctx context.Context, key string) error {
	return q.putMarkerWithRetry(ctx, key, &s3backend.Preconditions{IfNoneMatch: true})
}

// deleteMarker best-effort deletes a raw marker, retrying transient errors.
func (q *Queue) deleteMarker(ctx context.Context, key string) {
	if err := q.deleteMarkerWithRetry(ctx, key); err != nil {
		q.opts.Logger.Warn(err, "queue: marker delete failed", "key", key)
	}
}

// listWithRetry calls be.List with the configured retry policy.
func (q *Queue) listWithRetry(ctx context.Context, op, prefixTemplate, prefix string, opts *s3backend.ListOptions) (*s3backend.ListPage, error) {
	policy := q.opts.Retry
	nextDelay := s3collections.BackoffDelays(policy, nil)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		page, err := q.be.List(ctx, prefix, opts)
		if err == nil {
			q.recordListPage(ctx, op, prefixTemplate)
			return page, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !s3backend.IsRetryable(err) {
			return nil, err
		}
		if attempt == policy.MaxAttempts {
			return nil, err
		}
		q.recordRetry(ctx, "list")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(nextDelay()):
		}
	}
	return nil, errors.New("queue: list retries exhausted")
}

// putMarkerWithRetry writes a marker with the configured retry policy.
func (q *Queue) putMarkerWithRetry(ctx context.Context, key string, pre *s3backend.Preconditions) error {
	policy := q.opts.Retry
	nextDelay := s3collections.BackoffDelays(policy, nil)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		_, err := q.be.Put(ctx, key, nil, pre)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s3backend.IsRetryable(err) {
			return err
		}
		if attempt == policy.MaxAttempts {
			return err
		}
		q.recordRetry(ctx, "put_marker")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextDelay()):
		}
	}
	return errors.New("queue: put marker retries exhausted")
}

// deleteMarkerWithRetry removes a marker with the configured retry policy.
func (q *Queue) deleteMarkerWithRetry(ctx context.Context, key string) error {
	policy := q.opts.Retry
	nextDelay := s3collections.BackoffDelays(policy, nil)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		err := q.be.Delete(ctx, key)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s3backend.IsRetryable(err) {
			return err
		}
		if attempt == policy.MaxAttempts {
			return err
		}
		q.recordRetry(ctx, "delete_marker")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextDelay()):
		}
	}
	return errors.New("queue: delete marker retries exhausted")
}

// ClaimOptions configures a single Claim call.
type ClaimOptions struct {
	// VisibilityTimeout overrides DefaultVisibilityTimeout. The minimum is 1s.
	VisibilityTimeout time.Duration

	// RestrictToShards limits the claim scan to the given shards, in order.
	// If empty, Claim probes up to ClaimShardProbe shards selected round-robin.
	RestrictToShards []uint16

	// DeferPayload, when true, does not eagerly load the job payload into
	// Job.Payload. The worker must use OpenPayload to read the payload.
	// External payloads are always deferred to avoid unbounded allocation.
	DeferPayload bool
}

// Claim scans for a pending, visible job, claims it via cas, and returns a
// Job. If no claimable job exists in the selected shards, it returns ErrEmpty.
func (q *Queue) Claim(ctx context.Context, opts ClaimOptions) (*Job, error) {
	start := time.Now()
	vt := opts.VisibilityTimeout
	if vt <= 0 {
		vt = q.opts.DefaultVisibilityTimeout
	}
	if vt < time.Second {
		vt = time.Second
	}
	shards := q.claimShards(opts.RestrictToShards)
	for _, shard := range shards {
		job, err := q.claimShard(ctx, shard, vt, opts.DeferPayload)
		if err == nil {
			q.observeLatency(ctx, "claim", outcomeSuccess, time.Since(start))
			q.recordEvent(ctx, "claim", outcomeSuccess)
			return job, nil
		}
		if !errors.Is(err, ErrEmpty) {
			q.observeLatency(ctx, "claim", outcomeError, time.Since(start))
			q.recordEvent(ctx, "claim", outcomeError)
			return nil, err
		}
	}
	q.observeLatency(ctx, "claim", outcomeEmpty, time.Since(start))
	q.recordEvent(ctx, "claim", outcomeEmpty)
	return nil, ErrEmpty
}

// claimShards selects shards for a Claim call.
func (q *Queue) claimShards(restrict []uint16) []uint16 {
	if len(restrict) > 0 {
		out := make([]uint16, 0, len(restrict))
		for _, s := range restrict {
			out = append(out, s%q.opts.Shards)
		}
		return out
	}
	n := int(q.opts.Shards)
	if n > q.opts.ClaimShardProbe {
		n = q.opts.ClaimShardProbe
	}
	off := int(q.claimOffset.Add(1) % uint64(q.opts.Shards))
	out := make([]uint16, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, uint16((off+i)%int(q.opts.Shards)))
	}
	return out
}

// claimShard attempts to claim a job from a single shard.
func (q *Queue) claimShard(ctx context.Context, shard uint16, vt time.Duration, deferPayload bool) (*Job, error) {
	prefix := q.prefix + "shard/" + shardHex(shard) + "/ready/"
	now := q.now()
	deadline := now.Add(q.opts.ClockSkewTolerance)
	pages := 0
	contToken := ""
	for pages < q.opts.ClaimMaxPages {
		pages++
		opts := &s3backend.ListOptions{MaxKeys: q.opts.ClaimPageSize}
		if contToken != "" {
			opts.ContinuationToken = contToken
		}
		page, err := q.listWithRetry(ctx, "claim", readyPrefixTemplate, prefix, opts)
		if err != nil {
			return nil, err
		}
		for _, info := range page.Objects {
			_, kind, markerTS, jobID, ok := q.parseMarker(info.Key)
			if !ok || kind != "ready" {
				continue
			}
			if markerTS.After(deadline) {
				return nil, ErrEmpty
			}
			job, claimed, err := q.tryClaimJob(ctx, shard, jobID, vt, now, deferPayload)
			if err != nil {
				return nil, err
			}
			if claimed {
				return job, nil
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
	return nil, ErrEmpty
}

// tryClaimJob reads the canonical job and attempts a CAS claim.
func (q *Queue) tryClaimJob(ctx context.Context, shard uint16, jobID string, vt time.Duration, now time.Time, deferPayload bool) (*Job, bool, error) {
	appKey := jobAppKey(shard, jobID)
	rec, err := q.store.Get(ctx, appKey)
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			// Orphan marker; reaper/GC will clean it.
			return nil, false, nil
		}
		return nil, false, err
	}
	env, err := decodeJob(rec.Value)
	if err != nil {
		return nil, false, err
	}
	deadline := now.Add(q.opts.ClockSkewTolerance)
	switch {
	case env.State != statePending:
		// Stale ready marker. Opportunistic reconciliation.
		if env.State == stateClaimed && env.Lease != nil {
			q.ensureLeaseMarker(ctx, shard, jobID, env.Lease.Expiry)
		}
		q.deleteReadyMarker(ctx, shard, env.NotBefore, jobID)
		return nil, false, nil
	case env.NotBefore.After(deadline):
		// Not yet visible; later markers are even later, so stop this shard.
		return nil, false, ErrEmpty
	}

	claimToken := randomToken()
	expiry := now.Add(vt)
	rec2, err := q.store.Update(ctx, appKey, func(ctx context.Context, cur cas.Record) ([]byte, error) {
		curEnv, err := decodeJob(cur.Value)
		if err != nil {
			return nil, err
		}
		if curEnv.State != statePending || curEnv.NotBefore.After(deadline) {
			return nil, cas.ErrConflict
		}
		next := *curEnv
		next.State = stateClaimed
		next.Attempts++
		next.Lease = &leaseEnvelope{Owner: q.opts.WorkerID, Expiry: expiry, Token: claimToken}
		return encodeJob(&next)
	})
	if err != nil {
		if errors.Is(err, cas.ErrConflict) {
			q.recordConflict(ctx, "claim")
			return nil, false, nil
		}
		return nil, false, err
	}

	// Update succeeded. Write lease marker, delete ready marker.
	if err := q.createMarker(ctx, q.leaseMarkerKey(shard, expiry, jobID)); err != nil {
		q.opts.Logger.Warn(err, "queue: lease marker create failed", "job", jobID)
	}
	q.deleteReadyMarker(ctx, shard, env.NotBefore, jobID)

	// Build the Job, optionally populating payload.
	external := isExternalJob(env)
	claimedAttempts := env.Attempts + 1
	var payload, decoded []byte
	var payloadSize int64 = env.PayloadInfo.Size
	if !external {
		decoded, _ = jobInlinePayload(env)
		payloadSize = int64(len(decoded))
		if env.PayloadInfo.ID == "" {
			sum := sha256.Sum256(decoded)
			env.PayloadInfo = PayloadInfo{ID: hex.EncodeToString(sum[:]), Size: payloadSize}
		}
		if !deferPayload {
			payload = decoded
		}
	}
	// External payloads are never eagerly materialized by Claim; use
	// Job.OpenPayload to stream on demand.

	return &Job{
		ID:            jobID,
		Queue:         q.name,
		Shard:         shard,
		Payload:       payload,
		PayloadInfo:   env.PayloadInfo,
		PayloadSize:   payloadSize,
		Attempts:      claimedAttempts,
		Fence:         rec2.Revision,
		Lease:         Lease{Owner: q.opts.WorkerID, Expiry: expiry, Token: claimToken},
		NotBefore:     env.NotBefore,
		CreatedAt:     env.CreatedAt,
		external:      external,
		inlinePayload: decoded,
		q:             q,
		appKey:        appKey,
	}, true, nil
}

// ensureLeaseMarker creates a lease marker if it appears missing.
func (q *Queue) ensureLeaseMarker(ctx context.Context, shard uint16, jobID string, expiry time.Time) {
	if err := q.createMarker(ctx, q.leaseMarkerKey(shard, expiry, jobID)); err != nil {
		q.opts.Logger.Warn(err, "queue: lease marker backfill failed", "job", jobID)
	}
}

// deleteReadyMarker best-effort deletes a ready marker at the given notBefore.
func (q *Queue) deleteReadyMarker(ctx context.Context, shard uint16, notBefore time.Time, jobID string) {
	q.deleteMarker(ctx, q.readyMarkerKey(shard, notBefore, jobID))
}

// renewJob extends the lease of a claimed job. It validates both the owner
// and the claim token, plus the canonical revision matches j.Fence.
func (q *Queue) renewJob(ctx context.Context, j *Job, extendBy time.Duration) error {
	if extendBy <= 0 {
		return errors.New("queue: renew extension must be positive")
	}
	start := time.Now()
	oldExpiry := j.Lease.Expiry
	rec, err := q.updateJob(ctx, j.appKey, j.Fence, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State != stateClaimed || env.Lease == nil || (env.Lease.Owner != j.Lease.Owner || env.Lease.Token != j.Lease.Token) {
			return nil, ErrNotLeased
		}
		next := *env
		newExpiry := maxTime(env.Lease.Expiry, q.now()).Add(extendBy)
		next.Lease = &leaseEnvelope{Owner: j.Lease.Owner, Expiry: newExpiry, Token: j.Lease.Token}
		return &next, nil
	})
	if err != nil {
		outcome := outcomeError
		if errors.Is(err, ErrStaleLease) {
			q.recordConflict(ctx, "renew")
			outcome = outcomeConflict
		}
		q.observeLatency(ctx, "renew", outcome, time.Since(start))
		q.recordEvent(ctx, "renew", outcome)
		return err
	}
	j.Fence = rec.Revision
	if env, _ := decodeJob(rec.Value); env != nil && env.Lease != nil {
		j.Lease.Expiry = env.Lease.Expiry
	}
	if err := q.createMarker(ctx, q.leaseMarkerKey(j.Shard, j.Lease.Expiry, j.ID)); err != nil {
		q.opts.Logger.Warn(err, "queue: renewed lease marker create failed", "job", j.ID)
	}
	q.deleteMarker(ctx, q.leaseMarkerKey(j.Shard, oldExpiry, j.ID))
	q.observeLatency(ctx, "renew", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "renew", outcomeSuccess)
	return nil
}

// completeJob transitions a claimed job to completed.
func (q *Queue) completeJob(ctx context.Context, j *Job) error {
	start := time.Now()
	rec, err := q.updateJob(ctx, j.appKey, j.Fence, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State == stateCompleted || env.State == stateDead {
			return env, nil // idempotent no-op
		}
		if env.State != stateClaimed || env.Lease == nil || (env.Lease.Owner != j.Lease.Owner || env.Lease.Token != j.Lease.Token) {
			return nil, ErrNotLeased
		}
		next := *env
		next.State = stateCompleted
		next.Lease = nil
		now := q.now()
		next.CompletedAt = &now
		return &next, nil
	})
	if err != nil {
		if errors.Is(err, ErrStaleLease) {
			if cur, e := q.store.Get(ctx, j.appKey); e == nil {
				if ce, de := decodeJob(cur.Value); de == nil && (ce.State == stateCompleted || ce.State == stateDead) {
					return nil
				}
			}
		}
		outcome := outcomeError
		if errors.Is(err, ErrStaleLease) {
			q.recordConflict(ctx, "complete")
			outcome = outcomeConflict
		}
		q.observeLatency(ctx, "complete", outcome, time.Since(start))
		q.recordEvent(ctx, "complete", outcome)
		return err
	}
	j.Fence = rec.Revision
	q.deleteMarker(ctx, q.leaseMarkerKey(j.Shard, j.Lease.Expiry, j.ID))
	q.deleteReadyMarker(ctx, j.Shard, j.NotBefore, j.ID)
	j.Lease = Lease{}
	q.observeLatency(ctx, "complete", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "complete", outcomeSuccess)
	return nil
}

// retryJob transitions a claimed job back to pending, or to dead if attempts
// would exceed MaxAttempts.
func (q *Queue) retryJob(ctx context.Context, j *Job, opts RetryOptions) error {
	start := time.Now()
	now := q.now()
	rec, err := q.updateJob(ctx, j.appKey, j.Fence, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State != stateClaimed || env.Lease == nil || (env.Lease.Owner != j.Lease.Owner || env.Lease.Token != j.Lease.Token) {
			return nil, ErrNotLeased
		}
		next := *env
		reason := reasonEnvelope{At: now, Reason: opts.Reason}
		next.Reasons = append(next.Reasons, reason)
		if len(next.Reasons) > q.opts.ReasonHistory {
			next.Reasons = next.Reasons[len(next.Reasons)-q.opts.ReasonHistory:]
		}

		attemptsAfterNextClaim := next.Attempts + 1
		if q.opts.MaxAttempts > 0 && attemptsAfterNextClaim >= q.opts.MaxAttempts {
			next.State = stateDead
			next.Lease = nil
			next.Dead = &deadEnvelope{Reason: reason.Reason, At: now}
			return &next, nil
		}

		next.State = statePending
		next.Lease = nil
		next.NotBefore = now.Add(opts.Backoff)
		return &next, nil
	})
	if err != nil {
		outcome := outcomeError
		if errors.Is(err, ErrStaleLease) {
			q.recordConflict(ctx, "retry")
			outcome = outcomeConflict
		}
		q.observeLatency(ctx, "retry", outcome, time.Since(start))
		q.recordEvent(ctx, "retry", outcome)
		return err
	}

	// Decode resulting state to create the right marker.
	env, _ := decodeJob(rec.Value)
	if env != nil && env.State == stateDead {
		if err := q.createMarker(ctx, q.deadMarkerKey(j.Shard, now, j.ID)); err != nil {
			q.opts.Logger.Warn(err, "queue: dead marker create failed", "job", j.ID)
		}
		q.deleteMarker(ctx, q.leaseMarkerKey(j.Shard, j.Lease.Expiry, j.ID))
		j.Fence = rec.Revision
		j.Lease = Lease{}
		q.observeLatency(ctx, "retry", outcomeDead, time.Since(start))
		q.recordEvent(ctx, "retry", outcomeDead)
		return nil
	}

	notBefore := now.Add(opts.Backoff)
	if env != nil {
		notBefore = env.NotBefore
	}
	if err := q.createMarker(ctx, q.readyMarkerKey(j.Shard, notBefore, j.ID)); err != nil {
		q.opts.Logger.Warn(err, "queue: ready marker recreate failed", "job", j.ID)
	}
	q.deleteMarker(ctx, q.leaseMarkerKey(j.Shard, j.Lease.Expiry, j.ID))
	j.Fence = rec.Revision
	j.Lease = Lease{}
	j.NotBefore = notBefore
	q.observeLatency(ctx, "retry", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "retry", outcomeSuccess)
	return nil
}

// deadJob transitions a claimed job to dead-lettered.
func (q *Queue) deadJob(ctx context.Context, j *Job, reason string) error {
	start := time.Now()
	now := q.now()
	var alreadyDead bool
	rec, err := q.updateJob(ctx, j.appKey, j.Fence, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State == stateDead {
			alreadyDead = true
			return env, nil // idempotent no-op
		}
		if env.State != stateClaimed || env.Lease == nil || (env.Lease.Owner != j.Lease.Owner || env.Lease.Token != j.Lease.Token) {
			return nil, ErrNotLeased
		}
		next := *env
		reason := reasonEnvelope{At: now, Reason: reason}
		next.Reasons = append(next.Reasons, reason)
		if len(next.Reasons) > q.opts.ReasonHistory {
			next.Reasons = next.Reasons[len(next.Reasons)-q.opts.ReasonHistory:]
		}
		next.State = stateDead
		next.Lease = nil
		next.Dead = &deadEnvelope{Reason: reason.Reason, At: now}
		return &next, nil
	})
	if err != nil {
		if errors.Is(err, ErrStaleLease) {
			if cur, e := q.store.Get(ctx, j.appKey); e == nil {
				if ce, de := decodeJob(cur.Value); de == nil && ce.State == stateDead {
					return nil
				}
			}
		}
		outcome := outcomeError
		if errors.Is(err, ErrStaleLease) {
			q.recordConflict(ctx, "dead")
			outcome = outcomeConflict
		}
		q.observeLatency(ctx, "dead", outcome, time.Since(start))
		q.recordEvent(ctx, "dead", outcome)
		return err
	}
	if !alreadyDead {
		if err := q.createMarker(ctx, q.deadMarkerKey(j.Shard, now, j.ID)); err != nil {
			q.opts.Logger.Warn(err, "queue: dead marker create failed", "job", j.ID)
		}
		q.deleteMarker(ctx, q.leaseMarkerKey(j.Shard, j.Lease.Expiry, j.ID))
		j.Fence = rec.Revision
		j.Lease = Lease{}
	}
	q.observeLatency(ctx, "dead", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "dead", outcomeSuccess)
	return nil
}

// updateJob is a helper that decodes the current value, calls fn, and writes
// the new envelope via cas.CompareAndSwap with the expected revision (fence).
// Validation errors from fn are returned unchanged; CAS conflicts are wrapped
// as ErrStaleLease. The expected revision (fence) prevents a stale Job handle
// from operating on a later re-claim of the same job.
func (q *Queue) updateJob(ctx context.Context, appKey string, fence uint64, fn func(*jobEnvelope) (*jobEnvelope, error)) (cas.Record, error) {
	// Read current state and verify the fence matches.
	rec, err := q.store.Get(ctx, appKey)
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) || errors.Is(err, cas.ErrDeleted) {
			return cas.Record{}, ErrStaleLease
		}
		return cas.Record{}, err
	}
	if rec.Revision != fence {
		return cas.Record{}, ErrStaleLease
	}
	env, err := decodeJob(rec.Value)
	if err != nil {
		return cas.Record{}, err
	}
	next, err := fn(env)
	if err != nil {
		return cas.Record{}, err
	}
	if next == nil {
		return cas.Record{}, cas.ErrConflict
	}
	if sameJobEnvelope(next, env) {
		return rec, nil
	}
	body, err := encodeJob(next)
	if err != nil {
		return cas.Record{}, err
	}
	rec2, err := q.store.CompareAndSwap(ctx, appKey, fence, body)
	if err == nil {
		return rec2, nil
	}
	// Ambiguous write: read back and check if it matches our intended value.
	if errors.Is(err, cas.ErrConflict) || errors.Is(err, cas.ErrNotFound) || s3backend.IsRetryable(err) {
		rec3, getErr := q.store.Get(ctx, appKey)
		if getErr == nil {
			existing, decErr := decodeJob(rec3.Value)
			if decErr == nil && sameJobEnvelope(existing, next) {
				return rec3, nil
			}
		}
		return cas.Record{}, ErrStaleLease
	}
	return cas.Record{}, err
}

// sameJobEnvelope reports whether two envelopes are semantically equal.
func sameJobEnvelope(a, b *jobEnvelope) bool {
	aJSON, _ := encodeJob(a)
	bJSON, _ := encodeJob(b)
	return string(aJSON) == string(bJSON)
}

// maxTime returns the later of a and b.
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
