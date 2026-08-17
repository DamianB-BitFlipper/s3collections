package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/s3backend"
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
func (q *Queue) Enqueue(ctx context.Context, payload []byte, opts EnqueueOptions) (jobID string, existed bool, err error) {
	start := time.Now()
	if len(payload) > q.opts.MaxPayloadBytes {
		q.observeLatency(ctx, "enqueue", outcomeError, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeError)
		return "", false, cas.ErrTooLarge
	}
	jobID = generateJobID(q.name, opts.IdempotencyKey, nil)
	if q.opts.SequencerEnabled && opts.IdempotencyKey == "" {
		seq, err := q.nextSequence(ctx)
		if err != nil {
			q.observeLatency(ctx, "enqueue", outcomeError, time.Since(start))
			q.recordEvent(ctx, "enqueue", outcomeError)
			return "", false, fmt.Errorf("queue: sequencer failed: %w", err)
		}
		jobID = fmt.Sprintf("%020d-%s", seq, jobID)
	}
	shard := q.resolveShard(jobID, opts.Shard)
	appKey := jobAppKey(shard, jobID)
	notBefore := q.now().Add(opts.Delay)
	env := newJobEnvelope(jobID, q.name, shard, payload, q.now(), notBefore)
	body, err := encodeJob(env)
	if err != nil {
		q.observeLatency(ctx, "enqueue", outcomeError, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeError)
		return "", false, fmt.Errorf("queue: encode job: %w", err)
	}

	_, err = q.store.Create(ctx, appKey, body)
	if err == nil {
		if err := q.putMarker(ctx, q.readyMarkerKey(shard, notBefore, jobID), nil); err != nil {
			q.opts.Logger.Warn(err, "queue: ready marker create failed", "job", jobID)
		}
		q.observeLatency(ctx, "enqueue", outcomeSuccess, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeSuccess)
		return jobID, false, nil
	}
	if errors.Is(err, cas.ErrAlreadyExists) {
		// Dedup-through-retention: the canonical object (live or tombstone)
		// already exists.
		q.observeLatency(ctx, "enqueue", outcomeSuccess, time.Since(start))
		q.recordEvent(ctx, "enqueue", outcomeSuccess)
		return jobID, true, nil
	}
	q.observeLatency(ctx, "enqueue", outcomeError, time.Since(start))
	q.recordEvent(ctx, "enqueue", outcomeError)
	return "", false, err
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

// putMarker writes a zero-byte raw marker object, retrying transient errors.
func (q *Queue) putMarker(ctx context.Context, key string, pre *s3backend.Preconditions) error {
	return q.putMarkerWithRetry(ctx, key, pre)
}

// deleteMarker best-effort deletes a raw marker, retrying transient errors.
func (q *Queue) deleteMarker(ctx context.Context, key string) {
	if err := q.deleteMarkerWithRetry(ctx, key); err != nil {
		q.opts.Logger.Warn(err, "queue: marker delete failed", "key", key)
	}
}

// listWithRetry calls be.List with the configured retry policy.
func (q *Queue) listWithRetry(ctx context.Context, prefix string, opts *s3backend.ListOptions) (*s3backend.ListPage, error) {
	policy := q.opts.Retry
	nextDelay := s3collections.BackoffDelays(policy, nil)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		page, err := q.be.List(ctx, prefix, opts)
		if err == nil {
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
		job, err := q.claimShard(ctx, shard, vt)
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
func (q *Queue) claimShard(ctx context.Context, shard uint16, vt time.Duration) (*Job, error) {
	prefix := q.prefix + "shard/" + shardHex(shard) + "/ready/"
	startAfter := ""
	now := q.now()
	deadline := now.Add(q.opts.ClockSkewTolerance)
	pages := 0
	for pages < q.opts.ClaimMaxPages {
		pages++
		page, err := q.listWithRetry(ctx, prefix, &s3backend.ListOptions{
			StartAfter: startAfter,
			MaxKeys:    q.opts.ClaimPageSize,
		})
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
			job, claimed, err := q.tryClaimJob(ctx, shard, jobID, vt, now)
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
		startAfter = page.NextContinuationToken
		if startAfter == "" {
			break
		}
	}
	return nil, ErrEmpty
}

// tryClaimJob reads the canonical job and attempts a CAS claim.
func (q *Queue) tryClaimJob(ctx context.Context, shard uint16, jobID string, vt time.Duration, now time.Time) (*Job, bool, error) {
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
		// Stale ready marker. Opportunistically reconcile lease marker if
		// claimed, then delete the ready marker.
		if env.State == stateClaimed && env.Lease != nil {
			q.ensureLeaseMarker(ctx, shard, jobID, env.Lease.Expiry)
		}
		q.deleteReadyMarker(ctx, shard, env.NotBefore, jobID)
		return nil, false, nil
	case env.NotBefore.After(deadline):
		// Not yet visible; later markers are even later, so stop this shard.
		return nil, false, ErrEmpty
	}

	expiry := now.Add(vt)
	newEnv := *env
	newEnv.State = stateClaimed
	newEnv.Attempts++
	newEnv.Lease = &leaseEnvelope{Owner: q.opts.WorkerID, Expiry: expiry}

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
		next.Lease = &leaseEnvelope{Owner: q.opts.WorkerID, Expiry: now.Add(vt)}
		return encodeJob(&next)
	})
	if err != nil {
		if errors.Is(err, cas.ErrConflict) {
			return nil, false, nil
		}
		return nil, false, err
	}

	// Update succeeded. Write lease marker, delete ready marker.
	if err := q.putMarker(ctx, q.leaseMarkerKey(shard, expiry, jobID), nil); err != nil {
		q.opts.Logger.Warn(err, "queue: lease marker create failed", "job", jobID)
	}
	q.deleteReadyMarker(ctx, shard, env.NotBefore, jobID)

	payload, _ := jobPayload(&newEnv)
	return &Job{
		ID:        jobID,
		Queue:     q.name,
		Shard:     shard,
		Payload:   payload,
		Attempts:  newEnv.Attempts,
		Fence:     rec2.Revision,
		Lease:     Lease{Owner: q.opts.WorkerID, Expiry: expiry},
		NotBefore: newEnv.NotBefore,
		CreatedAt: newEnv.CreatedAt,
		q:         q,
		appKey:    appKey,
	}, true, nil
}

// ensureLeaseMarker creates a lease marker if it appears missing.
func (q *Queue) ensureLeaseMarker(ctx context.Context, shard uint16, jobID string, expiry time.Time) {
	if err := q.putMarker(ctx, q.leaseMarkerKey(shard, expiry, jobID), nil); err != nil {
		q.opts.Logger.Warn(err, "queue: lease marker backfill failed", "job", jobID)
	}
}

// deleteReadyMarker best-effort deletes a ready marker at the given notBefore.
func (q *Queue) deleteReadyMarker(ctx context.Context, shard uint16, notBefore time.Time, jobID string) {
	q.deleteMarker(ctx, q.readyMarkerKey(shard, notBefore, jobID))
}

// renewJob extends the lease of a claimed job.
func (q *Queue) renewJob(ctx context.Context, j *Job, extendBy time.Duration) error {
	start := time.Now()
	_, err := q.updateJob(ctx, j.appKey, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State != stateClaimed || env.Lease == nil || env.Lease.Owner != j.Lease.Owner {
			return nil, ErrNotLeased
		}
		next := *env
		newExpiry := maxTime(env.Lease.Expiry, q.now()).Add(extendBy)
		next.Lease = &leaseEnvelope{Owner: j.Lease.Owner, Expiry: newExpiry}
		j.Lease.Expiry = newExpiry
		return &next, nil
	})
	if err != nil {
		outcome := outcomeError
		if errors.Is(err, ErrStaleLease) {
			outcome = outcomeConflict
		}
		q.observeLatency(ctx, "renew", outcome, time.Since(start))
		q.recordEvent(ctx, "renew", outcome)
		return err
	}
	q.observeLatency(ctx, "renew", outcomeSuccess, time.Since(start))
	q.recordEvent(ctx, "renew", outcomeSuccess)
	return nil
}

// completeJob transitions a claimed job to completed.
func (q *Queue) completeJob(ctx context.Context, j *Job) error {
	start := time.Now()
	rec, err := q.updateJob(ctx, j.appKey, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State == stateCompleted || env.State == stateDead {
			return env, nil // idempotent no-op
		}
		if env.State != stateClaimed || env.Lease == nil || env.Lease.Owner != j.Lease.Owner {
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
		outcome := outcomeError
		if errors.Is(err, ErrStaleLease) {
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
	rec, err := q.updateJob(ctx, j.appKey, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State != stateClaimed || env.Lease == nil || env.Lease.Owner != j.Lease.Owner {
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
			outcome = outcomeConflict
		}
		q.observeLatency(ctx, "retry", outcome, time.Since(start))
		q.recordEvent(ctx, "retry", outcome)
		return err
	}

	// Decode resulting state to create the right marker.
	env, _ := decodeJob(rec.Value)
	if env != nil && env.State == stateDead {
		if err := q.putMarker(ctx, q.deadMarkerKey(j.Shard, now, j.ID), nil); err != nil {
			q.opts.Logger.Warn(err, "queue: dead marker create failed", "job", j.ID)
		}
		q.deleteMarker(ctx, q.leaseMarkerKey(j.Shard, j.Lease.Expiry, j.ID))
		q.observeLatency(ctx, "retry", outcomeDead, time.Since(start))
		q.recordEvent(ctx, "retry", outcomeDead)
		return nil
	}

	notBefore := now.Add(opts.Backoff)
	if env != nil {
		notBefore = env.NotBefore
	}
	if err := q.putMarker(ctx, q.readyMarkerKey(j.Shard, notBefore, j.ID), nil); err != nil {
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
	rec, err := q.updateJob(ctx, j.appKey, func(env *jobEnvelope) (*jobEnvelope, error) {
		if env.State == stateDead {
			alreadyDead = true
			return env, nil // idempotent no-op
		}
		if env.State != stateClaimed || env.Lease == nil || env.Lease.Owner != j.Lease.Owner {
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
		outcome := outcomeError
		if errors.Is(err, ErrStaleLease) {
			outcome = outcomeConflict
		}
		q.observeLatency(ctx, "dead", outcome, time.Since(start))
		q.recordEvent(ctx, "dead", outcome)
		return err
	}
	if !alreadyDead {
		if err := q.putMarker(ctx, q.deadMarkerKey(j.Shard, now, j.ID), nil); err != nil {
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
// the new envelope via cas.Update. Validation errors from fn are returned
// unchanged; CAS conflicts are wrapped as ErrStaleLease.
func (q *Queue) updateJob(ctx context.Context, appKey string, fn func(*jobEnvelope) (*jobEnvelope, error)) (cas.Record, error) {
	rec, err := q.store.Update(ctx, appKey, func(ctx context.Context, cur cas.Record) ([]byte, error) {
		env, err := decodeJob(cur.Value)
		if err != nil {
			return nil, err
		}
		next, err := fn(env)
		if err != nil {
			return nil, err
		}
		if next == nil {
			return nil, cas.ErrConflict
		}
		if bytesSameJob(next, env) {
			return cur.Value, nil
		}
		return encodeJob(next)
	})
	if err == nil {
		return rec, nil
	}
	if errors.Is(err, cas.ErrConflict) || errors.Is(err, cas.ErrDeleted) || errors.Is(err, cas.ErrNotFound) {
		return rec, ErrStaleLease
	}
	return rec, err
}

// bytesSameJob reports whether two envelopes are semantically equal.
func bytesSameJob(a, b *jobEnvelope) bool {
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
