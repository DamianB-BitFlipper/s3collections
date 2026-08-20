package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/tree"
)

// spoolAndHash copies r to a temporary file while computing its SHA-256
// hash, enforcing a maximum size. The caller must call the returned cleanup
// function to remove the temp file.
func spoolAndHash(ctx context.Context, r io.Reader, maxBytes int64) (digest string, size int64, f *os.File, cleanup func(), err error) {
	tmp, err := os.CreateTemp("", "s3collections-queue-payload-*")
	if err != nil {
		return "", 0, nil, nil, err
	}
	cleanup = func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(&ctxReader{ctx: ctx, r: r}, maxBytes+1))
	if err != nil {
		cleanup()
		return "", 0, nil, nil, err
	}
	if n > maxBytes {
		cleanup()
		return "", 0, nil, nil, fmt.Errorf("queue: payload exceeds max size %d", maxBytes)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return "", 0, nil, nil, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, tmp, cleanup, nil
}

// ctxReader wraps a reader with context cancellation.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr *ctxReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	return cr.r.Read(p)
}

func (q *Queue) touchPublishing(ctx context.Context, appKey, token string) error {
	_, err := q.store.Update(ctx, appKey, func(ctx context.Context, cur cas.Record) ([]byte, error) {
		env, e := decodeJob(cur.Value)
		if e != nil {
			return nil, e
		}
		if env.State != statePublishing || env.PrepToken != token {
			return nil, cas.ErrConflict
		}
		next := *env
		now := q.now()
		next.PublishingAt = &now
		return encodeJob(&next)
	})
	return err
}
func (q *Queue) startPublishingHeartbeat(ctx context.Context, appKey, token string) func() error {
	interval := q.opts.PreparationTimeout / 3
	if interval < time.Second {
		interval = time.Second
	}
	hctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hctx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := q.touchPublishing(hctx, appKey, token); err != nil {
					done <- err
					return
				}
			}
		}
	}()
	var once sync.Once
	var result error
	return func() error { once.Do(func() { cancel(); result = <-done }); return result }
}

// openExternalPayload opens a streaming reader for an external payload
// identified by its blob ID and expected size.
func (q *Queue) openExternalPayload(ctx context.Context, blobID PayloadID, size int64) (io.ReadCloser, error) {
	ref := tree.BlobRef{Hash: tree.BlobID(blobID), Size: size}
	reader, err := q.payloads.GetBlob(ctx, ref)
	if err != nil {
		if errors.Is(err, tree.ErrNotFound) {
			return nil, fmt.Errorf("%w: %v", ErrPayloadNotFound, err)
		}
		return nil, fmt.Errorf("queue: open external payload: %w", err)
	}
	return reader, nil
}

func (q *Queue) payloadSentinel(ctx context.Context) (tree.NodeID, error) {
	return q.payloads.CommitRoot(ctx, nil, []byte("queue-payload-ref-sentinel-v1"))
}

func (q *Queue) releaseOwnerRef(ctx context.Context, name string, revision uint64) error {
	if revision > 0 {
		err := q.payloads.DeleteRef(ctx, name, revision)
		if err == nil || errors.Is(err, tree.ErrNotFound) {
			return nil
		}
		if !errors.Is(err, tree.ErrConflict) {
			return err
		}
	}
	current, err := q.payloads.GetRef(ctx, name)
	if errors.Is(err, tree.ErrNotFound) {
		sentinel, se := q.payloadSentinel(ctx)
		if se != nil {
			return se
		}
		created, ce := q.payloads.CreateRef(ctx, name, sentinel)
		if errors.Is(ce, tree.ErrAlreadyExists) {
			current, err = q.payloads.GetRef(ctx, name)
			if errors.Is(err, tree.ErrNotFound) {
				return nil
			}
		} else if ce != nil {
			return ce
		} else {
			current = created
			err = nil
		}
	}
	if err != nil {
		return err
	}
	return q.payloads.DeleteRef(ctx, name, current.Revision)
}

// purgeExternalJob transitions a terminal (or stale publishing) job to
// statePurging, removes the owner ref, and tombstones the canonical job.
// It is safe to call from GC: a crash between ref removal and tombstone
// leaves the job in statePurging, which a future GC pass completes via
// resumePurging.
func (q *Queue) purgeExternalJob(ctx context.Context, shard uint16, jobID string, rec cas.Record, env *jobEnvelope) error {
	isExternal := env.PayloadRef != nil || (env.State == statePublishing && env.PayloadInfo.ID != "")
	if !isExternal {
		_, err := q.store.Delete(ctx, rec.Key, rec.Revision)
		return err
	}
	if env.State == statePurging {
		return q.resumePurging(ctx, shard, jobID, rec, env)
	}
	if env.State != stateCompleted && env.State != stateDead && env.State != statePublishing {
		return cas.ErrConflict
	}
	next := *env
	next.State = statePurging
	now := q.now()
	next.PurgedAt = &now
	body, err := encodeJob(&next)
	if err != nil {
		return err
	}
	purgingRec, err := q.store.CompareAndSwap(ctx, rec.Key, rec.Revision, body)
	if err != nil {
		return fmt.Errorf("queue: transition to purging: %w", err)
	}
	return q.resumePurging(ctx, shard, jobID, purgingRec, &next)
}

// resumePurging completes the purge of a job left in statePurging by a
// crashed prior GC pass. It attempts ref deletion (idempotent) and then
// tombstones the canonical job.
func (q *Queue) resumePurging(ctx context.Context, shard uint16, jobID string, rec cas.Record, env *jobEnvelope) error {
	// Try ref deletion using PayloadRef if available, or deterministic name.
	refName := ownerRefName(shard, jobID, env.PrepToken)
	refRev := uint64(0)
	if env.PayloadRef != nil {
		refName = env.PayloadRef.OwnerRefName
		refRev = env.PayloadRef.OwnerRefRevision
	}
	if err := q.releaseOwnerRef(ctx, refName, refRev); err != nil {
		return err
	}
	_, err := q.store.Delete(ctx, rec.Key, rec.Revision)
	if errors.Is(err, cas.ErrNotFound) {
		return nil
	}
	return err
}

// resumePublishing completes publication of a job left in statePublishing by
// a crashed prior attempt. The publishing envelope carries PayloadInfo (blob
// ID and size) so maintenance can verify the blob was uploaded, commit a root,
// publish the owner ref, and CAS the job to pending. If the blob has not yet
// been uploaded (crash before PutBlob completed), the job is left in
// publishing until PreparationTimeout, after which it is purged.
func (q *Queue) resumePublishing(ctx context.Context, shard uint16, jobID string, env *jobEnvelope) {
	if env.PayloadInfo.ID == "" {
		return
	}
	blobID := tree.BlobID(env.PayloadInfo.ID)

	// Verify blob was uploaded.
	stat, err := q.payloads.StatBlobID(ctx, blobID)
	if err != nil {
		return // blob not uploaded yet; leave for timeout purge
	}

	// Commit root (idempotent — content-addressed).
	rootID, err := q.payloads.CommitRoot(ctx, []tree.BlobRef{stat}, nil)
	if err != nil {
		return
	}

	if err = q.touchPublishing(ctx, jobAppKey(shard, jobID), env.PrepToken); err != nil {
		return
	}
	// Ensure owner ref points at root (idempotent).
	refName := ownerRefName(shard, jobID, env.PrepToken)
	refRev, err := q.ensureOwnerRef(ctx, refName, rootID)
	if err != nil {
		return
	}

	appKey := jobAppKey(shard, jobID)
	descriptor := &payloadRefDescriptor{
		BlobRef:          stat,
		NodeID:           rootID,
		OwnerRefName:     refName,
		OwnerRefRevision: refRev,
	}
	pendingEnv := newExternalJobEnvelope(jobID, q.name, shard, descriptor, env.PayloadInfo, env.CreatedAt, env.NotBefore)
	pendingEnv.PrepToken = env.PrepToken
	pendingBody, _ := encodeJob(pendingEnv)

	cur, e := q.store.Get(ctx, appKey)
	if e != nil {
		return
	}
	curEnv, e := decodeJob(cur.Value)
	if e != nil || curEnv.State != statePublishing || curEnv.PrepToken != env.PrepToken {
		return
	}
	_, err = q.store.CompareAndSwap(ctx, appKey, cur.Revision, pendingBody)
	if err != nil {
		q.opts.Logger.Warn(err, "queue: resume publishing CAS failed", "job", jobID)
		return
	}

	if err := q.createMarker(ctx, q.readyMarkerKey(shard, env.NotBefore, jobID)); err != nil {
		q.opts.Logger.Warn(err, "queue: resume ready marker failed", "job", jobID)
	}
}

// ensureOwnerRef creates or updates the deterministic owner ref to point at
// rootID. It is idempotent: if the ref already targets rootID, the existing
// revision is returned.
func (q *Queue) ensureOwnerRef(ctx context.Context, name string, rootID tree.NodeID) (uint64, error) {
	ref, err := q.payloads.CreateRef(ctx, name, rootID)
	if err == nil {
		return ref.Revision, nil
	}
	if !errors.Is(err, tree.ErrAlreadyExists) {
		return 0, err
	}
	existing, err := q.payloads.GetRef(ctx, name)
	if err != nil {
		return 0, err
	}
	if existing.NodeID == rootID {
		return existing.Revision, nil
	}
	ref, err = q.payloads.CompareAndSwapRef(ctx, name, existing.Revision, rootID)
	if err != nil {
		return 0, err
	}
	return ref.Revision, nil
}
