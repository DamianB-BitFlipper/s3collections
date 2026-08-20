package queue

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/damianb/s3collections/tree"
)

// PayloadID is the content hash of a payload, identical to tree.BlobID.
type PayloadID = tree.BlobID

// PayloadInfo describes a job payload.
type PayloadInfo struct {
	// ID is the content hash (PayloadID) of the payload. For inline payloads
	// it is the SHA-256 of the decoded payload bytes. For external payloads
	// it is the tree.BlobID of the stored blob.
	ID PayloadID `json:"id"`
	// Size is the payload size in bytes.
	Size int64 `json:"size"`
}

// Lease is a snapshot of a job lease.
type Lease struct {
	// Owner is the WorkerID that holds the lease.
	Owner string
	// Expiry is the lease expiration time.
	Expiry time.Time
	// Token is a random per-claim token that prevents a stale Job handle
	// (from the same WorkerID) from operating on a later re-claim.
	Token string
}

// Job represents a claimed job returned by Claim.
type Job struct {
	// ID is the unique job identifier.
	ID string
	// Queue is the queue name.
	Queue string
	// Shard is the shard index containing the job.
	Shard uint16
	// Payload is the user-supplied payload bytes. It is non-nil for inline
	// jobs when DeferPayload is false. It is nil for external jobs or when
	// DeferPayload is true; use OpenPayload to stream the payload.
	Payload []byte
	// PayloadInfo describes the payload (ID and size). Always populated.
	PayloadInfo PayloadInfo
	// PayloadSize is the payload size in bytes. Always populated.
	PayloadSize int64
	// Attempts is the number of successful claims so far, including this one.
	Attempts int
	// Fence is the cas revision of the canonical job at the moment of claim
	// or the most recent renew. Consumers can pass it to Guard for
	// exactly-once downstream effects.
	Fence uint64
	// Lease is the current lease snapshot.
	Lease Lease
	// NotBefore is the earliest time the job may be claimed again.
	NotBefore time.Time
	// CreatedAt is the time the job was first enqueued.
	CreatedAt time.Time

	// external is true when the payload is stored in the payload tree.
	external      bool
	inlinePayload []byte
	q             *Queue
	appKey        string
}

// OpenPayload returns a reader for the job payload. For inline jobs it
// returns a bytes reader. For external jobs it streams and verifies the
// payload from the payload tree. The caller must close the reader.
func (j *Job) OpenPayload(ctx context.Context) (io.ReadCloser, error) {
	if !j.external {
		return io.NopCloser(bytes.NewReader(j.inlinePayload)), nil
	}
	return j.q.openExternalPayload(ctx, j.PayloadInfo.ID, j.PayloadInfo.Size)
}

// Renew extends the job's lease. extendBy must be positive.
func (j *Job) Renew(ctx context.Context, extendBy time.Duration) error {
	return j.q.renewJob(ctx, j, extendBy)
}

// Complete transitions a claimed job to completed. It is idempotent: calling
// it on an already completed or dead job returns nil.
func (j *Job) Complete(ctx context.Context) error {
	return j.q.completeJob(ctx, j)
}

// RetryOptions configures a Retry call.
type RetryOptions struct {
	// Backoff delays the next claim until now+Backoff. Zero allows immediate
	// re-claiming.
	Backoff time.Duration
	// Reason describes why the job is being retried; it is appended to the
	// envelope reason history.
	Reason string
}

// Retry transitions a claimed job back to pending with the given backoff. If
// Options.MaxAttempts is non-zero and Attempts+1 would reach it, the job is
// moved to dead instead.
func (j *Job) Retry(ctx context.Context, opts RetryOptions) error {
	return j.q.retryJob(ctx, j, opts)
}

// Dead transitions a claimed job to dead-lettered.
func (j *Job) Dead(ctx context.Context, reason string) error {
	return j.q.deadJob(ctx, j, reason)
}

// Guard returns ErrFenceStale if provided is older than latest. It is a
// helper for downstream consumers that store the highest seen fence alongside
// durable effects.
func Guard(latest, provided uint64) error {
	if provided < latest {
		return ErrFenceStale
	}
	return nil
}

// jobState enumerates the states stored in the canonical envelope.
type jobState string

const (
	statePending    jobState = "pending"
	stateClaimed    jobState = "claimed"
	stateCompleted  jobState = "completed"
	stateDead       jobState = "dead"
	statePublishing jobState = "publishing"
	statePurging    jobState = "purging"
)

// leaseEnvelope is the JSON form of a lease.
type leaseEnvelope struct {
	Owner  string    `json:"owner"`
	Expiry time.Time `json:"expiry"`
	Token  string    `json:"token"`
}

// reasonEnvelope records a retry/dead reason.
type reasonEnvelope struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
}

// deadEnvelope records dead-letter metadata.
type deadEnvelope struct {
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// payloadRefDescriptor is the external payload reference stored in the job
// envelope. It is mutually exclusive with the legacy inline payload field.
type payloadRefDescriptor struct {
	// BlobRef is the tree blob reference for the payload.
	BlobRef tree.BlobRef `json:"blobRef"`
	// NodeID is the tree root node containing the payload blob.
	NodeID tree.NodeID `json:"nodeId"`
	// OwnerRefName is the deterministic tree ref name pointing at NodeID.
	OwnerRefName string `json:"ownerRefName"`
	// OwnerRefRevision is the revision of the tree ref at the time of
	// publication.
	OwnerRefRevision uint64 `json:"ownerRefRevision"`
}

// jobEnvelope is the canonical JSON representation of a job stored by cas.
// It is metadata-only: payload bytes are either inline (base64 in LegacyPayload,
// capped at InlinePayloadBytes) or external (in PayloadRef).
type jobEnvelope struct {
	ID    string `json:"id"`
	Queue string `json:"queue"`
	// Shard is the queue shard index. It is stored in JSON as a numeric
	// value; S3 key layouts format the same value as four lower-case hex
	// digits (for example, "00af").
	Shard       uint16           `json:"shard"`
	State       jobState         `json:"state"`
	Attempts    int              `json:"attempts"`
	CreatedAt   time.Time        `json:"createdAt"`
	NotBefore   time.Time        `json:"notBefore"`
	Lease       *leaseEnvelope   `json:"lease,omitempty"`
	Reasons     []reasonEnvelope `json:"reasons,omitempty"`
	PayloadInfo PayloadInfo      `json:"payloadInfo"`
	// LegacyPayload holds inline payload bytes as base64. It is omitted when
	// empty (external jobs). Old envelopes always emit it, possibly as "".
	LegacyPayload string `json:"payload,omitempty"`
	// PayloadRef is the external payload descriptor. Mutually exclusive with
	// LegacyPayload: exactly one is non-empty when the job is in a finalized
	// state (pending/claimed/completed/dead).
	PayloadRef *payloadRefDescriptor `json:"payloadRef,omitempty"`
	// PrepToken is a random token written into the publishing envelope so
	// that ambiguous CAS-create results can be reconciled.
	PrepToken    string        `json:"prepToken,omitempty"`
	PublishingAt *time.Time    `json:"publishingAt,omitempty"`
	CompletedAt  *time.Time    `json:"completedAt,omitempty"`
	Dead         *deadEnvelope `json:"dead,omitempty"`
	PurgedAt     *time.Time    `json:"purgedAt,omitempty"`
}

// encodeJob serializes env to JSON bytes.
func encodeJob(env *jobEnvelope) ([]byte, error) {
	return json.Marshal(env)
}

// decodeJob parses a job envelope. It validates that exactly one payload
// representation exists for finalized states.
func decodeJob(data []byte) (*jobEnvelope, error) {
	var env jobEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	// Validate payload representation for finalized states.
	switch env.State {
	case statePending, stateClaimed, stateCompleted, stateDead:
		hasInline := env.LegacyPayload != ""
		hasExternal := env.PayloadRef != nil
		if hasInline && hasExternal {
			return nil, fmt.Errorf("queue: job has both inline and external payload")
		}
		if hasInline {
			if _, err := base64.StdEncoding.DecodeString(env.LegacyPayload); err != nil {
				return nil, fmt.Errorf("queue: invalid inline payload base64: %w", err)
			}
		}
		if hasExternal && (env.PayloadInfo.ID != env.PayloadRef.BlobRef.Hash || env.PayloadInfo.Size != env.PayloadRef.BlobRef.Size || env.PayloadInfo.Size < 0) {
			return nil, fmt.Errorf("queue: external payload metadata mismatch")
		}
		// An empty payload (both representations absent) is valid; it
		// decodes as a zero-length inline payload. This preserves backward
		// compatibility with old envelopes that had empty payloads.
	case statePublishing, statePurging:
		// Publishing/purging may not have a finalized payload yet (publishing)
		// or may retain the descriptor (purging for cleanup).
	default:
		return nil, fmt.Errorf("queue: unknown job state %q", env.State)
	}
	return &env, nil
}

// jobInlinePayload returns the decoded inline payload from an envelope.
func jobInlinePayload(env *jobEnvelope) ([]byte, error) {
	if env.LegacyPayload == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(env.LegacyPayload)
}

// isExternalJob reports whether the envelope stores its payload externally.
func isExternalJob(env *jobEnvelope) bool {
	return env.PayloadRef != nil
}

// newInlineJobEnvelope builds a pending job envelope with inline payload.
func newInlineJobEnvelope(id, queue string, shard uint16, payload []byte, createdAt, notBefore time.Time) *jobEnvelope {
	sum := sha256.Sum256(payload)
	return &jobEnvelope{
		ID:            id,
		Queue:         queue,
		Shard:         shard,
		State:         statePending,
		Attempts:      0,
		CreatedAt:     createdAt,
		NotBefore:     notBefore,
		PayloadInfo:   PayloadInfo{ID: hex.EncodeToString(sum[:]), Size: int64(len(payload))},
		LegacyPayload: base64.StdEncoding.EncodeToString(payload),
	}
}

// newPublishingJobEnvelope builds a publishing-state envelope with metadata
// but no payload bytes. payloadInfo carries the payload ID and size so that
// maintenance can resume publication after a crash.
func newPublishingJobEnvelope(id, queue string, shard uint16, prepToken string, payloadInfo PayloadInfo, createdAt, notBefore time.Time) *jobEnvelope {
	return &jobEnvelope{
		ID:           id,
		Queue:        queue,
		Shard:        shard,
		State:        statePublishing,
		Attempts:     0,
		CreatedAt:    createdAt,
		NotBefore:    notBefore,
		PrepToken:    prepToken,
		PublishingAt: &createdAt,
		PayloadInfo:  payloadInfo,
	}
}

// newExternalJobEnvelope builds a pending job envelope with an external
// payload descriptor.
func newExternalJobEnvelope(id, queue string, shard uint16, ref *payloadRefDescriptor, payloadInfo PayloadInfo, createdAt, notBefore time.Time) *jobEnvelope {
	return &jobEnvelope{
		ID:          id,
		Queue:       queue,
		Shard:       shard,
		State:       statePending,
		Attempts:    0,
		CreatedAt:   createdAt,
		NotBefore:   notBefore,
		PayloadInfo: payloadInfo,
		PayloadRef:  ref,
	}
}

// idempotencyKeyHash returns the deterministic hash used for idempotent job ids.
func idempotencyKeyHash(queueName, key string) string {
	h := sha256.Sum256([]byte(queueName + "|" + key))
	return hex.EncodeToString(h[:])
}

// randomSuffix returns 16 hex digits from crypto/rand.
func randomSuffix() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand reads from the operating-system entropy source and
		// essentially never returns an error in practice.
		panic(fmt.Sprintf("queue: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// randomToken returns 32 hex digits from crypto/rand for lease tokens.
func randomToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("queue: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// generateJobID returns a deterministic or random job id using the queue's
// injected clock. Without an IdempotencyKey the result is "<usec20>-<rand16hex>".
func generateJobID(queueName, idempotencyKey string, now time.Time) string {
	if idempotencyKey != "" {
		return "idem-" + idempotencyKeyHash(queueName, idempotencyKey)
	}
	return fmt.Sprintf("%020d-%s", now.UnixMicro(), randomSuffix())
}

// shardForJob computes the shard for jobID using FNV-1a 32-bit.
func shardForJob(jobID string, shards uint16) uint16 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(jobID))
	return uint16(h.Sum32() % uint32(shards))
}

// shardHex formats a shard as 4 lower-case hex digits.
func shardHex(shard uint16) string {
	return fmt.Sprintf("%04x", shard)
}

// ts20 formats a time as a zero-padded 20-digit decimal microsecond value.
func ts20(t time.Time) string {
	return fmt.Sprintf("%020d", t.UnixMicro())
}

// jobAppKey returns the cas application key for a canonical job object.
func jobAppKey(shard uint16, jobID string) string {
	return fmt.Sprintf("shard/%s/jobs/%s", shardHex(shard), jobID)
}

// ownerRefName returns the deterministic tree ref name for a job's payload
// ownership ref.
func ownerRefName(shard uint16, jobID, token string) string {
	return fmt.Sprintf("job/%s/%s/%s", shardHex(shard), jobID, token)
}

// readyMarkerKey returns the raw backend object key for a ready marker.
func (q *Queue) readyMarkerKey(shard uint16, ts time.Time, jobID string) string {
	return fmt.Sprintf("%sshard/%s/ready/%s/%s", q.prefix, shardHex(shard), ts20(ts), jobID)
}

// leaseMarkerKey returns the raw backend object key for a lease marker.
func (q *Queue) leaseMarkerKey(shard uint16, ts time.Time, jobID string) string {
	return fmt.Sprintf("%sshard/%s/lease/%s/%s", q.prefix, shardHex(shard), ts20(ts), jobID)
}

// deadMarkerKey returns the raw backend object key for a dead marker.
func (q *Queue) deadMarkerKey(shard uint16, ts time.Time, jobID string) string {
	return fmt.Sprintf("%sshard/%s/dead/%s/%s", q.prefix, shardHex(shard), ts20(ts), jobID)
}

// parseMarker extracts shard, kind, timestamp, and jobID from a raw marker
// key. kind is one of "ready", "lease", or "dead".
func (q *Queue) parseMarker(key string) (shard uint16, kind string, ts time.Time, jobID string, ok bool) {
	prefix := q.prefix + "shard/"
	if !strings.HasPrefix(key, prefix) {
		return
	}
	rest := key[len(prefix):]
	parts := strings.SplitN(rest, "/", 4)
	if len(parts) != 4 {
		return
	}
	var err error
	shard64, err := strconv.ParseUint(parts[0], 16, 16)
	if err != nil {
		return
	}
	kind = parts[1]
	if kind != "ready" && kind != "lease" && kind != "dead" {
		return
	}
	usec, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return
	}
	return uint16(shard64), kind, time.UnixMicro(usec).UTC(), parts[3], true
}

// newJobEnvelope is a backward-compatible alias for newInlineJobEnvelope.
// Existing tests and callers that construct inline envelopes use this name.
func newJobEnvelope(id, queue string, shard uint16, payload []byte, createdAt, notBefore time.Time) *jobEnvelope {
	return newInlineJobEnvelope(id, queue, shard, payload, createdAt, notBefore)
}

// jobPayload is a backward-compatible alias for jobInlinePayload.
// Existing tests and callers that decode inline payloads use this name.
func jobPayload(env *jobEnvelope) ([]byte, error) {
	return jobInlinePayload(env)
}
