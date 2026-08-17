package queue

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Lease is a snapshot of a job lease.
type Lease struct {
	// Owner is the WorkerID that holds the lease.
	Owner string
	// Expiry is the lease expiration time.
	Expiry time.Time
}

// Job represents a claimed job returned by Claim.
type Job struct {
	// ID is the unique job identifier.
	ID string
	// Queue is the queue name.
	Queue string
	// Shard is the shard index containing the job.
	Shard uint16
	// Payload is the user-supplied payload bytes.
	Payload []byte
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

	q      *Queue
	appKey string
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
	statePending   jobState = "pending"
	stateClaimed   jobState = "claimed"
	stateCompleted jobState = "completed"
	stateDead      jobState = "dead"
)

// leaseEnvelope is the JSON form of a lease.
type leaseEnvelope struct {
	Owner  string    `json:"owner"`
	Expiry time.Time `json:"expiry"`
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

// jobEnvelope is the canonical JSON representation of a job stored by cas.
type jobEnvelope struct {
	ID          string           `json:"id"`
	Queue       string           `json:"queue"`
	Shard       uint16           `json:"shard"`
	State       jobState         `json:"state"`
	Attempts    int              `json:"attempts"`
	CreatedAt   time.Time        `json:"createdAt"`
	NotBefore   time.Time        `json:"notBefore"`
	Lease       *leaseEnvelope   `json:"lease,omitempty"`
	Reasons     []reasonEnvelope `json:"reasons,omitempty"`
	Payload     string           `json:"payload"`
	CompletedAt *time.Time       `json:"completedAt,omitempty"`
	Dead        *deadEnvelope    `json:"dead,omitempty"`
}

// encodeJob serializes env to JSON bytes.
func encodeJob(env *jobEnvelope) ([]byte, error) {
	return json.Marshal(env)
}

// decodeJob parses a job envelope. It validates the payload base64.
func decodeJob(data []byte) (*jobEnvelope, error) {
	var env jobEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	if env.Payload != "" {
		if _, err := base64.StdEncoding.DecodeString(env.Payload); err != nil {
			return nil, fmt.Errorf("queue: invalid payload base64: %w", err)
		}
	}
	return &env, nil
}

// jobPayload returns the decoded payload from an envelope.
func jobPayload(env *jobEnvelope) ([]byte, error) {
	if env.Payload == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(env.Payload)
}

// newJobEnvelope builds a pending job envelope.
func newJobEnvelope(id, queue string, shard uint16, payload []byte, createdAt, notBefore time.Time) *jobEnvelope {
	return &jobEnvelope{
		ID:        id,
		Queue:     queue,
		Shard:     shard,
		State:     statePending,
		Attempts:  0,
		CreatedAt: createdAt,
		NotBefore: notBefore,
		Payload:   base64.StdEncoding.EncodeToString(payload),
	}
}

// generateJobID returns a deterministic or random job id.
func generateJobID(queueName, idempotencyKey string, rnd *rand.Rand) string {
	if idempotencyKey != "" {
		h := sha256.Sum256([]byte(queueName + "|" + idempotencyKey))
		return "idem-" + hex.EncodeToString(h[:])
	}
	usec := time.Now().UTC().UnixMicro()
	b := make([]byte, 8)
	if rnd != nil {
		_, _ = rnd.Read(b)
	} else {
		_, _ = rand.Read(b)
	}
	return fmt.Sprintf("%020d-%s", usec, hex.EncodeToString(b))
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
