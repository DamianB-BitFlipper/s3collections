package queue

import "errors"

// Sentinel errors returned by Queue and Job methods.
var (
	// ErrEmpty is returned by Claim when no claimable job exists in the
	// probed shards.
	ErrEmpty = errors.New("queue empty")

	// ErrStaleLease is returned by Renew/Complete/Retry/Dead when the job
	// was concurrently modified and the caller no longer holds the lease.
	ErrStaleLease = errors.New("lease lost or expired; state changed")

	// ErrNotLeased is returned by Renew/Complete/Retry/Dead when the job is
	// not currently claimed by this WorkerID.
	ErrNotLeased = errors.New("job not leased by this worker")

	// ErrFenceStale is returned by Guard when the provided fence token is
	// older than the latest known fence.
	ErrFenceStale = errors.New("stale fence")

	// ErrPayloadTooLarge is returned by Enqueue or EnqueueReader when the
	// payload exceeds MaxPayloadBytes.
	ErrPayloadTooLarge = errors.New("queue: payload exceeds maximum size")

	// ErrBackendCapability is returned when the backend does not support a
	// required streaming capability (e.g. multipart or stream PUT).
	ErrBackendCapability = errors.New("queue: backend capability unavailable")

	// ErrPayloadNotFound is returned by OpenPayload when the external payload
	// can no longer be found (e.g. after GC).
	ErrPayloadNotFound = errors.New("queue: payload not found")
)
