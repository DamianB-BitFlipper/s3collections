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
)
