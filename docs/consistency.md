# Consistency Guarantees

This document states the consistency model of `s3collections` precisely. It is
intended for operators designing around the library and assumes familiarity
with the architecture doc (`docs/design/00-architecture.md`) and the binding
contract (`docs/design/04-consolidation.md`).

## Foundation: what S3 provides

The library relies only on these S3 semantics:

* Strong read-after-write for `Get` and `List`.
* Atomic per-key `Put`.
* Conditional writes: `If-None-Match: *` (create-if-absent) and
  `If-Match: <etag>` (compare-and-swap), returning `412 Precondition Failed`
  on conflict.
* Unconditional per-key `Delete`.
* Strongly consistent, lexicographic, paginated prefix listing.

No multi-key transactions, no conditional delete, and no global ordering are
assumed.

## Per-component guarantees

### `cas.Store` — per-key linearizability

* Every successful mutating call on a single application key is linearizable:
  `Create`, `CompareAndSwap`, `Update`, and `Delete` act as one atomic
  read-modify-write. Once a call returns success, all later `Get`/`List`
  calls observe the new state; once it returns a conflict error, no caller
  observed the attempted state.
* `Get` returns the latest live value or `ErrNotFound`. Tombstoned keys are
  indistinguishable from absent keys to `Get`. Use `GetMeta` to inspect
  tombstone metadata.
* `CompareAndSwap` succeeds only if the current live revision equals the
  supplied `expect` value; otherwise it returns `ErrConflict`.
* `Update` invokes `fn` with the current `Record` (live or, with
  `WithIncludeTombstone()`, tombstoned). `fn` must be side-effect free; it
  may be called multiple times under contention. A returned `next` slice that
  is nil or equal to the current value is a no-op.
* `Delete` writes a tombstone envelope and increments the revision. It
  requires the caller to supply the expected current revision and returns
  `ErrConflict` on a mismatch.

#### Revision semantics and fencing

* `Revision` is a monotonic counter per key, incremented on every successful
  mutating CAS operation. It is a valid fencing token for downstream effects:
  if a caller observes revision `R` and later observes `R' > R`, the state
  associated with `R'` superseded the state associated with `R`.
* The `queue` package exposes this revision as `Job.Fence`; downstream
  consumers can use `queue.Guard(latest, provided)` to reject stale effects.
* Revisions are **not** comparable across different keys.

#### Resurrection

* `cas.Update` forbids resurrection by default: if the key is a tombstone and
  `WithResurrect()` is not supplied, it returns `ErrDeleted`.
* With `WithResurrect()`, `fn` receives the tombstone `Record` and may return
  a live value. The resulting record has `rev = tombRev + 1`, `state = live`,
  `created_at` preserved, and `updated_at = now`.
* Resurrection races with physical tombstone GC. It is safe only when
  tombstones of the affected key family are never physically deleted, or when
  retention is vastly larger than a single operation's duration.

### `lru.Store` — approximate recency and safe eviction

* `Set` on a missing key creates it. `Set` on an existing live key updates
  metadata. `Set` on a tombstoned key resurrects it (using
  `cas.WithResurrect()`).
* `Get` returns the latest metadata for the key. With `TouchOnGet`, it also
  calls `Touch` internally; touch failures that are not `ErrNotFound` are
  logged but do not fail `Get`.
* `Touch` records recent use. It coalesces writes according to
  `TouchPolicy.CoalesceWindow` to avoid hot-key write storms.
* The evictor maintains a per-shard CLOCK: it clears access bits on one pass
  and evicts entries whose bit was not set by the next pass, but only when
  the shard is over its proportional capacity target.
* **What can be lost.** Under capacity pressure, an entry that has not been
  touched recently may be evicted. Because eviction is a CAS tombstone, a
  concurrent `Set` that resurrects the same key races with the evictor's
  physical GC; if `TombstoneMinAge` is too small relative to S3 RTT, the
  freshly resurrected entry can be physically deleted. This manifests as a
  cache miss, self-healing on the next `Set`.
* **What is guaranteed never lost.** A successfully `Set` value persists at
  least until another successful `Set` on the same key, or until it is
  evicted by the evictor. A value that is actively touched will not be
  evicted.
* `Len()` and `Stats()` are approximate under concurrency; they perform
  full-list scans and should not be on the hot path.

### `queue.Queue` — at-least-once delivery plus deduplication-through-retention

* `Enqueue` stores the job under a canonical key. With an `IdempotencyKey`,
  repeated enqueues return `existed=true` while the canonical object exists
  (live, completed, or tombstone within retention). Without an idempotency
  key, every enqueue creates a distinct job.
* `Claim` atomically transitions a pending job to `claimed` via CAS and
  records a lease. The job is not visible to other callers until the lease
  expires or the job is completed/retried/dead-lettered.
* `Complete`, `Retry`, and `Dead` are valid only for jobs currently claimed
  by the caller's `WorkerID`. They return `ErrNotLeased` if the job is not
  leased by this worker, and `ErrStaleLease` if the underlying CAS revision
  changed (another worker claimed/reaped the job).
* `Complete` is idempotent: calling it on an already completed or dead job
  returns nil.
* **At-least-once delivery.** A job is delivered at least once unless it is
  explicitly completed before the lease expires. Crashes between a successful
  downstream effect and `Complete` can cause duplicate delivery after the
  lease expires.
* **Fence-guarded exactly-once downstream effects.** The `Job.Fence` revision
  is monotonic per job. Consumers can record the highest processed fence per
  job id and reject effects with `queue.Guard(latest, provided)`. This does
  not prevent duplicate *claims*, only duplicate *effects*.
* **Dead-lettering.** `Retry` moves a claimed job back to pending, or to
  `dead` if `MaxAttempts` would be exceeded. `Dead` moves it to dead-lettered
  immediately. `ListDead` and `RequeueDead` allow operational inspection and
  replay.
* **Ordering.** In sharded mode, ordering is per-shard rough FIFO based on
  ready-marker timestamps, not strict. In sequencer mode
  (`SequencerEnabled`), all jobs land in shard 0 and a single CAS sequencer
  key enforces a total enqueue order; throughput is limited to roughly tens
  of enqueues per second.

## Cross-key invariants

The library never relies on multi-key atomicity. Cross-key invariants are
maintained optimistically and repaired by background loops:

* Queue ready/lease/dead markers are raw backend objects that can lag the
  canonical job state. The reaper backfills missing markers and deletes stale
  ones.
* LRU evictors and queue reapers may run on every replica concurrently; their
  work is idempotent.
* Clock skew is treated as a safety margin, not a correctness mechanism.

## Clock-skew policy per component

| Component | Clock parameter | Default | Purpose |
|-----------|-----------------|---------|---------|
| `cas` | `ClockSkewHint` | 2m | Extra margin subtracted from GC cutoff so tombstones are physically deleted only when they are safely older than retention. |
| `lru` | `TombstoneMinAge` | 24h | Minimum age before physical deletion of evicted entries. Negative disables physical GC. |
| `queue` | `ClockSkewTolerance` | 2s | Margin for visibility timeouts and reaper lease expiry; prevents premature or delayed reclaims due to replica clock differences. |

The wall clock is never used to prove mutual exclusion or to order events
across keys. Correctness reduces to CAS revision monotonicity within a single
key.
