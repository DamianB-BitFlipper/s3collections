# Failure Modes and Crash Windows

This document describes what can go wrong between any two S3 calls, how the
library recovers, and where data loss is possible.

## CAS store (`cas.Store`)

All mutating CAS operations are read-modify-write loops: `Get` (or head-read),
compute, then `Put` with a precondition. The crash window is between any two
backend calls.

| Crash point | Outcome | Repair path |
|-------------|---------|-------------|
| Before `Get` | No state change. | Caller retries. |
| After `Get`, before `Put` | No state change. | Caller re-reads and retries. |
| After `Put` success, before return | Update is durable. | Idempotent retry re-reads the same revision and returns it. |
| `Put` returns 412 | No state change. | `Update` retries after backoff; `Create`/`CompareAndSwap`/`Delete` return conflict to caller. |
| `Put` returns 5xx/SlowDown | Ambiguous: write may or may not have applied. | Idempotent retry re-reads; if applied, returns success; otherwise retries the CAS. |

* **Ambiguous writes.** S3 may apply a conditional `Put` but return a retryable
  service error. All CAS operations are idempotent with respect to their
  revision: re-reading reveals whether the write applied.
* **Corrupt envelopes.** During `List`/`GC`, corrupt objects are skipped and
  counted by `s3collections_corrupt_total`. They are not repaired automatically;
  operators should investigate and remove them manually.
* **Data-loss boundary.** `cas` never physically deletes live objects. The only
  data loss path is explicit `Delete` (tombstone) followed by GC before the
  retention window passes. Choose `TombstoneRetention` and `ClockSkewHint`
  longer than any reader/writer that must observe the tombstone.

## LRU store (`lru.Store`)

| Crash point | Outcome | Repair path |
|-------------|---------|-------------|
| `Set` crashes after backend `Put` | Metadata is durable. | Idempotent retry re-reads and returns. |
| `Set` on tombstone races with evictor GC | Freshly live entry may be physically deleted. | Manifests as `Get` miss; next `Set` restores it. |
| Evictor crashes during a shard pass | Partial eviction is durable; remaining over-capacity entries are evicted on the next pass. | Restart evictor. |
| `Touch` crashes after CAS | Touch applied; caller may retry. | Safe to retry. |

* **Resurrection race window.** The evictor lists tombstones, verifies age,
  and calls `cas.GC`. A `Set` that resurrects the same key in the middle of
  that window can be deleted. The window is roughly one S3 RTT. Set
  `TombstoneMinAge` (default 24h) much larger than your expected RTT, or set
  it negative to disable physical GC and accept tombstone accumulation.
* **Capacity overshoot.** Eviction is lazy and sampled per shard tick. A
  sudden burst can temporarily exceed `CapacityBytes`/`CapacityItems` until
  the next eviction pass catches up.
* **What is guaranteed never lost.** There is no raw `Delete` of live LRU
  entries; eviction writes a tombstone first. Live data is lost only by
  eviction (a cache miss) or the resurrection race above.

## Work queue (`queue.Queue`)

The queue separates canonical job state (`cas.Store`) from marker objects
(raw backend objects). Markers are append-only hints; the reaper reconciles
markers against canonical state.

### Crash windows for enqueue

| Crash point | Outcome | Repair path |
|-------------|---------|-------------|
| Before canonical `cas.Create` | No job. | Caller retries; idempotency key yields same job id. |
| After canonical create, before ready marker | Job exists but may be invisible to `Claim` until reaper backfills the marker. | `StartMaintenance` backfills missing ready markers. |
| After ready marker | Job is visible and claimable. | — |

### Crash windows for claim

| Crash point | Outcome | Repair path |
|-------------|---------|-------------|
| After `cas.Update` to claimed, before lease marker | Job is claimed; the lease marker may be missing. | Reaper backfills the lease marker if it later sees a claimed job. |
| After lease marker, before deleting ready marker | Both markers exist briefly; reaper reconciles. | Reaper deletes stale ready markers. |
| Before updating canonical job to claimed | No claim. | Next caller claims. |

### Crash windows for complete/retry/dead

| Crash point | Outcome | Repair path |
|-------------|---------|-------------|
| After canonical update, before marker cleanup | Stale lease/dead marker; next reaper/GC pass deletes it. | Idempotent retry of `Complete` succeeds; reaper cleans markers. |
| Before canonical update | Job state unchanged. | Caller retries; may get `ErrStaleLease` if another worker changed it. |

### Transient 5xx / SlowDown handling

* All backend calls are retried with bounded jittered backoff using the
  configured `RetryPolicy`.
* Markers use independent retry loops; a marker write failure is logged as a
  warning but does not fail the user-facing operation.
* If retries are exhausted, the operation returns the underlying error. The
  canonical state remains consistent because mutations are conditional.

### Poison messages

* A job whose payload cannot be processed should be moved to dead-lettered
  via `Job.Dead(ctx, reason)`. It remains inspectable via `ListDead` and
  replayable via `RequeueDead`.
* If a worker repeatedly calls `Retry` without progress, `MaxAttempts`
  dead-letters the job automatically.

### Reaper / maintenance crash behavior

* `StartMaintenance` starts one background goroutine per `Queue`. If the
  process crashes, maintenance does not run until restarted.
* A stopped reaper causes expired leases to remain unclaimed until it
  restarts, and completed/dead jobs to remain past retention. It does **not**
  lose jobs.
* A stopped GC causes marker and tombstone accumulation; storage costs grow
  but correctness is unaffected.

### Data-loss boundaries

* **Tombstone GC race (LRU).** Described above. Window ≈ one S3 RTT; default
  `TombstoneMinAge` is 24h.
* **CAS tombstone GC race.** `cas.GC` deletes tombstones older than
  `TombstoneRetention - ClockSkewHint` (default 5m − 2m = 3m effective). A
  queue re-enqueue with the same idempotency key after GC will create a new
  job, which is usually the desired behavior.
* **Guaranteed not lost.** Pending jobs, claimed jobs with unexpired leases,
  completed jobs within retention, and dead-lettered jobs within retention
  are never physically deleted.
