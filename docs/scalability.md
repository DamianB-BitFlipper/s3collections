# Scalability and Cost Model

This document helps operators size `s3collections` deployments and understand
S3 costs.

## Per-key throughput ceiling

The fundamental limit is one successful mutating CAS operation per S3 RTT per
hot key under low contention:

```
max steady-state ≈ 1 / RTT
```

With a typical RTT of 80–150 ms, expect roughly 7–12 successful writes per
second per hot key. Under contention, expected attempts per operation rise,
so practical throughput falls.

| Contention | Expected attempts/op | Effective throughput (100 ms RTT) |
|------------|---------------------:|----------------------------------:|
| 1 writer | 1.0 | ~10 ops/s |
| 2 writers | ~1.3 | ~7–8 ops/s |
| 4 writers | ~2.0 | ~5 ops/s |
| Many writers | ~geometric | approaches `1 / (RTT × writers)` |

If a single logical key is hot, shard it or use the sharded components below.

## LIST costs

LIST is the most expensive primitive because it scans prefixes. Operations
that issue LIST:

* `cas.Store.List` and `cas.Store.GC`.
* `lru.Store.Len`, `lru.Store.Stats`, and evictor scans.
* `queue.Queue.Claim` (ready markers), reaper (lease/dead markers), and GC
  (canonical jobs).

Every LIST call costs one S3 API request and returns up to 1000 keys. Tune
page sizes and shard counts to bound the keys scanned per call.

## Shard-count selection

### LRU

```go
Shards = max(desired_parallel_eviction, expected_hot_key_fanout)
```

* Default: 128.
* More shards reduce per-shard LIST size and hot-key collisions, but increase
  the number of LIST calls the evictor makes per tick.
* Recommended: 64–1024 for most workloads. Use a power of two or decimal
  round number; shard strings are zero-padded decimal.
* Each shard contributes one proportional capacity target:
  `targetBytes = CapacityBytes / ShardCount * 0.95`.

### Queue

```go
Shards = max(throughput / per_shard_target, fanout_workers)
```

* Default: 256, maximum 65535.
* More shards spread ready-marker LIST load and reduce claim latency, but
  increase reaper/GC scan work linearly.
* Recommended starting point: 256. Increase if `s3collections_queue_depth`
  is high and Claim latency spikes; decrease if reaper cost dominates.
* `ClaimShardProbe` (default `min(Shards, 8)`) bounds how many shards a
  single `Claim` scans.

## S3 operations cost table

| Operation | Backend calls (happy path) | Notes |
|-----------|---------------------------:|-------|
| `cas.Create` | 1 `Put` | Failed create costs 1 `Put` (412). |
| `cas.Get` | 1 `Get` | Plus retries on transient errors. |
| `cas.CompareAndSwap` | 1 `Get` + 1 `Put` | Plus conflict retries. |
| `cas.Update` | 1 `Get` + 1 `Put` | `fn` may be invoked multiple times. |
| `cas.Delete` | 1 `Get` + 1 `Put` (tombstone) | Physical delete later via `GC`. |
| `cas.List` | 1+ `List` + 1 `Get` per returned object | Expensive for large prefixes. |
| `cas.GC` | 1+ `List` + 1 `Delete` per tombstone | Scans all tombstones under prefix. |
| `lru.Set` (fast path) | 1 `Put` (create) | Slow path: 1 `Get` + 1 `Put`. |
| `lru.Get` | 1 `Get` | Plus optional `Touch`. |
| `lru.Touch` | 1 `Get` + 1 `Put` | Coalesced by `TouchPolicy`. |
| `queue.Enqueue` | 1 `Put` (canonical) + best-effort marker `Put` | Idempotent re-enqueue is 1 `Get` effectively via CAS conflict. |
| `queue.Claim` | 1+ `List` + 1 `Get` + 1 `Put` + marker writes | Depends on `ClaimMaxPages` and shard probe. |
| `queue.Complete` | 1 `Get` + 1 `Put` + marker deletes | Marker deletes are best-effort. |

## Marker cardinality

Queue markers are zero-byte raw objects. Their count equals the sum of
ready, leased, and dead jobs at any moment. Markers are append-only by design
(timestamps are in the key), so a thrashing job can create multiple ready and
lease markers; the reaper/GC cleans stale ones.

## Recommended instance counts

* Stateless replicas can be scaled horizontally without coordination.
* Run background loops (`StartEvictor`, `StartMaintenance`) on **every**
  replica. The work is idempotent; duplicate effort is cheaper than missing a
  loop during failover.
* For the queue, more replicas increase claim concurrency linearly up to the
  aggregate shard throughput.

## When NOT to use this library

* **High-QPS single key.** If one logical key sustains >~10 writes/s,
  S3's per-key latency will throttle you. Use a real database or a
  memory-hot tier.
* **Strict global ordering at scale.** Sequencer mode enforces total order
  but is capped at roughly tens of enqueues/s. Sharded mode gives only
  per-shard rough FIFO.
* **Large payloads.** Queue payloads are capped at 256 KiB by default
  (`MaxPayloadBytes`). Store large blobs elsewhere and put references in the
  queue.
* **Cross-key transactions.** The library provides no multi-key atomicity.
* **Sub-millisecond latencies.** Every mutating operation costs at least one
  S3 RTT.

## Sizing checklist

1. Measure your hot-key write rate; ensure it is below `1 / RTT`.
2. Choose shard counts to bound LIST page sizes under 1000.
3. Set retention windows (`TombstoneMinAge`, `CompletedRetention`,
   `DeadRetention`) longer than your longest replay/repair horizon.
4. Estimate LIST calls per second from evictor interval and queue reaper
   interval; ensure it fits your S3 budget.
5. Monitor `s3collections_latency_seconds`, `s3collections_cas_attempts`, and
   `s3collections_conflicts_total` to detect hot keys.
