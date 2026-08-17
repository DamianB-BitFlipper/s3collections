# Operations Guide

This guide covers running background loops, retention knobs, capacity
planning, and recovery for production deployments.

## Background loops

### LRU evictor

Start one evictor per `lru.Store`:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
if err := store.StartEvictor(ctx); err != nil {
    log.Fatal(err)
}
```

* Runs `EvictorWorkers` goroutines (default `min(ShardCount, 4)`).
* Each worker ticks every `EvictorInterval` (default 2s) and processes its
  assigned shards.
* Initial startup is jittered to avoid stampedes.
* Call `Close()` to stop the evictor and mark the store closed.
* **Every replica should run it.** The work is idempotent; running multiple
  evictors only wastes some LIST calls, which is cheaper than losing eviction
  during a replica failure.

### Queue maintenance

Start one maintenance loop per `queue.Queue`:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
q.StartMaintenance(ctx)
```

* A single goroutine runs reaper passes every `ReaperInterval` (default 5s,
  jittered) and GC passes every `GCInterval` (default 5m, jittered).
* **Every replica should run it.** Passes are idempotent.
* If the loop is already running, additional calls are no-ops.
* The loop stops when the context is canceled.

## Idempotency and jitter

Both loops add jitter to their intervals so replicas do not thunder-herd S3.
Because every pass is idempotent, overlapping passes are safe and only
increase cost slightly.

## Retention knobs and safety margins

| Knob | Default | Meaning | Safety margin |
|------|---------|---------|---------------|
| `cas.TombstoneRetention` | 5m | How long a CAS tombstone is retained before physical deletion. | `cas.ClockSkewHint` (default 2m) is subtracted from the GC cutoff. Effective minimum age ≈ 3m. |
| `lru.TombstoneMinAge` | 24h | Minimum age of an evicted entry's tombstone before physical deletion. | Should be >> S3 RTT to prevent losing freshly resurrected entries. Set < 0 to disable physical GC. |
| `queue.CompletedRetention` | 24h | How long completed jobs are kept before tombstoning. | Jobs remain listable/completable during this window. |
| `queue.DeadRetention` | 7d | How long dead-lettered jobs are kept before tombstoning. | Allows time for `ListDead` inspection and `RequeueDead` replay. |
| `queue.ClockSkewTolerance` | 2s | Margin for visibility timeouts and reaper decisions. | Larger values delay reclaim of expired leases; smaller values risk premature reclaim under skew. |

Set retention values longer than the longest operation or replay window you
care about. For example, if you replay dead letters up to 48 hours later,
set `DeadRetention` to at least 72 hours.

## Capacity planning for tombstone accumulation

### CAS tombstones

* Each tombstone is a small JSON envelope (~150 bytes plus S3 overhead).
* `cas.GC` deletes tombstones older than
  `TombstoneRetention - ClockSkewHint`.
* If you disable physical GC (`lru.TombstoneMinAge < 0`), tombstones for
  every distinct key ever evicted accumulate indefinitely. At ~150 bytes per
  tombstone plus S3 overhead, this is cheap for thousands of keys but should
  be monitored for millions.

### Queue markers

* Ready, lease, and dead markers are zero-byte raw objects.
* Markers for completed/dead jobs are cleaned after retention.
* A thrashing job can create multiple markers per transition; the reaper/GC
  removes stale ones.

## Recovery runbook

### Queue appears wedged (Claim returns ErrEmpty but jobs exist)

1. Verify `StartMaintenance` is running on at least one replica.
2. Check `s3collections_reaper_runs_total` and `s3collections_queue_depth`.
3. Look for warnings about reaper LIST failures.
4. If ready markers are missing (e.g., due to a transient marker write
   failure), the reaper backfills them from canonical job state during the
   next pass.
5. If leases expired without reclaim, increase `ClockSkewTolerance` or check
   replica clock synchronization.

### LRU is over capacity

1. Verify `StartEvictor` is running on at least one replica.
2. Check `s3collections_lru_entries` and `s3collections_lru_bytes`.
3. Ensure `CapacityBytes` and `CapacityItems` are set; zero disables that
   dimension.
4. If evictor workers are CPU-starved, increase `EvictorWorkers` (up to
   `ShardCount`) or reduce `EvictorInterval`.
5. Remember that eviction is lazy; short bursts above capacity are expected.

### Hot shard symptoms

* `s3collections_conflicts_total{component="queue",op="enqueue"}` spikes for
  one shard.
* `s3collections_cas_attempts` histogram shows high attempts for `enqueue`.
* Claim latency rises for one shard.

Mitigations:

* Increase `Shards` and create a new queue (requires migration).
* Use idempotency keys so re-enqueues are cheap conflicts rather than new
  tombstones.
* Avoid sequencer mode for high-volume streams.

### Data-loss suspicion

* Check `s3collections_corrupt_total`; non-zero values indicate envelopes that
  failed validation.
* Verify retention knobs are not shorter than your recovery window.
* For LRU, confirm `TombstoneMinAge` is >> S3 RTT.
* Review logs for exhausted retry budgets or marker write failures.

## Operational checklist

* [ ] `StartEvictor` is called for every `lru.Store` on every replica.
* [ ] `StartMaintenance` is called for every `queue.Queue` on every replica.
* [ ] A real `Meter` is wired and dashboards/alerters exist.
* [ ] Retention values are documented and longer than replay windows.
* [ ] S3 LIST costs are budgeted (evictor + reaper + GC scans).
* [ ] Replica clocks are synchronized (NTP) and `ClockSkewTolerance` covers
      observed drift.
* [ ] On-call runbook includes "queue wedged", "LRU over capacity", and
      "hot shard" sections.
