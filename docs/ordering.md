# Ordering: Strict Global vs. Sharded

`s3collections` offers two ordering models. The choice is made per queue at
construction time via `queue.Options.SequencerEnabled`.

## Strict global ordering (sequencer mode)

When `SequencerEnabled` is true, all jobs are enqueued into shard 0 and a
single CAS key (`sequencer`) is incremented via `cas.Update` for every
enqueue without an idempotency key. The resulting sequence number is embedded
in the job id, giving all jobs a total order.

### Ceiling

Each enqueue is a read-modify-write on one hot key. With an S3 RTT of
80–150 ms and the inevitable retries under contention, practical throughput
is roughly **tens of successful enqueues per second**. Do not use this mode
for high-volume work.

### When to use

* Administrative or configuration streams.
* Work that genuinely requires a global total order and low rate.
* Scenarios where `Job.Fence` alone is not enough because cross-job ordering
  matters.

## Scalable sharded operation (default)

By default a queue uses `Shards` (default 256, max 65535) independent shards.
Each shard has its own ready/lease/dead marker prefixes and canonical job
prefix. Jobs are hashed by job id to a shard.

### Per-shard rough FIFO

Within a single shard, `Claim` scans ready markers in lexicographic order.
Ready-marker keys include a 20-digit microsecond timestamp, so the scan
roughly follows enqueue time. This is **not strict FIFO**:

* Concurrent enqueues can produce out-of-order timestamps due to clock skew
  and network delays.
* A delayed marker write (enqueue succeeded, marker failed) can make a job
  temporarily invisible until the reaper backfills it.
* `Claim` probes only `ClaimShardProbe` shards by default, so a worker may
  claim from shard 5 before shard 1 if the probe order lands that way.

Cross-shard ordering is partial at best. Consumers must tolerate reordering.

### Throughput scaling

Aggregate enqueue throughput scales roughly linearly with shard count,
assuming uniform job-id distribution. Claim throughput scales with the number
of replicas and shards, bounded by the per-shard ready-marker LIST cost.

## Guidance for choosing shard counts

* Start with the default 256 queue shards unless you have a reason to change.
* Increase shards if:
  * Claim latency spikes because ready-marker prefixes grow large.
  * You observe hot-shard symptoms (`s3collections_conflicts_total` spikes
    on `enqueue` for one shard).
* Decrease shards if:
  * Reaper/GC LIST cost dominates your S3 bill.
  * You have fewer workers than shards and many empty scans.

For LRU, shard count controls evictor parallelism and per-shard LIST size.
The same trade-off applies: more shards reduce hot-key collisions but
increase scan overhead.

## Migration path

There is **no in-place migration** between sharded and sequencer mode. The
choice is fixed at queue construction because it changes the job id format,
shard routing, and storage layout.

To switch modes:

1. Drain the old queue.
2. Create a new queue with the desired `SequencerEnabled` value.
3. Point producers and consumers at the new queue.

If you need both ordering models over the same logical stream, use two
separate queues.
