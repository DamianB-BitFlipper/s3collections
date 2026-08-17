# s3collections

Distributed, S3-backed data structures for Go stateless services.

`s3collections` gives your replicas durable, strongly-consistent state without
running any database. It builds on the S3 guarantees you already rely on:
strong read-after-write `Get`/`List`, atomic per-key `Put`, and conditional
writes (`If-None-Match` / `If-Match`). Every operation is context-aware,
idempotent, instrumented, and retried with bounded jittered backoff.

The library is standard-library only and has no third-party dependencies.

## What it provides

* **`cas`** — a versioned compare-and-swap store. Per-key linearizable (`Create`, `Get`, `CompareAndSwap`, `Update`, `Delete`, `List`, `GC`).
* **`lru`** — a distributed LRU metadata store. Shards entries, tracks live
  bytes/items, and runs CLOCK eviction workers.
* **`queue`** — a durable at-least-once work queue with visibility timeouts,
  dead-lettering, idempotent enqueue, and optional strict global ordering.
* **`s3backend`** — the storage contract plus in-memory (`Memory`) and
  fault-injection (`Chaos`) backends, plus an AWS SigV4 HTTP/S3 client
  (`NewHTTPClient`).
* **`s3collections` (root)** — shared `Meter`, `Logger`, `Tracer`, `RetryPolicy`,
  `BackoffDelays`, and `CaptureMeter` test helper.

## Install

```bash
go get github.com/damianb/s3collections
```

Requires Go 1.26 or later.

## Quickstart

### CAS store

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/damianb/s3collections"
    "github.com/damianb/s3collections/cas"
    "github.com/damianb/s3collections/s3backend"
)

func main() {
    ctx := context.Background()
    be := s3backend.NewMemory()
    store, err := cas.New(be, "demo/",
        cas.WithWriterID("replica-1"),
        cas.WithTombstoneRetention(5*time.Minute),
    )
    if err != nil {
        log.Fatal(err)
    }

    rec, err := store.Create(ctx, "config/v1", []byte(`{"timeout":30}`))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("created at revision", rec.Revision)

    rec2, err := store.CompareAndSwap(ctx, "config/v1", rec.Revision, []byte(`{"timeout":60}`))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("updated to revision", rec2.Revision)
}
```

### LRU metadata store

```go
be := s3backend.NewMemory()
store, err := lru.New(be, lru.Options{
    ShardCount:      16,
    CapacityBytes:   1 << 30, // 1 GiB
    CapacityItems:   10000,
    TouchOnGet:      true,
    TombstoneMinAge: 24 * time.Hour,
})
if err != nil {
    log.Fatal(err)
}

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
if err := store.StartEvictor(ctx); err != nil {
    log.Fatal(err)
}

if err := store.Set(ctx, "user:42", lru.EntryMeta{
    SizeBytes:    4096,
    CreatedAt:    time.Now(),
    LastAccessAt: time.Now(),
}); err != nil {
    log.Fatal(err)
}

ent, err := store.Get(ctx, "user:42")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("entry: size=%d rev=%d\n", ent.Meta.SizeBytes, ent.Revision)
```

### Work queue

```go
be := s3backend.NewMemory()
q, err := queue.New(be, "tasks", func(o *queue.Options) {
    o.WorkerID = "worker-1"
    o.Shards = 16
})
if err != nil {
    log.Fatal(err)
}

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
q.StartMaintenance(ctx)

id, existed, err := q.Enqueue(ctx, []byte(`{"task":"send-email"}`), queue.EnqueueOptions{
    IdempotencyKey: "email-123",
})
if err != nil {
    log.Fatal(err)
}
fmt.Println("enqueued", id, "existed=", existed)

job, err := q.Claim(ctx, queue.ClaimOptions{})
if err != nil {
    log.Fatal(err)
}
fmt.Println("claimed", job.ID)

if err := job.Complete(ctx); err != nil {
    log.Fatal(err)
}
```

## Consistency model in Five Bullets

1. **Per-key linearizability.** Every successful `Create`, `CompareAndSwap`,
   `Update`, or `Delete` on a single key is atomic and immediately visible to
   all readers. This is the strongest guarantee the library provides.
2. **Cross-key invariants are best-effort with repair.** No component relies on
   multi-key transactions. Marker objects, secondary indexes, and eviction
   bookkeeping are repaired by background loops.
3. **Deletion is two-phase.** Reusable keys are tombstoned via CAS first;
   physical deletion happens only after retention, and only when resurrection
   is impossible or disabled.
4. **Wall clocks are heuristic only.** Lease expiry, visibility timeouts, and
   GC use clocks with explicit skew margins; mutual exclusion and fencing are
   derived from monotonic CAS revisions.
5. **Ordering is configurable per collection.** Choose sharded mode for
   throughput and sequencer mode for strict global order; never assume global
   order in sharded mode.

## Documentation

* [docs/consistency.md](docs/consistency.md) — precise per-component guarantees
* [docs/failure-modes.md](docs/failure-modes.md) — crash windows and data-loss boundaries
* [docs/scalability.md](docs/scalability.md) — throughput ceilings, costs, and when not to use the library
* [docs/observability.md](docs/observability.md) — metrics, logging, and adapter sketches
* [docs/ordering.md](docs/ordering.md) — strict order vs. sharded order
* [docs/operations.md](docs/operations.md) — running background loops, retention knobs, recovery runbook
* [docs/design/](docs/design/) — implementation design docs (developer reference)

## Testing

Run the unit tests for each package. Tests use the in-memory backend by
default and require no network access.

```bash
go test ./cas/ -race -count=1
go test ./lru/ -race -count=1
go test ./queue/ -race -count=1
go test ./s3backend/ -race -count=1
```

### S3 integration tests

Build-tagged integration tests exercise the SigV4 HTTP backend against a real
S3-compatible endpoint. Set these environment variables and run with the
`s3integration` tag:

```bash
export S3_ENDPOINT="https://s3.us-east-1.amazonaws.com"
export S3_REGION="us-east-1"
export S3_BUCKET="my-test-bucket"
export S3_ACCESS_KEY="AKIA..."
export S3_SECRET_KEY="..."
export S3_FORCE_PATH_STYLE="false"   # true for MinIO

go test -tags s3integration ./s3backend/ -race -count=1
```

Each run writes objects under a unique timestamped prefix and cleans
append-only objects it creates.

## Production Checklist

* **Start background loops on every replica.** For each `lru.Store` call
  `StartEvictor(ctx)`; for each `queue.Queue` call `StartMaintenance(ctx)`.
  The loops are idempotent and safe to run concurrently.
* **Wire a real Meter.** Use `s3collections.Meter` with your Prometheus,
  OTel, or statsd adapter. Alert on `s3collections_conflicts_total` spikes,
  rising `s3collections_queue_depth`, and sudden drops in
  `s3collections_reaper_deleted_total`.
* **Size retention for your recovery window.** `TombstoneMinAge` (LRU default
  24h), `CompletedRetention` (queue default 24h), `DeadRetention` (queue
  default 7d), and the underlying CAS `TombstoneRetention` (default 5m +
  `ClockSkewHint` 2m) must be longer than the longest repair/replay window
  you care about.
* **Plan for LIST costs.** `Len()`, evictor scans, and queue reaper/GC all
  issue LIST calls. Tune `ShardCount`, page sizes, and intervals to match
  your S3 budget.
* **Do not use the library when** a single key is hot at >~1 successful write
  per RTT, when you need strict cross-key transactions, or when payloads are
  large (queue payloads are capped at 256 KiB by default).

## Examples

Runnable examples using the in-memory backend are in `examples/`:

```bash
go run ./examples/casdemo
go run ./examples/lrudemo
go run ./examples/queuedemo
```

## License

License TBD.
