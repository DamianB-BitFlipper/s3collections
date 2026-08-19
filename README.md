# s3collections

`s3collections` implements immutable blob/tree primitives, a compare-and-swap
store, an LRU metadata index, and an at-least-once work queue on top of S3-compatible object storage. It is
for stateless Go services that already have a bucket and can accept
object-storage latency in exchange for not operating another database.

The module uses only the Go standard library. Its built-in client implements
the small part of the S3 API the data structures need and signs requests with
AWS Signature Version 4.

## Packages

| Package | Use it for |
| --- | --- |
| [`tree`](tree/) | Content-addressed blobs and immutable tree nodes, named refs, traversal, leases, and reachability GC. |
| [`cas`](cas/) | Versioned records with atomic create, update, and delete per key. |
| [`lru`](lru/) | Distributed metadata for a bounded cache, with sharded CLOCK eviction. The cached objects themselves live elsewhere. |
| [`queue`](queue/) | Durable, at-least-once jobs with leases, retries, dead letters, and idempotent enqueue. |
| [`s3backend`](s3backend/) | The storage interface, a SigV4 HTTP client, an in-memory backend, and a fault-injection backend. |

## Install

```sh
go get github.com/damianb/s3collections
```

Go 1.26 or later is required.

## Connect to object storage

The HTTP backend works with AWS S3 and S3-compatible services that provide the
consistency and conditional-write behavior described in the
[`API reference`](docs/reference.md).

```go
be, err := s3backend.NewHTTPClient(s3backend.HTTPConfig{
	Endpoint:     os.Getenv("S3_ENDPOINT"),
	Region:       os.Getenv("S3_REGION"),
	Bucket:       os.Getenv("S3_BUCKET"),
	AccessKey:    os.Getenv("S3_ACCESS_KEY"),
	SecretKey:    os.Getenv("S3_SECRET_KEY"),
	SessionToken: os.Getenv("S3_SESSION_TOKEN"), // optional
	PathStyle:    os.Getenv("S3_FORCE_PATH_STYLE") == "true",
	Prefix:       "my-service/",
})
if err != nil {
	log.Fatal(err)
}
```

Typical endpoint settings are:

| Service | Endpoint | Region | Path style |
| --- | --- | --- | --- |
| AWS S3 | `https://s3.<region>.amazonaws.com` | The bucket's region | `false` |
| Cloudflare R2 | `https://<account-id>.r2.cloudflarestorage.com` | `auto` | `false` |
| MinIO | For example, `http://localhost:9000` | Usually `us-east-1` | `true` |

The built-in client accepts static SigV4 credentials and an optional session
token. It does not discover credentials from AWS profiles, instance roles, or
container metadata, and it does not refresh expiring credentials.

For tests, use the in-memory backend:

```go
be := s3backend.NewMemory()
```

The examples below assume `be` is configured and `ctx` is a live
`context.Context`.

## Immutable blobs and trees

`tree.Store` provides storage primitives rather than VM-specific snapshot
semantics. Blob bytes are content-addressed by SHA-256. Nodes contain parent
pointers, blob references, and opaque metadata; a branch-factor-one tree is a
list. Named refs are mutable branch/tag heads updated separately from commits.

```go
store, err := tree.New(be, "snapshots")
if err != nil { log.Fatal(err) }

sum := sha256.Sum256(payload)
blob, err := store.PutBlob(ctx, tree.BlobID(hex.EncodeToString(sum[:])),
    bytes.NewReader(payload), tree.WithExpectedBlobSize(int64(len(payload))))
if err != nil { log.Fatal(err) }

root, err := store.CommitRoot(ctx, []tree.BlobRef{blob}, opaqueMetadata)
if err != nil { log.Fatal(err) }
head, err := store.CreateRef(ctx, "branches/main", root)
if err != nil { log.Fatal(err) }

child, err := store.CommitChild(ctx, root, nil, nextOpaqueMetadata)
if err != nil { log.Fatal(err) }
_, err = store.CompareAndSwapRef(ctx, head.Name, head.Revision, child)
```

`ResolveLineage` follows authoritative parent pointers from a node to its root.
`ListChildren` uses advisory reverse-edge markers and is eventually consistent.
Refs and active leases are the durable roots for reachability GC. Run
`PlanGC`, wait its grace period, then pass the unchanged plan to `SweepGC`.
There is deliberately no public node-delete operation.

Full blob reads verify SHA-256 at EOF. Range reads validate the returned byte
count but cannot validate the full-object hash without downloading the full
object. Encoding, compression, and encryption descriptors are opaque; callers
perform transformations and key management.

## CAS

`cas.Store` keeps a monotonically increasing revision with each value. A write
only succeeds when the revision supplied by the caller is still current.

```go
store, err := cas.New(be, "config/",
	cas.WithWriterID("api-1"),
	cas.WithTombstoneRetention(10*time.Minute),
)
if err != nil {
	log.Fatal(err)
}

rec, err := store.Create(ctx, "limits", []byte(`{"requests":100}`))
if err != nil {
	log.Fatal(err)
}

rec, err = store.CompareAndSwap(
	ctx,
	"limits",
	rec.Revision,
	[]byte(`{"requests":200}`),
)
if errors.Is(err, cas.ErrConflict) {
	// Another replica changed the record. Read it again and retry.
}
if err != nil {
	log.Fatal(err)
}
```

`Create`, `CompareAndSwap`, `Update`, and `Delete` are linearizable for one
key. There are no transactions across keys.

## LRU metadata

The LRU stores the size and access metadata for objects cached elsewhere. It
hashes keys across shards so eviction workers can scan in parallel without a
single global recency index.

```go
store, err := lru.New(be, lru.Options{
	Prefix:          "cache-index/",
	ShardCount:      128,
	CapacityBytes:   10 << 30, // 10 GiB
	CapacityItems:   100_000,
	TouchOnGet:      true,
	EvictorWorkers:  4,
	TombstoneMinAge: 24 * time.Hour,
})
if err != nil {
	log.Fatal(err)
}

evictCtx, stopEvictor := context.WithCancel(ctx)
defer stopEvictor()
if err := store.StartEvictor(evictCtx); err != nil {
	log.Fatal(err)
}

now := time.Now()
err = store.Set(ctx, "avatars/user-42", lru.EntryMeta{
	SizeBytes:    4096,
	CreatedAt:    now,
	LastAccessAt: now,
})
if err != nil {
	log.Fatal(err)
}

entry, err := store.Get(ctx, "avatars/user-42")
if err != nil {
	log.Fatal(err)
}
fmt.Printf("size=%d revision=%d\n", entry.Meta.SizeBytes, entry.Revision)
```

Capacity is a soft target enforced in the background. Eviction is approximate
and happens independently per shard; this is not a strict global LRU.

## Queue

Claims are leases. While a worker holds a lease the job is hidden from other
workers; if the lease expires, maintenance makes the job claimable again.

```go
q, err := queue.New(be, "email",
	queue.WithWorkerID("worker-1"),
	queue.WithShards(256),
	queue.WithDefaultVisibilityTimeout(30*time.Second),
	queue.WithMaxAttempts(5),
)
if err != nil {
	log.Fatal(err)
}

maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
defer stopMaintenance()
q.StartMaintenance(maintenanceCtx)

jobID, existed, err := q.Enqueue(ctx, []byte(`{"template":"welcome"}`),
	queue.EnqueueOptions{IdempotencyKey: "welcome:user-42"},
)
if err != nil {
	log.Fatal(err)
}
fmt.Printf("job=%s already-existed=%v\n", jobID, existed)

job, err := q.Claim(ctx, queue.ClaimOptions{})
if errors.Is(err, queue.ErrEmpty) {
	return
}
if err != nil {
	log.Fatal(err)
}

workErr := sendEmail(job.Payload)
if workErr != nil {
	err = job.Retry(ctx, queue.RetryOptions{
		Backoff: time.Minute,
		Reason:  workErr.Error(),
	})
} else {
	err = job.Complete(ctx)
}
if err != nil {
	log.Fatal(err)
}
```

Run `StartMaintenance` on every replica. Long-running jobs should call
`job.Renew` before their visibility timeout expires. Delivery is at least
once, so handlers must tolerate duplicate execution.

## Tradeoffs

This library is a reasonable fit when the state is modest, S3 is already part
of the system, and an S3 round trip per operation is acceptable. It is usually
the wrong fit when you need:

- sub-millisecond access;
- a frequently written single key;
- transactions or invariants across several keys;
- strict global queue order at high throughput;
- large queue payloads (the default limit is 256 KiB).

`List` calls drive much of the cost. Queue claims and maintenance, LRU scans,
and garbage collection all list prefixes, so shard counts and scan intervals
should be chosen with the expected object count and provider pricing in mind.

## Testing

Unit and concurrency tests use the in-memory and fault-injection backends and
do not require network access:

```sh
go test -race ./...
```

The integration test exercises the backend contract against a real endpoint:

```sh
export S3_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
export S3_REGION="auto"
export S3_BUCKET="my-test-bucket"
export S3_ACCESS_KEY="..."
export S3_SECRET_KEY="..."
export S3_FORCE_PATH_STYLE="false"

go test -tags s3integration -race ./s3backend
```

The test writes below a unique prefix and removes the objects it creates.

Runnable examples using the in-memory backend are in [`examples/`](examples/):

```sh
go run ./examples/treedemo
go run ./examples/casdemo
go run ./examples/lrudemo
go run ./examples/queuedemo
```

The [`API reference`](docs/reference.md) lists methods, options, defaults,
errors, consistency requirements, and operational notes.

## License

License TBD.
