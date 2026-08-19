# API reference

This is the practical user-facing reference for `s3collections`. For runnable
programs, see [`examples/`](../examples/).

## Backend

Every data structure accepts an `s3backend.Backend`:

```go
type Backend interface {
	Get(ctx context.Context, key string) (*Object, error)
	Put(ctx context.Context, key string, body []byte, pre *Preconditions) (etag string, err error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, opts *ListOptions) (*ListPage, error)
}
```

The associated request and response types are:

```go
type Object struct {
	Key     string
	Body    []byte
	ETag    string
	ModTime time.Time
}

type Preconditions struct {
	IfNoneMatch bool
	IfMatchETag string
}

type ListOptions struct {
	StartAfter        string
	ContinuationToken string
	MaxKeys           int
}
```

`ListPage` contains `Objects`, `IsTruncated`, and the opaque
`NextContinuationToken`. A backend should return errors wrapping
`ErrNotFound` and `ErrPreconditionFailed` so callers can use `errors.Is`.

A backend must provide strong read-after-write `Get` and `List`, atomic
per-key writes, atomic `If-None-Match` and `If-Match` writes, and paginated
lexicographic prefix listing. These properties are part of the correctness
contract, not performance hints.

### HTTP backend

```go
client, err := s3backend.NewHTTPClient(s3backend.HTTPConfig{
	Endpoint:     "https://s3.us-west-2.amazonaws.com",
	Region:       "us-west-2",
	Bucket:       "my-bucket",
	AccessKey:    "...",
	SecretKey:    "...",
	SessionToken: "...", // optional
	PathStyle:    false,
	Prefix:       "my-service/",
	HTTPClient:   nil,
})
```

`Endpoint`, `Region`, `Bucket`, `AccessKey`, and `SecretKey` are required.
`Prefix` is prepended to stored keys and removed from list results. Set
`PathStyle` for endpoints that use `/bucket/key` instead of
`bucket.endpoint/key`.

The client signs requests with SigV4 and supports `GetObject`, ranged and
streaming reads, `HeadObject`, `PutObject`, standard multipart upload,
`DeleteObject`, and `ListObjectsV2`. It does not use provider-specific append
or write-offset extensions. It does not perform retries; higher-level
packages retry errors for which `s3backend.IsRetryable(err)` is true. It uses
static credentials and does not discover or refresh them.

`s3backend.ErrNotFound` represents a missing object.
`s3backend.ErrPreconditionFailed` represents a failed conditional write.
Other service and transport failures use `*s3backend.Error`.

### Test backends

```go
memory := s3backend.NewMemory()

chaos := s3backend.NewChaos(memory, s3backend.ChaosConfig{
	ErrorRate:          0.05,
	AmbiguousWriteRate: 0.01,
	DelayRate:          0.10,
	Delay:              100 * time.Millisecond,
})
```

`Memory` implements the backend contract in process. `Chaos` wraps another
backend and injects retryable failures, writes that succeed but report an
error, and latency.

## Immutable blob and tree store

```go
store, err := tree.New(backend, "snapshots", options...)
```

A store name scopes every key. Names and ref names are base64url-encoded before
being used in object keys. Defaults allow blob payloads from 40 KiB through
500 MiB.

### Blobs

```go
ref, err := store.PutBlob(ctx, expectedSHA256, reader,
    tree.WithExpectedBlobSize(size),
    tree.WithEncodingDescriptor(encodingMetadata),
    tree.WithEncryptionDescriptor(encryptionMetadata),
    tree.WithBlobMetadata(opaqueMetadata))

reader, err := store.GetBlob(ctx, ref, tree.WithRange(offset, length))
stat, err := store.StatBlob(ctx, ref)
```

Blob identities are lowercase SHA-256 hex over the exact stored stream. Raw
bytes and their immutable metadata manifest are separate objects; the manifest
is published last. Retrying identical content is idempotent. Encoding,
compression, and encryption descriptors are opaque bytes—this package does not
compress, encrypt, or manage keys.

Full reads verify byte count and SHA-256 when read to EOF. Range reads use one
`Range` GET after validating the supplied `BlobRef`; they validate response
bounds and length, but not the full-blob hash. Large HTTP writes use the
portable S3 multipart protocol. Configure an AbortIncompleteMultipartUpload
lifecycle rule as defense in depth for uploads abandoned before an upload ID
can be recovered.

### Immutable nodes and refs

```go
root, err := store.CommitRoot(ctx, payloadRefs, opaqueMetadata)
child, err := store.CommitChild(ctx, root, payloadRefs, opaqueMetadata)
node, err := store.GetNode(ctx, child)

head, err := store.CreateRef(ctx, "branches/main", root)
head, err = store.CompareAndSwapRef(ctx, head.Name, head.Revision, child)
err = store.DeleteRef(ctx, head.Name, head.Revision)
```

Node IDs hash canonical manifests. A root encodes both parent and root as JSON
`null`; `Root(rootID)` returns `rootID`. Child commit verifies its parent
lineage and every referenced blob before publishing the node. Committing a node
and advancing a named ref are separate operations. Ref deletion is a
revisioned tombstone because portable S3 has no conditional delete.

### Topology

`ResolveLineage` returns root/boundary through target. Its optional stop
predicate lets callers interpret opaque metadata without embedding VM semantics
in this package. `Root`, `IsAncestor`, and `LowestCommonAncestor` use only
parent pointers. `ListChildren` uses advisory reverse-edge objects; call
`RepairEdges` to reconcile missing hints. Restore and GC never depend on them.

### Leases and GC

`AcquireLease`, `RenewLease`, and `ReleaseLease` use owner/token/fence data and
conditional writes. Stale lease copies cannot renew or release newer fences.

`PlanGC` marks from all live refs, active leases, caller-provided retained
roots, recent commits, and recent blob publications. It returns a persisted
plan of versioned candidates older than the cutoff. After `NotBefore`,
`SweepGC` takes the store mutation gate, refreshes reachability, validates
candidate versions/ages, and deletes unreachable nodes, blobs, and stale edge
hints. There is no public `DeleteNode`. A failed process can leave the mutation
gate held; operators can inspect `MutationGate` and, after establishing
external exclusivity, use `RecoverMutationGate` with the expected fence.

## Compare-and-swap store

Create a store with a backend and an object-key prefix:

```go
store, err := cas.New(backend, "config/", options...)
```

### Methods

```go
Create(ctx, key, value, writeOptions...) (cas.Record, error)
Get(ctx, key) (cas.Record, error)
GetMeta(ctx, key) (cas.Record, error)
CompareAndSwap(ctx, key, expectedRevision, value, writeOptions...) (cas.Record, error)
Update(ctx, key, updateFn, writeOptions...) (cas.Record, error)
Delete(ctx, key, expectedRevision, writeOptions...) (cas.Record, error)
List(ctx, options) (*cas.ListPage, error)
GC(ctx, options) (deleted int, err error)
```

`Create` fails with `cas.ErrAlreadyExists` when either a live record or a
tombstone already occupies the key. `CompareAndSwap` and `Delete` fail with
`cas.ErrConflict` when the supplied revision is stale.

`Update` reads the current record, calls the supplied function, and attempts a
conditional write. The function may run more than once during contention and
must not have side effects. Returning `nil` or the current value is a no-op.

`Get` returns only live values. `GetMeta` also exposes tombstone metadata.
`List` includes both live records and tombstones.

```go
type ListOptions struct {
	Prefix            string
	StartAfter        string
	ContinuationToken string
	MaxKeys           int
}

type GCOptions struct {
	Prefix     string
	OlderThan  time.Time
	MaxDeletes int
}
```

`Delete` writes a tombstone rather than immediately deleting the object. `GC`
physically removes old tombstones after `TombstoneRetention` plus the
`ClockSkewHint` safety margin. The defaults make a tombstone eligible after
roughly seven minutes.

### Record

```go
type Record struct {
	Key       string
	Value     []byte
	Revision  uint64
	State     cas.State // cas.Live or cas.Tombstone
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time
	WriterID  string
	ETag      string
}
```

Revisions increase monotonically for one key and are not comparable across
keys.

### Store options

| Option | Default | Meaning |
| --- | --- | --- |
| `WithWriterID` | `"cas"` | Identity stored in record envelopes. |
| `WithMaxValueBytes` | 256 KiB | Maximum value size before envelope overhead. |
| `WithRetry` | Shared retry default | Retry policy for store operations. |
| `WithTombstoneRetention` | 5 minutes | Time before a tombstone may be collected. |
| `WithClockSkewHint` | 2 minutes | Extra age required before collection. |
| `WithKeyCodec` | Path-segment codec | Application-key encoding. |
| `WithMeter`, `WithLogger`, `WithTracer` | No-op | Observability adapters. |

Write options apply to one mutation:

- `WithRetryPolicy` overrides the retry policy.
- `WithIncludeTombstone` lets an update function inspect a tombstone.
- `WithResurrect` lets `Update` replace a tombstone with a live value. Avoid
  physical GC for that key family, or use retention much longer than an
  update, because resurrection can race with deletion.

Errors are matched with `errors.Is`: `ErrAlreadyExists`, `ErrNotFound`,
`ErrDeleted`, `ErrConflict`, `ErrTooLarge`, and `ErrCorrupt`.

## LRU metadata store

The LRU stores metadata about cached objects, not the objects themselves. Keys
are hashed across shards, and background workers use a two-pass CLOCK scan to
approximate recency without maintaining a global index.

```go
store, err := lru.New(backend, lru.Options{...})
```

### Methods

```go
Set(ctx, key, metadata) error
Get(ctx, key) (lru.Entry, error)
Touch(ctx, key) error
Delete(ctx, key) error
Len(ctx) (int64, error)
Stats(ctx) (lru.Stats, error)
StartEvictor(ctx) error
Close() error
```

`Set` creates, updates, or resurrects an entry and marks it recently used.
`Touch` updates recency according to `TouchPolicy`. `Delete` writes a
tombstone. `Len` and `Stats` scan stored entries and should not be placed on a
latency-sensitive path.

`StartEvictor` starts background eviction until its context is canceled or
the store is closed. Call it on every replica. Capacity is a soft target and
is divided proportionally among shards, so eviction is not a strict global
LRU.

```go
type EntryMeta struct {
	SizeBytes    int64
	CreatedAt    time.Time
	LastAccessAt time.Time
	AccessCount  uint64
}

type Entry struct {
	Key      string
	Meta     EntryMeta
	Revision uint64
}
```

### Options

| Field | Default | Meaning |
| --- | --- | --- |
| `Prefix` | `"lru/"` | Unique object-key prefix for the store. |
| `ShardCount` | 128 | Number of independently scanned key ranges. |
| `CapacityBytes` | Disabled | Soft live-byte limit. |
| `CapacityItems` | Disabled | Soft live-entry limit. |
| `EvictorInterval` | 2 seconds | Base interval between worker passes. |
| `EvictorWorkers` | `min(ShardCount, 4)` | Concurrent shard workers. |
| `EvictorBatchSize` | 512 | Maximum entries processed per shard per pass. |
| `TouchOnGet` | `false` | Call `Touch` after a successful `Get`. |
| `TouchPolicy.CoalesceWindow` | 1 second | Minimum interval between touch writes for a key. |
| `ListPageSize` | 1000 | Maximum records per list page. |
| `TombstoneMinAge` | 24 hours | Minimum age before physical deletion; negative disables GC. |
| `WriterID` | `"lru"` | Identity stored in CAS envelopes. |
| `Retry` | Shared retry default | Retry policy. |
| `Meter`, `Logger`, `Tracer` | No-op | Observability adapters. |

`lru.ErrNotFound` means the key is absent or tombstoned. `lru.ErrClosed`
means the store has been closed.

## Work queue

Create a named queue with a backend:

```go
q, err := queue.New(backend, "email", options...)
```

Queue state is stored under `queue/<name>/`. Use a unique name for each
logical queue.

### Enqueue and claim

```go
jobID, existed, err := q.Enqueue(ctx, payload, queue.EnqueueOptions{
	IdempotencyKey: "welcome:user-42",
	Delay:          time.Minute,
	Shard:          nil, // hash the job ID; set a shard explicitly if needed
})

job, err := q.Claim(ctx, queue.ClaimOptions{
	VisibilityTimeout: 30 * time.Second,
	RestrictToShards:  nil,
})
```

An idempotency key produces a deterministic job ID. Repeating the enqueue
returns `existed=true` while the canonical job object, including its retained
tombstone, still exists.

`Claim` returns `queue.ErrEmpty` when it finds no visible job in the shards it
probes. A claim creates a lease. Other workers cannot claim that job until it
is completed, retried, dead-lettered, or its lease expires.

### Claimed jobs

```go
type Job struct {
	ID        string
	Queue     string
	Shard     uint16
	Payload   []byte
	Attempts  int
	Fence     uint64
	Lease     queue.Lease
	NotBefore time.Time
	CreatedAt time.Time
}

job.Renew(ctx, extension)
job.Complete(ctx)
job.Retry(ctx, queue.RetryOptions{Backoff: time.Minute, Reason: "temporary failure"})
job.Dead(ctx, "permanent failure")
```

`Renew` extends a lease. `Complete`, `Retry`, and `Dead` require the caller to
still own the current lease. A stale worker receives `ErrStaleLease` or
`ErrNotLeased`.

Delivery is at least once: a worker can continue running after its lease
expires while another worker reclaims the job. Handlers must be idempotent.
`Job.Fence` is the job's CAS revision; applications that persist the highest
seen revision with downstream state can use `queue.Guard` to reject stale
effects.

### Maintenance and dead letters

```go
q.StartMaintenance(ctx)

items, cursor, err := q.ListDead(ctx, queue.ListDeadOptions{
	StartAfter: cursor,
	Limit:      100,
})

err = q.RequeueDead(ctx, jobID, shard)
```

Run `StartMaintenance` on every replica. It reclaims expired leases, repairs
missing marker objects, records approximate queue depth, and garbage-collects
old completed and dead jobs. Repeated calls on the same queue are harmless.

`ListDead` orders items by dead-letter time. Pass its opaque cursor back as
`StartAfter`; pagination is finished when it returns an empty page and an
empty cursor.

### Queue options

| Option | Default | Meaning |
| --- | --- | --- |
| `WithShards` | 256 | Spreads claim and maintenance scans. |
| `WithWorkerID` | Random ID | Lease owner identity. |
| `WithDefaultVisibilityTimeout` | 30 seconds | Lease duration when `ClaimOptions` does not override it. |
| `WithClockSkewTolerance` | 2 seconds | Margin used for lease and schedule checks. |
| `WithClaimPageSize` | 128 | Ready markers per list page. |
| `WithClaimMaxPages` | 4 | Maximum pages scanned per shard by one claim. |
| `WithClaimShardProbe` | `min(Shards, 8)` | Shards examined by one claim. |
| `WithReaperInterval` | 5 seconds | Base lease-reaper interval. |
| `WithGCInterval` | 5 minutes | Base garbage-collection interval. |
| `WithCompletedRetention` | 24 hours | Time completed jobs are kept before tombstoning; preserves deduplication. |
| `WithDeadRetention` | 7 days | Time dead jobs remain available for inspection or replay. |
| `WithMaxAttempts` | Unlimited | Delivery limit before `Retry` dead-letters a job. |
| `WithReasonHistory` | 8 | Retry/dead reasons retained per job. |
| `WithSequencerEnabled` | `false` | Use one CAS sequencer and one shard for strict global order. |
| `WithMaxPayloadBytes` | 256 KiB | Maximum enqueue payload. |
| `WithRetry` | Shared retry default | Retry policy. |
| `WithMeter`, `WithLogger`, `WithTracer` | No-op | Observability adapters. |

`ErrEmpty`, `ErrStaleLease`, `ErrNotLeased`, and `ErrFenceStale` are matched
with `errors.Is`.

## Retries and observability

The root package defines the shared retry policy:

```go
type RetryPolicy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
	Jitter      float64
}
```

The default is eight total attempts, a 50 ms base delay, a 2 second cap, and
full jitter. `DefaultRetry`, `RetryPolicy.WithDefaults`, and `BackoffDelays`
are available to applications and custom backends.

`Meter`, `Logger`, and `Tracer` are small interfaces implemented by the
application. Nil adapters are replaced with no-ops. `NewCaptureMeter` returns
an in-memory meter for tests.

Metric names are stable:

- `s3collections_latency_seconds`
- `s3collections_cas_attempts`
- `s3collections_conflicts_total`
- `s3collections_retries_total`
- `s3collections_list_pages_total`
- `s3collections_corrupt_total`
- `s3collections_reaper_runs_total`
- `s3collections_reaper_deleted_total`
- `s3collections_lru_entries`
- `s3collections_lru_evictions_total`
- `s3collections_lru_bytes`
- `s3collections_lru_touch_writes_total`
- `s3collections_lru_touch_skips_total`
- `s3collections_queue_events_total`
- `s3collections_queue_depth`

Do not put user keys or job IDs in metric labels.

## Operational notes

- S3 latency is part of every operation. This library is not a low-latency
  replacement for an in-memory database.
- `List`, LRU eviction, queue claims, maintenance, and GC consume list
  requests. More shards reduce each scan but increase the number of prefixes
  visited.
- There are no transactions across keys. Queue markers and other secondary
  state are repaired asynchronously from canonical CAS records.
- Retention controls recovery and deduplication windows. Keep tombstones,
  completed jobs, and dead jobs longer than the longest retry or replay window
  that matters to the application.
- A single hot key is limited by object-store round-trip time and conditional
  write contention.
