# API and operations reference

## Storage interfaces

All collections use the interfaces in `storage`:

```go
type Engine struct {
    Metadata storage.KV
    Blobs    storage.BlobStore
}
```

Create one with `storage.NewEngine(kv, blobs)`.

`KV.Transaction` is serializable and commits its callback atomically. `Scan`
uses lexicographic keys and supports prefix, start-after, limit, and reverse
options. Values returned by an implementation must be treated as copies.
`BlobStore` provides streaming `Put`, `Open`, range reads, `Stat`, and `Delete`.
Closing an engine closes its KV resources; close it once during shutdown.

The production layout deliberately has two parts:

- SlateDB Go v0.15 stores small metadata and transaction state on S3.
- An AWS SDK for Go v2 S3 BlobStore stores queue payloads and tree blobs.

The S3 object-store URL contains only the bucket (`s3://bucket`). The SlateDB
DB path is a separate value (`tenant/service/metadata`). SlateDB resolves that
URL through Rust `object_store` and its environment options. The body store is
a separate AWS SDK v2 client configured through `S3Config` and the normal Go
SDK credential chain. Configure and test region, endpoint, path style, and
credentials for both clients; Go SDK configuration is not automatically shared
with SlateDB. Never put secrets in the URL.

Consult the exported `storage` constructor and option types for exact names;
they are the compatibility boundary. Do not use the removed `s3backend`
constructors.

## SlateDB concurrency

A database path has exactly one writer process. Multiple goroutines and
collections may share that process and engine. Multiple processes must not
write the same path. SlateDB may fence an older writer when another writer
opens it. Treat that error as terminal for the old engine. Do not retry through
it. Coordinate ownership outside this library and close the old writer before
failover. Readers must follow SlateDB's v0.15 consistency semantics.

Native SlateDB support requires cgo, the `slatedb` build tag, and the pinned
v0.15 Rust library. See [native.md](native.md).

## Constructors

Pass an engine as the KV. Pass its blob store explicitly where bodies exist:

```go
kv, err := storage.OpenSlateDB(storage.SlateDBConfig{
    ObjectStoreURL: "s3://my-bucket",
    Path:           "service/metadata",
})
if err != nil { return err }
blobs, err := storage.NewS3BlobStore(ctx, storage.S3Config{
    Bucket: "my-bucket",
    Prefix: "service/bodies/",
    Region: "us-east-1",
})
if err != nil { return err }
eng, err := storage.NewEngine(kv, blobs)
if err != nil { return err }
defer eng.Close()

c := cas.New(eng.Metadata, "settings/")
cache, err := lru.New(eng.Metadata, lru.Options{Prefix: "cache/"})
q := queue.New(eng.Metadata, eng.Blobs, "jobs")
t := tree.New(eng)
```

CAS `New` returns only a store. LRU `New` returns a store and error. Queue
`New` returns only a queue. Tree accepts the complete engine. Exact
options and method signatures are documented by Go (`go doc ./cas`, etc.).
Collection names scope keys and must not be reused for unrelated data.

## CAS

CAS stores compact values, revisions, and tombstones in KV. Values default to a
512 KiB maximum and WithMaxValueBytes can set a stricter limit; large bodies
belong in BlobStore. Create, update, compare-and-swap, and delete are single
serializable transactions. A revision
conflict means the caller must read and decide again. There are no atomic
transactions across separate public collection calls.

## LRU

LRU is metadata for bodies cached elsewhere. Its capacity target and recency
policy are approximate. Background eviction can lag. Do not use it as the
only ownership record for an external body.

## Queue

A queue job envelope and lease are KV metadata. **Every payload body is a
separate BlobStore object**, including small payloads. Metadata records its
size and SHA-256; OpenPayload verifies both at EOF. Enqueue uploads the body
before publishing the job transaction. A worker claim is a lease; expiration
allows redelivery. Delivery is at least once, so completion and side effects
must be idempotent.

A crash between upload and metadata publication leaves an orphan, not a visible
partial job. StartMaintenance requeues expired leases. The current queue API
does not enumerate BlobStore objects, so deployments must remove unpublished
uploads with a separate application-owned inventory/lifecycle process after a
grace period. Never choose a grace period shorter than the longest
upload/publication delay.

## Tree

Tree blob bytes are separate BlobStore objects. KV holds immutable manifests,
nodes, refs, leases, and GC state. Blob upload completes before its manifest is
published. Named refs and leases define durable reachability roots. Node
publication and ref advancement are separate operations.

PlanGC marks current ref reachability and SweepGC deletes the returned objects.
Use SweepGCFenced when writer ownership can change: it checks the lease token
inside the metadata transaction. Blob deletion follows that transaction and
cannot be atomic with it, so callers must serialize ref publication through
the same lease and retry partial sweeps. Apply any required age/grace policy
before invoking a sweep. Full reads verify content hashes; range reads verify
length only.

## S3 operations

Grant the writer list/get/put/delete permissions for both the SlateDB path and
body prefixes. Add an S3 lifecycle rule to abort incomplete multipart uploads.
Do not add a generic expiry rule for body objects: queue/tree reachability and
retention, not object age alone, determine whether a completed body is live.
Observe fencing, transaction failures, orphan counts, maintenance lag, and GC
sweep failures.
