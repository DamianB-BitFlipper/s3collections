# s3collections

`s3collections` provides durable CAS records, cache metadata, work queues, and
immutable trees. Small transactional metadata is stored in
[SlateDB](https://slatedb.io/) **Go v0.15** on S3. Large bodies use a separate
`storage.BlobStore`; the supplied S3 implementation uses AWS SDK for Go v2.

## Storage model

Applications compose a `storage.Engine` from SlateDB metadata and an S3
blob store:

```go
kv, err := storage.OpenSlateDB(storage.SlateDBConfig{
	ObjectStoreURL: "s3://my-bucket", // bucket only
	Path:           "production/metadata",
})
if err != nil { log.Fatal(err) }
blobs, err := storage.NewS3BlobStore(ctx, storage.S3Config{
	Bucket: "my-bucket",
	Prefix: "production/bodies/",
	Region: "us-east-1",
})
if err != nil { log.Fatal(err) }
eng, err := storage.NewEngine(kv, blobs)
if err != nil { log.Fatal(err) }
defer eng.Close()

records := cas.New(eng.Metadata, "accounts/")
cache, err := lru.New(eng.Metadata, lru.Options{Prefix: "cache/"})
q := queue.New(eng.Metadata, eng.Blobs, "email")
t := tree.New(eng)
```

Constructors take the transactional KV and, for collections with bodies, an
explicit blob store or engine. Queue payload bodies and tree blob bodies never live in
SlateDB. This keeps transactions small and permits streaming large objects.
Use a different collection name for each logical collection.

See [the API and operations reference](docs/reference.md) for the actual engine
constructor and configuration once selected for your deployment.

## Important deployment rules

- **One SlateDB writer process per database path.** Do not open the same path
  for writing in several processes. SlateDB fencing detects a superseded
  writer, but fencing is a safety stop, not multi-writer coordination. Stop
  the fenced process and reopen deliberately. Several collections in one
  process can share one engine.
- The object-store URL identifies the **bucket only** (for example,
  `s3://my-bucket`). Put the SlateDB namespace in the separate database-path
  option (for example, `production/metadata`). Do not append that path to the
  bucket URL.
- The SlateDB Go binding is native code through cgo. Normal portable builds
  can use the non-native engine(s). Build the SlateDB implementation with the
  repository's `slatedb` build tag, cgo enabled, and the matching pinned v0.15
  Rust library. See [native builds](docs/native.md).
- Metadata commit and body upload cannot be one transaction. Uploads happen
  first and metadata publishes them last. A crash can therefore leave an
  unreferenced body. It must never leave committed metadata pointing at a
  partially uploaded body. An application GC pass must remove unpublished bodies after a grace period;
  configure an S3 lifecycle rule to abort incomplete multipart uploads and to
  provide defense in depth.

## Packages

| Package | Purpose |
| --- | --- |
| `storage` | `Engine`, serializable KV transactions, streaming blobs, engines |
| `cas` | versioned records and compare-and-swap |
| `lru` | bounded cache metadata and approximate eviction |
| `queue` | leased, at-least-once work with separate payload bodies |
| `tree` | content-addressed bodies, immutable nodes, refs, and GC |

Queue delivery is at least once. Handlers must be idempotent. Tree nodes and
queue job envelopes are transactional metadata; every queue/tree body is a
separate blob object. Never infer body liveness from an S3 listing alone.

## Install and test

```sh
go get github.com/damianb/s3collections
go test -race ./...
```

Go 1.26 or later is required. Runnable programs are in [`examples/`](examples/).
The default CI path does not require Rust, cgo, AWS credentials, or a bucket.
