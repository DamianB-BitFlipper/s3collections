# Consolidation: Final Implementation Contract (v1)

Status: BINDING for implementation. This document resolves cross-doc conflicts
between 00-architecture.md, 01-cas-store.md, 02-lru-store.md, and
03-work-queue.md. Where this doc and a component doc disagree, THIS doc wins.
Component docs remain the source of truth for everything not mentioned here.

## 0. Module and packages

Module `github.com/damianb/s3collections`, Go 1.26, standard library only.

Packages (dependency order, no cycles):
- `s3backend`  — storage contract + Memory + Chaos (DONE, committed) + HTTP/SigV4 client (to build).
- `s3collections` (root) — shared public plumbing: observability interfaces, retry policy, testing helpers.
- `cas` — versioned CAS store (depends on s3backend + root).
- `lru` — distributed LRU metadata store (depends on s3backend, cas, root).
- `queue` — durable work queue (depends on s3backend, cas, root).

## 1. Resolved conflicts and decisions

### D1. Backend interface: keep the committed s3backend as-is.
The design docs sketched slightly different Backend shapes; the committed
`s3backend.Backend` (Get→*Object, Put→etag with Preconditions, Delete, List
with ListOptions/ListPage) is final. Bucket/endpoint binding happens at
backend construction (e.g. `s3backend.NewHTTPClient`), NOT per call.
Consequence: `cas.New(b s3backend.Backend, prefix string, opts ...Option)` —
NO bucket parameter (overrides 01-cas-store.md §1).

### D2. Root package `s3collections` owns shared plumbing.
Implements the architecture doc §3/§4 with these exact decisions:
- `Meter`, `Logger`, `Tracer`, `Span`, `Label` interfaces + Noop impls.
- `CaptureMeter` — an in-memory Meter for tests (usable by consumers' tests).
- `RetryPolicy{MaxAttempts int; Base, Max time.Duration; Jitter float64}`,
  `DefaultRetry()` = {8, 50ms, 2s, 1.0}, and an unexported backoff iterator
  in `internal/` is NOT used; instead root exports a `Sleeper`-free helper:
  `func BackoffDelays(p RetryPolicy, rnd *rand.Rand) func() time.Duration`.
- NO unified Error-with-Code type. Each package defines idiomatic sentinel
  errors + typed errors (e.g. cas.NotFoundError), matched with errors.Is/As.
  Mapping: s3backend.ErrNotFound → cas.ErrNotFound; s3backend.ErrPreconditionFailed
  on create → cas.ErrAlreadyExists, on update → cas.ErrConflict;
  s3backend.Error{Retryable} → retry.
- Component options accept Meter/Logger/Tracer (nil = noop). The stable metric
  names from 00-architecture.md §4.4 are REQUIRED. Component-specific Hooks
  structs from the component docs (cas.Hooks, lru.Hooks, queue.Hooks) are
  DROPPED in favor of Meter/Logger; tests assert via CaptureMeter. Queue event
  observability: counter `s3collections_queue_events_total{queue,event,outcome}`
  plus `s3collections_latency_seconds{component="queue",op,outcome}`.

### D3. Tombstone resurrection: cas forbids by default; lru opts in via expert option.
The conflict: 01 forbids resurrection (makes GC's unconditional DELETE safe —
correct), but 02's LRU needs Set-after-evict (resurrection).
Resolution:
- cas keeps "no resurrection" as the DEFAULT (Create fails on tombstone with
  ErrAlreadyExists; Update/CompareAndSwap return ErrDeleted on tombstones).
- cas.Update gains an expert per-call option `WithResurrect()`: fn is invoked
  with the tombstone Record and MAY return a live value, producing
  rev=tombRev+1, state=live, created_at preserved, updated_at=now.
  Godoc must state the hazard: physical GC of that key's tombstone then races
  with resurrection; safe ONLY if tombstones of this key family are never
  physically deleted, or deleted with retention >> any single op duration.
- lru uses cas.Update(..., WithResurrect()) for Set on tombstoned entries.
  lru physical tombstone GC: Option `TombstoneMinAge` DEFAULTS TO 24h (not 5m),
  and its godoc + docs must state: a resurrection that completes entirely
  inside the GC worker's verify→DELETE window (~1 RTT) can lose the freshly
  resurrected entry (an early eviction, self-healing on next Set); choose
  retention >> RTT; set TombstoneMinAge<0 to disable physical GC entirely
  (tombstones then accumulate ~150B per distinct key ever evicted).
- queue never resurrects (job IDs are unique per enqueue or idempotency-key
  derived and dedup-through-tombstone is DESIRED behavior). queue therefore
  enjoys fully-safe physical GC via cas tombstones. See D5.

### D4. cas gains List and GC methods (needed by lru evictor and queue reaper).
01-cas-store.md omitted listing; it is REQUIRED. Final additions to cas:
```go
type ListOptions struct {
    Prefix            string // app-level key prefix (NOT escaped by caller; matched pre-escape on encoded segments)
    StartAfter        string // app key to start after (exclusive)
    ContinuationToken string // opaque; takes precedence over StartAfter
    MaxKeys           int
}
type ListPage struct {
    Records               []Record // includes tombstones (State field set)
    IsTruncated           bool
    NextContinuationToken string
}
func (s *Store) List(ctx context.Context, opts *ListOptions) (*ListPage, error)

type GCOptions struct {
    Prefix     string    // restrict sweep to this app-level key prefix
    OlderThan  time.Time // delete tombstones with DeletedAt before this
    MaxDeletes int       // 0 = unlimited
}
// GC physically deletes eligible tombstones. Safe per the no-resurrection
// rule (D3). Returns the number of objects deleted.
func (s *Store) GC(ctx context.Context, opts *GCOptions) (int, error)
```
List maps app-prefix → encoded object prefix, pages the backend, decodes
envelopes. Corrupt envelopes during List: skip the object, count
`s3collections_corrupt_total{component="cas"}`, continue.

### D5. queue deletion path: cas tombstones, never raw backend deletes of jobs.
03-work-queue.md says GC "DELETEs the canonical job object" directly. That
races with re-enqueue of the same idempotency key (unconditional DELETE could
remove a fresh pending job). Resolution: ALL queue job-object removals go
through cas.Store.Delete (conditional tombstone); physical deletion happens
only via cas.Store.GC after retention. Consequences:
- Complete transitions to state=completed (as designed); completed objects are
  first tombstoned by queue GC after CompletedRetention (default 24h), then
  physically removed by cas GC after the cas tombstone retention (default 5m).
- Re-enqueue with the same IdempotencyKey while the job object exists (any
  state, incl. completed, or as a tombstone) returns existed=true. This is
  desired dedup-through-retention and must be documented.
- Dead jobs: pruned after DeadRetention via the same two-phase path.
- Marker objects (ready/lease/dead) are RAW backend objects (zero-byte), NOT
  cas records: they are append-only hint keys, physically deleted on sight,
  reconciled against canonical job state. This is Category A (safe) per
  00-architecture.md §6.2.

### D6. Component constructors take the backend, not a cas.Store.
02/03 sketched `New(store cas.Store, ...)`. Final:
- `func lru.New(b s3backend.Backend, opts lru.Options) (*lru.Store, error)`
  builds an internal cas.Store rooted at Options.Prefix (default "lru/").
  Entry app-keys: "entries/<shard>/<userkey>" (shard = zero-padded decimal,
  width = digits(ShardCount-1); cas KeyCodec escapes each segment).
- `func queue.New(b s3backend.Backend, name string, opts ...queue.Option) (*queue.Queue, error)`
  roots at "queue/<name>/"; canonical job app-keys "shard/<hhhh>/jobs/<jobID>"
  via internal cas.Store; markers as raw objects under
  "queue/<name>/shard/<hhhh>/{ready,lease,dead}/...".
- Pass-through knobs in lru.Options/queue.Options: WriterID, Meter, Logger,
  Tracer, Retry s3collections.RetryPolicy, and (queue) MaxPayloadBytes
  (default 256KiB), (lru) MaxValueBytes not needed (EntryMeta is small).

### D7. queue job ID formats (03 was self-contradictory).
- With IdempotencyKey: jobID = "idem-" + hex(sha256(queueName + "|" + key))
  (deterministic; no timestamp; ordering comes from ready-marker TS).
- Without: jobID = "<usec20>-<rand16hex>" (zero-padded micros, sortable).

### D8. Shard encodings.
- lru: decimal, zero-padded to width of ShardCount-1 (e.g. 000..127).
- queue: 4 lower-hex digits (0000..ffff), Shards ≤ 65536.
Both are lexicographically stable; documented per component.

### D9. Clock-skew defaults stay per-component (different purposes).
- cas: ClockSkewHint 2m — safety margin for tombstone GC only.
- queue: ClockSkewTolerance 2s — visibility/lease timing only.
- lru: no correctness clock use; TombstoneMinAge margin as in D3.

### D10. Observability metric names (binding, extends 00 §4.4).
Required: s3collections_latency_seconds{component,op,outcome} (histogram),
s3collections_cas_attempts{component,op} (histogram),
s3collections_conflicts_total{component,op} (counter),
s3collections_retries_total{component,op,reason} (counter),
s3collections_list_pages_total{component,op,prefix} (counter; prefix label is
the STATIC prefix template e.g. "lru/entries/<shard>/", never user data),
s3collections_corrupt_total{component} (counter),
s3collections_reaper_runs_total{component,shard? — NO: shard cardinality;
use {component} only}, s3collections_reaper_deleted_total{component,kind}.
lru adds: s3collections_lru_entries{shard=NO} gauge {component="lru",kind=live|tombstone}
(approximate), s3collections_lru_evictions_total{reason}, s3collections_lru_bytes gauge.
queue adds: s3collections_queue_events_total{queue,event,outcome},
s3collections_queue_depth{queue,kind=ready|leased|dead} gauge (best-effort,
sampled during reaper runs).
CAUTION on label cardinality: never put user keys/job IDs in label values.

### D11. lru API deltas from 02.
- `New` per D6. `Store` remains an interface? NO — concrete *lru.Store struct
  with methods (matches cas/queue style; consumers can define their own
  interfaces). Methods: Get/Set/Touch/Delete/Len/Stats/StartEvictor/Close.
- `Entry.ETag` → `Entry.Revision uint64` (cas revision; we never expose ETags).
- Hooks dropped (D2). Stats keeps state snapshot fields only
  (Shards, Capacities, ApproxItems, ApproxBytes, Tombstones, LastEvictionAt);
  lifetime counters move to Meter.
- Len: LIST-based count of live entries; document cost; no approx mode in v1
  (EnableApproxCounters DROPPED from v1 scope — reduces surface; reaper/GC
  capacity decisions use LIST sampling as documented in 02).
- Set: create via cas.Create; on ErrAlreadyExists → cas.Update (with
  WithResurrect) — one integrated retry loop.

### D12. queue API deltas from 03.
- `New` per D6. Hooks dropped (D2) → metrics per D10.
- cas integration: canonical job state transitions use cas.Update (fn validates
  current state and returns new envelope) — this subsumes 03's UpdateCAS(matchETag)
  sketches; Fence = returned Record.Revision. Complete idempotence via Update
  no-op when already completed/dead.
- ErrNotLeased is returned by Renew/Complete/Retry/Dead when the job's lease
  owner != WorkerID or state != claimed (checked inside the Update fn);
  ErrStaleLease when the underlying CAS loses the race (rev mismatch).
- Guard(latest, provided uint64) error — kept as designed.
- SequencerEnabled strict-ordering mode: KEPT, documented ceiling (~tens of
  enqueues/s). Sequencer object: cas key "sequencer" updated via cas.Update.
- StartReaper/StartGC: merged into ONE background loop method
  `StartMaintenance(ctx)` (reaper pass every ReaperInterval, GC pass every
  GCInterval, jittered, idempotent, safe to run on every replica). Separate
  start methods are NOT provided in v1 (less API surface; single loop is
  enough). Godoc documents that every replica SHOULD run it.

### D13. Envelope/codec details.
- cas envelope exactly as 01 §3 (v/state/key/rev/value_b64/value_sha256/
  created_at/updated_at/deleted_at/writer_id). JSON field names as in 01.
- lru entry value (stored INSIDE cas value): 02's JSON {v,k,m{size,created,
  last,count},a,cleared}. Tombstone side data (del_reason) DROPPED — cas
  tombstone carries DeletedAt; eviction reason is a metric label.
- queue job value (INSIDE cas value): 03's JSON envelope, with payload
  base64. Reason history capped at 8.

### D14. HTTP/SigV4 backend (package s3backend).
`s3backend.NewHTTPClient(cfg HTTPConfig) (*s3backend.HTTPClient, error)`:
endpoint, region, bucket, path-style flag, static credentials, http.Client
override, SSE none. Implements Backend (Get/Put w/ preconditions/Delete/
ListObjectsV2) with SigV4 signing, XML decode for List, error mapping
(404→ErrNotFound, 412→ErrPreconditionFailed, 5xx/SlowDown→Error{Retryable}).
No retries inside (callers retry via cas). Tested against an httptest fake.
Build-tagged integration test vs real S3/MinIO per 00 §5.5.

### D15. Testing bar (binding for all coders).
- `go test ./<pkg>/ -race -count=1` must pass; `gofmt -l` clean; `go vet` clean.
- Unit tests per component test plans (01 §10, 02, 03) using
  s3backend.Memory + s3backend.Chaos.
- Concurrency stress per test plans; keep stress durations ≤ ~20s default.
- Fuzz targets: cas envelope parse; lru key escape; queue job JSON round-trip.
- No third-party deps. No network access in default tests.

## 2. Work decomposition for coders
- coder-cas: root package plumbing is provided; implement package cas per 01 + D1..D4, D13, D15.
- coder-http: s3backend HTTP client per D14.
- coder-lru: package lru per 02 + D3, D6, D8, D10, D11, D15 (after cas lands).
- coder-queue: package queue per 03 + D5..D8, D10, D12, D13, D15 (after cas lands).
- docs/examples coder: README, docs/*.md user docs, examples/ per 00 §7.3 (after APIs land).
