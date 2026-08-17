
# Distributed LRU Metadata Store (package lru)

Scope: design and exact Go API for a production-grade, S3-backed distributed LRU metadata store (no centralized hot key). The LRU metadata tracks cache entries (key, size, created, last-access, access-count) and enforces a total capacity across many stateless replicas using only S3 semantics and the CAS package as the sole primitive for per-key conditional writes.

This document is self-contained and does not modify or depend on code in other packages; it states explicit assumptions for the `cas` and `s3backend` packages.


## Assumptions about sibling components

- s3backend
  - Provides a low-level Backend interface with methods to GET, PUT (with If-None-Match: * and If-Match: <etag>), LIST (prefix, paginated, lexicographic, strongly consistent), and DELETE (no conditional). Returns `ETag` on successful PUT. Errors are structured and map to HTTP/S3 semantics (PreconditionFailed, SlowDown, 5xx, etc.).
  - Strong read-after-write consistency for GET and LIST. Per-key PUT is atomic.
  - ETags are opaque strings. The in-memory backend uses per-key incrementing versions.

- cas
  - `cas.Store` (assumed minimal API) wraps per-key JSON blobs with CAS, using only S3 semantics:
    - `Get(ctx, key) (Value, error)` where `Value{ETag string, Body []byte}`; returns `cas.ErrNotFound` if absent.
    - `PutIfAbsent(ctx, key, body []byte) (Value, error)` uses If-None-Match: *.
    - `CAS(ctx, key, baseETag string, newBody []byte) (Value, error)` uses If-Match: <etag>; returns `cas.ErrConflict` on ETag mismatch.
  - No multi-key ops. No conditional DELETE. Tombstones are expressed by writing a JSON body that marks the object deleted.
  - Retries with jittered backoff are implemented in lru; cas does not hide 412/5xx.


## Public API (exact)

```go
package lru

import (
    "context"
    "time"
)

// Store is a distributed LRU metadata index. All methods are safe for
// concurrent use by many replicas. No global process state.
type Store interface {
    // Get returns metadata for key if present and not tombstoned.
    // If the entry exists but is tombstoned, returns ErrNotFound.
    Get(ctx context.Context, key string) (Entry, error)

    // Set inserts or updates metadata for key and marks it recently used.
    // - If key is absent, creates it (If-None-Match: *).
    // - If key exists (including tombstone), updates by CAS from current ETag.
    //   Resurrection from tombstone is allowed.
    // Idempotent for identical meta.
    Set(ctx context.Context, key string, meta EntryMeta) error

    // Touch marks key as recently used (sets access bit) and may update the
    // last-access timestamp and access count subject to TouchPolicy.
    // No-op if key is absent or tombstoned (ErrNotFound).
    Touch(ctx context.Context, key string) error

    // Delete tombstones the key via CAS (does not perform physical S3 DELETE).
    // A later GC may purge tombstones physically.
    Delete(ctx context.Context, key string) error

    // Len returns the total number of live entries (not counting tombstones).
    // If approximate counters are enabled, returns approximate count.
    // Otherwise performs LIST-based counting (potentially expensive).
    Len(ctx context.Context) (int64, error)

    // Stats returns aggregated statistics and approximate usage.
    Stats(ctx context.Context) (Stats, error)

    // StartEvictor starts background eviction workers. It is safe to call
    // multiple times; subsequent calls are no-ops. The evictor runs until the
    // passed context is done or Close is called.
    StartEvictor(ctx context.Context) error

    // Close releases resources and stops background workers started by
    // StartEvictor. Idempotent.
    Close() error
}

// New constructs a Store using the given cas store and options.
func New(store cas.Store, opts Options) (Store, error)

// Entry is the public view of a metadata record. ETag is included to allow
// advanced users to stitch custom flows; typical callers do not need it.
type Entry struct {
    Key           string
    Meta          EntryMeta
    ETag          string // CAS revision
    Deleted       bool   // true if this is a tombstone (Get never returns Deleted=true)
}

// EntryMeta is stored in S3 as JSON and is part of the CAS-ed envelope.
type EntryMeta struct {
    SizeBytes     int64
    CreatedAt     time.Time
    LastAccessAt  time.Time
    AccessCount   uint64
}

// Stats exposes internal state useful for monitoring and tests.
type Stats struct {
    Shards               int
    CapacityBytes        int64
    CapacityItems        int64

    ApproxItems          int64
    ApproxBytes          int64

    Tombstones           int64 // approximate count of tombstones
    Evictions            int64 // lifetime count
    Touches              int64 // attempts
    TouchWrites          int64 // touches that wrote
    TouchSkips           int64 // coalesced/no-op
    CASConflicts         int64
    Retries              int64

    LastEvictionAt       time.Time
}

// TouchPolicy controls how aggressively Touch writes.
type TouchPolicy struct {
    // CoalesceWindow bounds how often we will write touches per key.
    // If LastAccessAt >= now-CoalesceWindow and AccessCount < CountStepThreshold,
    // we skip the write. 0 disables coalescing (not recommended).
    CoalesceWindow time.Duration

    // If true, increment AccessCount on each Touch write; otherwise keep count.
    UpdateAccessCount bool

    // If true, update LastAccessAt when we do write. If false, we only set the
    // access bit and leave timestamps unchanged.
    UpdateLastAccess bool
}

// Options configures a Store.
type Options struct {
    // Namespace prefix for S3 keys; must be unique per logical LRU.
    // Example: "lru/" (trailing slash optional).
    Prefix string

    // Number of shards to distribute entries across. Must be >=1.
    // Recommend a power of two. Default: 128.
    ShardCount int

    // Target capacity. If both are set >0, eviction triggers when either
    // dimension exceeds its cap (bytes OR items).
    CapacityBytes int64 // 0 to disable byte-based cap
    CapacityItems int64 // 0 to disable item-based cap

    // Eviction workers and cadence.
    EvictorInterval  time.Duration // periodic tick per worker; default 2s
    EvictorWorkers   int           // number of concurrent shard workers; default min(ShardCount, 4)
    EvictorBatchSize int           // max entries to process per shard per tick; default 512

    // Touch behavior.
    TouchOnGet   bool        // if true, Get internally calls Touch for the key
    TouchPolicy  TouchPolicy // default: {CoalesceWindow: 1s, UpdateAccessCount: true, UpdateLastAccess: true}

    // LIST paging size hint (max keys per page). 0 uses backend default.
    ListPageSize int

    // Retry/backoff controls.
    MaxRetries      int           // per operation (CAS/Try); default 6
    BaseBackoff     time.Duration // default 50ms
    MaxBackoff      time.Duration // default 2s

    // Approximate per-shard counters. If enabled, writers best-effort update a
    // small per-shard object with (items, bytes). Evictor still falls back to
    // LIST when near capacity or counters look stale.
    EnableApproxCounters bool

    // GC of tombstones and stale entries.
    TombstoneMinAge time.Duration // age before physical DELETE; default 5m

    // Observability hooks (all optional; nil-safe).
    Hooks Hooks
}

// Hooks allows applications/tests to observe behavior. All methods are optional.
type Hooks interface {
    OnRetry(op, key string, attempt int, err error)
    OnConflict(op, key string)
    OnEvict(shard int, key string, size int64, reason string)
    OnTouch(key string, wrote bool)
    OnError(op, key string, err error)
}

// Errors returned by Store methods.
var (
    ErrNotFound       = errors.New("lru: not found")
    ErrClosed         = errors.New("lru: closed")
)
```


## S3 key layout

All keys live under a caller-provided prefix, defaulting to `lru/`.

- Entry objects (one per logical cache key):
  - `lru/entries/<shard>/<escaped-key>`
    - `<shard>` is decimal zero-padded to width needed for ShardCount, e.g., `000`, `127`.
    - `<escaped-key>` is URL path-segment escaped (RFC 3986) of the logical key.

- Per-shard approximate counters (optional):
  - `lru/shards/<shard>/counters.json`

No global index or hot key exists. LISTs are always per-shard prefixes.


## Entry envelope format (JSON)

Entries and tombstones are stored as JSON to keep the on-disk/in-memory format simple and debuggable. Short field names keep payloads small.

Live entry example:

```json
{
  "v": 1,
  "k": "user:1234:profile",            // optional for debugging
  "m": {
    "size": 16384,
    "created": "2026-08-17T12:00:00Z",
    "last": "2026-08-17T12:01:30Z",
    "count": 7
  },
  "a": true,                            // access bit (second-chance)
  "cleared": "2026-08-17T12:01:00Z",   // when evictor last cleared 'a'; absent if never cleared
  "deleted": false
}
```

Tombstone written by eviction or Delete:

```json
{
  "v": 1,
  "k": "user:1234:profile",
  "m": {
    "size": 16384,
    "created": "2026-08-17T12:00:00Z",
    "last": "2026-08-17T12:01:30Z",
    "count": 7
  },
  "a": false,
  "cleared": "2026-08-17T12:02:00Z",
  "deleted": true,
  "del_at": "2026-08-17T12:02:00Z",
  "del_reason": "evict"                 // or "delete"
}
```

- `v` is the envelope version.
- `a` is the second-chance access bit; replicas set it to true on Set/Touch; evictor clears it on its first pass.
- `cleared` is only written by the evictor when it flips `a` from true->false; used to avoid immediate eviction in the same pass and to make behavior robust across restarts.
- `deleted` governs visibility; any read of `deleted: true` treats the entry as absent.
- `m` holds the public `EntryMeta`.

All writes are via CAS; ETag is the revision. Physical S3 DELETE is a GC optimization, never used for correctness.


## Core design: sharded CLOCK (second-chance) without hot keys

Problem: a single global LRU index object becomes a hot key at modest QPS. Solution: shard the keyspace and store one object per entry. Each entry carries its own second-chance access bit (`a`) and optional `cleared` timestamp. Eviction is performed by background workers that LIST a shard prefix in lexicographic order and apply a two-pass CLOCK:

- First pass: clear `a` for entries with `a=true` by CAS-ing a write `a=false` and set `cleared` to now. Do not evict in this pass.
- Second pass: when the worker later encounters an entry with `a=false`, and `cleared` is older than a small grace (e.g., EvictorInterval/2), it attempts eviction by CAS-ing a tombstone.

No shared mutable state across replicas is required; ordering and progress come from LIST cursors. Each worker keeps an in-memory scan cursor per shard (last processed key) to continue across ticks. A crash/restart simply resets cursors to the shard prefix start; correctness is unaffected.

Alternative considered: per-shard recency index objects probabilistically updated on Touch (e.g., reservoir sampling or count-min sketches). Those become hot keys under write storms and create extra CAS contention. Given S3 op costs, the pure entry-local second-chance is preferred: touches write only the entry; eviction throughput scales with shard-level parallelism and LIST page sizes. We choose the CLOCK design.


## Concurrency and Touch

- Touch flow:
  1. GET current envelope (ETag e0). If not found or tombstoned, return ErrNotFound.
  2. If TouchPolicy.CoalesceWindow > 0 and `LastAccessAt >= now - window`, skip write (approximate recency is acceptable and explicitly documented).
  3. Otherwise prepare a new envelope: set `a=true`; if `UpdateLastAccess`, set `last=now`; if `UpdateAccessCount`, increment `count`.
  4. CAS with If-Match e0. On `cas.ErrConflict` or retriable 5xx/SlowDown, retry with jittered backoff up to `MaxRetries`. On exhaustion, drop the touch (best-effort). Hooks are called on retries and conflicts.

- Losing a touch is acceptable: LRU recency is approximate. A missed touch merely increases the chance the entry will be selected for eviction on the next sweep; the second-chance bit still provides a guard because other touches or a later coalesced touch will set `a=true`.

- Get optionally calls Touch internally (TouchOnGet), applying the same coalescing. Applications that call Touch explicitly should disable TouchOnGet to avoid double work.


## Safe eviction (no loss of newer updates)

Eviction is expressed as a CAS to a tombstone body; S3 DELETE is never used to enforce correctness.

Per candidate key K:

1. GET envelope with ETag e0.
2. If `deleted: true` => schedule GC if older than TombstoneMinAge, continue.
3. If `a == true` => try to clear it: CAS(e0, set a=false, cleared=now). On conflict, ignore and continue. Do not evict in this tick.
4. If `a == false` and `cleared` older than a small grace:
   - Re-GET to obtain current ETag e1 and verify `a==false`.
   - If `e1 != e0` or `a==true`, skip (someone touched/updated it); continue.
   - CAS(e1, write tombstone with `deleted=true`, `del_reason="evict"`).
   - On conflict, skip; on success, account freed size and items.

Race: "touch during eviction"
- Touch performs GET(eX) then CAS(eX, set a=true, maybe update last/count).
- Evictor re-GETs to e1 just before CAS. If Touch wrote in between, e1 changes; eviction CAS fails with conflict. The entry remains live and marked `a=true`.
- If evictor cleared `a` earlier (first pass) and then Touch writes `a=true`, a subsequent eviction CAS will conflict (different ETag) and abort. Eviction never deletes an entry with newer updates.


## Capacity enforcement

- Cap dimensions: bytes (sum of `m.size`) and/or items. When both caps are set, exceeding either triggers eviction until both are under target with a small headroom (e.g., 5%).

- Soft vs hard: The store enforces a soft cap by default. Writers (Set) never block; the background evictor converges capacity below caps. Optionally, a future StrictSet mode could synchronously run a small eviction batch on Set when far above cap; out of scope for v1.

- Counting usage:
  - If `EnableApproxCounters` is true, writers best-effort update `lru/shards/<shard>/counters.json` by CAS with additive merges. Under contention, updates may be dropped. Evictor uses counters as a trigger signal; when above ~80% cap it LISTs to measure precisely.
  - If counters are disabled, evictor regularly samples shards by LIST and estimates occupancy by summing sizes of a few pages; if above threshold, it proceeds to eviction scanning.

- Eviction batch size and scanning:
  - Each tick per shard processes up to `EvictorBatchSize` entries, stopping earlier if enough bytes/items were freed to fall under caps with headroom.
  - List scanning uses continuation tokens (lexicographic `StartAfter`) to amortize traversal across ticks. If a tick hits the end, the cursor wraps to the shard prefix start on next tick.

- Behavior under write storms:
  - Many Set/Touch operations increase CAS conflicts, which we bound by retry budgets. Missed touches are acceptable; some recently-used entries may be evicted (see Guarantees below).
  - Evictor focuses on freeing the largest entries first only if it happens to encounter them; we do not maintain a size-ordered index to avoid hot keys. Over time, CLOCK approximates recency regardless of size; large entries statistically free capacity faster as they are encountered.

- Stale/orphaned metadata cleanup:
  - Writers that crash after creating metadata but before writing corresponding data in a higher layer can leave "orphan" entries. The LRU package cannot verify external data existence; it provides a heuristic GC: entries with `m.count==0` and `CreatedAt` older than `2*EvictorInterval` may be tombstoned opportunistically during scans.
  - Applications can opt-in to a higher-layer probe by wrapping lru with their own verifier; out of scope here.


## Consistency guarantees

- Set visibility: After `Set` returns, a subsequent `Get` from any replica observes the written entry (S3 strong read-after-write).
- Touch visibility: After a successful `Touch`, the entry has `a=true` and (optionally) updated last-access/count. An evictor must first clear `a` in a later pass before eviction; if contention causes the touch write to be dropped, the entry remains with its prior `a` value and may be selected for clearing earlier.
- Delete: After `Delete` returns, the entry is tombstoned and `Get` returns `ErrNotFound`. A new `Set` may resurrect the entry by reading the tombstone and CAS-ing a live envelope from its ETag.
- Read under concurrent eviction: Eviction never removes newer updates. If eviction wins the CAS to tombstone, concurrent Set/Touch CAS attempts fail with `ErrConflict` and the caller retries.
- Evicting recently used entries: Under extreme contention and with coalescing, an entry used very recently could be evicted if its touches are consistently dropped and the evictor already cleared `a=false`. This is bounded by the coalescing window and evictor cadence: an item must go through at least one full clear pass (flip `a` true->false), then survive until a later pass without any successful touch CAS. With default windows (1s coalesce, 2s interval), this implies roughly >2s of no successful touch writes before eviction risk.


## Cost analysis (S3 operations)

Per foreground API call (worst/typical):
- Get: 1 GET. If TouchOnGet and coalesce passes: +1 GET + 1 PUT (CAS). Typical: 1 GET.
- Set (absent): 1 PUT If-None-Match. If strict creation race: +retries. Typical: 1 PUT.
- Set (update or resurrect): 1 GET + 1 PUT (CAS). Typical: 2 ops.
- Touch: 1 GET + (maybe) 1 PUT (CAS). With coalescing, most touches skip the PUT. Typical: 1–2 ops.
- Delete: 1 GET + 1 PUT (CAS to tombstone). Physical DELETE later by GC: +1 DELETE.

Evictor per-candidate costs (upper bounds):
- LIST page: 1 LIST yields up to backend max keys (e.g., 1000). We only LIST when above threshold or to maintain cursors.
- For each candidate examined: 1 GET.
  - If `a==true`: +1 PUT (CAS clear) [first pass].
  - If `a==false` and grace satisfied: +1 GET (re-GET) +1 PUT (CAS to tombstone).
- GC of tombstones older than TombstoneMinAge: +1 DELETE per tombstone.

Memory footprint:
- Per shard worker: O(1) state plus cursor string. No in-memory index. Per key, only the JSON envelope stored in S3.

Recommended shard count:
- Aim for ~50–200k entries per shard to keep sweep costs acceptable. Rule of thumb: `ShardCount = max(64, min(4096, ceil(TotalEntries / 100k)))`.
- For example, with 10M entries and 128 shards (~78k entries/shard), a complete sweep would touch at most ~78 pages if page size=1000. With `EvictorBatchSize=1000` and interval 2s, each shard can free capacity in a few seconds without full sweeps.

Scalability limits:
- The bottleneck is GET+CAS per candidate during eviction. With `EvictorWorkers=8`, each doing ~200 ops/s (conservative with backoffs), we sustain ~1600 S3 ops/s of eviction-side traffic, sufficient for many caches at modest write rates. Touch and Set scale linearly with key distribution and shard count since there is no hot key.


## Failure modes and retries

- All operations use bounded retries with exponential jittered backoff on retriable errors (5xx, SlowDown) and CAS conflicts.
- Crashes between any two S3 calls:
  - Touch: may be dropped; safe.
  - Set: if crash before CAS -> no write; after CAS -> write is visible atomically.
  - Delete/Evict: crash before tombstone CAS -> no effect; after CAS -> tombstone visible; physical DELETE is best-effort GC.
- Clock skew: timestamps are advisory. No correctness depends on wall-clock; clearing/eviction decisions tolerate skew by relying on ETag changes (fencing). `cleared` time only gates same-pass eviction and is compared against local time with generous margins (>= EvictorInterval/2).


## API usage examples (illustrative snippets)

```go
l, _ := lru.New(casStore, lru.Options{Prefix: "lru/", ShardCount: 128, CapacityBytes: 100<<30})
ctx := context.Background()
_ = l.StartEvictor(ctx)

aKey := "user:1234:profile"
_ = l.Set(ctx, aKey, lru.EntryMeta{SizeBytes: 16<<10, CreatedAt: time.Now(), LastAccessAt: time.Now(), AccessCount: 1})
// later
if e, err := l.Get(ctx, aKey); err == nil {
    // optional manual touch if TouchOnGet is false
    _ = l.Touch(ctx, aKey)
    _ = e // use
}
```


## Test plan

Unit tests against the in-memory strong S3 backend and a chaos/fault wrapper. All tests run with `-race` and include concurrency stress.

1. Core CRUD
   - Set absent -> Get returns entry with `a=true`.
   - Set existing -> metadata updated; ETag changes.
   - Delete -> Get returns ErrNotFound; resurrect via Set works (Get->CAS from tombstone).

2. Touch coalescing
   - Rapid repeated Touch within CoalesceWindow causes at most one CAS write; Hooks record TouchSkips.
   - With TouchOnGet=true, Get performs coalesced Touch.

3. Eviction vs touch race
   - Arrange: entry with `a=false` and nearing capacity; concurrently Touch loop from multiple goroutines.
   - Verify: eviction CAS loses when Touch wins; entry survives and has `a=true`.

4. Two-pass CLOCK behavior
   - Insert N entries; run a single evictor tick that only clears `a` (no eviction due to grace).
   - Next tick: verify entries with no intervening Touch are tombstoned until capacity < cap.

5. Capacity enforcement under write storms (K replicas)
   - K workers call Set on random new keys with random sizes around a target byte cap. Start evictor.
   - Verify: steady-state live bytes remains within cap +/- 5% after warm-up.
   - Measure conflicts, retries, and touch skips via Hooks.

6. Stale/orphaned cleanup
   - Create entries with `count=0` and old `CreatedAt`. Verify the scanner opportunistically tombstones them.

7. Tombstone GC
   - After evictions, wait TombstoneMinAge; run GC pass; verify S3 DELETE has reduced tombstone count while new Set can still resurrect via tombstone CAS before physical delete.

8. Chaos: injected 5xx/SlowDown/latency
   - Wrap backend; run prolonged mixed workload (Get/Set/Touch/Delete) with evictor running.
   - Assert no panics, eventual convergence to capacity, and bounded retries.

9. Fuzz: keys and shard distribution
   - Fuzz key strings with high-entropy lengths and chars; ensure escape/unescape is lossless and paths remain valid.

10. Concurrency stress
    - 100 goroutines performing Get/Touch on overlapping keys with evictor scanning the same shard. Ensure no data races (Go race detector) and Stats counters non-decreasing and sane.


## Open questions / to validate in implementation

- Exact `cas.Store` API surface: we assumed `PutIfAbsent`, `Get`, `CAS`. If it differs, adapt flows to avoid extra GETs.
- Whether to include a StrictSet mode (synchronous eviction) in v1 or defer.
- Default shard count heuristic and LIST page size: tune based on in-memory backend perf.
- Counters: keep disabled by default; if enabled, exact merge policy for additive CAS updates.
- Should `Len` be approximate-only to avoid expensive LISTs? Current API returns an exact value if counters are disabled (may be slow on large stores) — callers must be aware.

