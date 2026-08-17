# CAS Store (package cas): Versioned Compare-and-Swap over S3

Status: draft v1
Scope: exact API surface, S3 key/object format, state machines, retries, GC, costs, and tests for the foundational CAS store used by lru and queue.

Assumptions about s3backend (only these S3 semantics are relied on):
- Strong read-after-write consistency for GET and LIST (prefix listing is strongly consistent, paginated, lexicographic).
- Atomic per-key PUT.
- Conditional PUT with If-None-Match: * and If-Match: <etag>. 412 on failed preconditions.
- DELETE per key (no conditionals).
- ETags are opaque. The in-memory backend uses per-key incrementing versions.

Out-of-scope: any multi-key transactions, server TTL, notifications, timers.

## 1) Public Go API (exact)

Package: `github.com/damianb/s3collections/cas`

Core types and errors:

```go
package cas

import (
    "context"
    "time"
)

// Errors returned by Store methods.
var (
    ErrAlreadyExists = errors.New("cas: already exists")           // Create only
    ErrNotFound      = errors.New("cas: not found")
    ErrDeleted       = errors.New("cas: key tombstoned")           // Visible on some ops
    ErrConflict      = errors.New("cas: conflict (stale revision)") // 412 classification
    ErrTooLarge      = errors.New("cas: value exceeds max size")
    ErrCorrupt       = errors.New("cas: envelope/value checksum mismatch")
)

// NotFoundError carries additional info for tombstones; use errors.As.
type NotFoundError struct {
    Key        string
    Tombstoned bool         // true if object exists as tombstone
    Revision   uint64       // last known revision if Tombstoned == true, else 0
    DeletedAt  time.Time    // zero if unknown
}
func (e *NotFoundError) Error() string { /* ... */ }

// Operation identifies the Store operation for hooks/logging.
type Operation string
const (
    OpCreate Operation = "create"
    OpGet    Operation = "get"
    OpCAS    Operation = "cas"
    OpDelete Operation = "delete"
    OpGC     Operation = "gc"
)

// Record is returned by Get/Create/CompareAndSwap/Update/Delete.
type Record struct {
    Key       string
    Value     []byte      // nil for tombstone results from Delete
    Revision  uint64      // logical, monotonically increasing by 1 per mutation
    State     State       // Live or Tombstone
    CreatedAt time.Time
    UpdatedAt time.Time
    DeletedAt time.Time   // zero if State==Live
    WriterID  string      // from options; observability only
    ETag      string      // last seen opaque ETag; not a concurrency token
}

type State int
const (
    Live State = iota
    Tombstone
)

// UpdateFn is called with the current record (which may be tombstoned if includeTombstone=true).
// It must return the new value to write, or (nil, ErrDeleted) to explicitly keep as tombstone, or
// (nil, nil) to indicate no-op if caller chooses.
// UpdateFn must be pure and side-effect free; it may be invoked multiple times under contention.
type UpdateFn func(ctx context.Context, cur Record) (next []byte, err error)

// Store is safe for concurrent use by multiple goroutines.
type Store struct { /* internal fields */ }

// New creates a Store targeting a bucket and prefix. Prefix may be empty or end with '/'.
func New(b s3backend.Backend, bucket, prefix string, opts ...Option) (*Store, error) // b from package s3backend

// Create creates a new key if and only if it does not exist at all (no object, not even a tombstone).
// On success returns the created live record (revision==1). If a tombstone or live object exists, returns ErrAlreadyExists.
func (s *Store) Create(ctx context.Context, key string, value []byte, opts ...WriteOption) (Record, error)

// Get returns the live record. If the key is tombstoned or missing, returns ErrNotFound.
// If tombstoned, the returned error will satisfy errors.As(err, *NotFoundError) with Tombstoned=true and last Revision.
func (s *Store) Get(ctx context.Context, key string) (Record, error)

// GetMeta returns metadata even for tombstones. Missing keys return ErrNotFound (NotFoundError with Tombstoned=false).
func (s *Store) GetMeta(ctx context.Context, key string) (Record, error)

// CompareAndSwap replaces the value iff the current state is Live and its Revision==expect.
// Implementation: GET current (to obtain ETag and Revision), check Revision==expect, then PUT with If-Match: <ETag>.
// If the key is tombstoned, returns ErrDeleted. If revisions mismatch, returns ErrConflict before PUT.
func (s *Store) CompareAndSwap(ctx context.Context, key string, expect uint64, newValue []byte, opts ...WriteOption) (Record, error)

// Update performs a read-modify-write loop calling fn, retrying on conflicts with backoff.
// If fn returns a byte slice equal to the current Value and makes no state change, Update short-circuits and
// returns the current Record without performing a PUT (no-op). Equality is byte-for-byte.
// If the key is tombstoned, fn is not invoked and ErrDeleted is returned unless IncludeTombstone was set in opts.
// When IncludeTombstone is true, fn is invoked with a tombstone Record, but any attempt to write a live value results in ErrDeleted.
// This preserves the invariant that re-creation is blocked while tombstone exists (see §4 Resurrections).
func (s *Store) Update(ctx context.Context, key string, fn UpdateFn, opts ...WriteOption) (Record, error)

// Delete marks the key with a tombstone (state->Tombstone), incrementing the revision.
// Semantics:
// - If the key is Live and expect==current live revision, write tombstone (rev=liveRev+1) and return it.
// - If the key is Tombstone (rev=tombRev):
//     * If expect==tombRev OR expect==tombRev-1 (the live revision used to delete), succeed idempotently and return the tombstone without writing.
//     * Else: ErrConflict (stale token).
// - If the key is missing: ErrNotFound.
// - Any other revision mismatch when Live: ErrConflict.
func (s *Store) Delete(ctx context.Context, key string, expect uint64, opts ...WriteOption) (Record, error)

// Retry is a helper to apply the given policy around an arbitrary operation, classifying conflicts, 5xx, and throttling.
func Retry(ctx context.Context, policy RetryPolicy, op func(context.Context) error) error

// IsConflict returns true iff err classifies as a CAS conflict (use errors.Is(err, ErrConflict)).
// Create's 412 (If-None-Match) maps to ErrAlreadyExists and should NOT be treated as a conflict.
func IsConflict(err error) bool

// Notes on Delete idempotence and conflicts:
// - Delete requires the caller to pass the observed revision. If the object is already a tombstone
//   at rev R, Delete is idempotent when expect==R or expect==R-1 (the prior live revision). Otherwise ErrConflict.
//   This preserves fencing and detects stale callers.

// Options and per-call overrides.

type Options struct {
    WriterID       string        // included in envelopes for observability.
    MaxValueBytes  int           // default 256*1024 (256 KiB). Hard cap for single-object PUTs; no multipart.
    Retry          RetryPolicy   // default backoff policy; may be overridden per call.
    Hooks          Hooks         // optional observability callbacks.
    ClockSkewHint  time.Duration // only for GC/timestamp safety margins; 0->default 2m
    KeyCodec       KeyCodec      // key validation/escaping; default URLPathEscape on segments
}

type Option func(*Options)

type WriteOptions struct {
    Retry            *RetryPolicy // overrides Store default
    IncludeTombstone bool         // see Update semantics
}

type WriteOption func(*WriteOptions)

type RetryPolicy struct {
    MaxAttempts int           // including the first try; default 8
    Base        time.Duration // default 10ms
    Max         time.Duration // default 500ms per attempt delay cap
    Jitter      float64       // 0..1 full jitter; default 1.0
}

type Hooks struct {
    // Called before and after backend operations.
    OnAttempt func(ctx context.Context, op Operation, key string, attempt int)
    OnBackoff func(ctx context.Context, op Operation, key string, attempt int, delay time.Duration, err error)
    OnResult  func(ctx context.Context, op Operation, key string, rec Record, err error)
}

// KeyCodec encodes application keys to S3 object keys and validates inputs.
type KeyCodec interface {
    // Encode returns the S3 object key under the store's prefix. Must be injective.
    Encode(appKey string) (objectKey string, err error)
    // Decode reverses Encode for listing/GC.
    Decode(objectKey string) (appKey string, err error)
}
```

Notes:
- We deliberately expose a logical `Revision` as the fencing/concurrency token, not S3 ETag, because ETag is backend/encoding-dependent and opaque. Revision increments by 1 on every successful mutation (Create->1, CAS/Update/Delete -> +1) and is persisted in the envelope. This makes a portable fencing token for lru/queue.
- Resurrections are disallowed (details in §4). Users must Create after GC removes a tombstone.

## 2) Revision semantics

- The envelope contains a `rev` field, a monotonically increasing uint64. First creation sets `rev=1`.
- Every successful state change (CAS/Update/Delete) sets `rev=prev+1`.
- All writes are guarded by S3's If-Match on the last observed ETag to ensure atomicity of the envelope write. The envelope's `rev` is checked by the library prior to writing to avoid silent divergence but is not used as the server-side precondition.
- Clients observe `Revision` in API results. This value is stable across backends and encodings and is appropriate as a fencing token in higher layers such as queue claim tokens or LRU epoch markers.

Why not expose ETag:
- ETag may change when metadata or encoding changes; it's opaque and not monotonically increasing across backends.
- We may later compress values or change JSON formatting; ETag would change even if logical state didn't.
- A numeric monotonic `Revision` is easy to compare and reason about and allows layers to construct invariants like "if my token < stored revision, abandon" without reading ETags.

## 3) S3 key layout, key validation/escaping, envelope JSON

- Bucket: supplied by caller.
- Prefix: supplied by caller (may be empty, must not contain back-to-back '/').
- Object key: `prefix + encode(key) + ".cas.v1.json"`.
  - `encode(key)`: URL path escape each path segment using `url.PathEscape`, preserving '/'. Keys are validated to be <= 1024 runes and only contain printable Unicode (no control chars). 0-length keys and keys with trailing '/' are rejected. This encoding is injective and safe for lexicographic prefix listing.

Max value size policy:
- Default `MaxValueBytes = 256 KiB`. Intended use is metadata for LRU and queue, not large payloads.
- Hard cap enforceable via Options; single-part PUT only. S3 allows very large objects, but this library deliberately caps to keep latencies reasonable and retries cheap.

Envelope JSON (exact):

Live record example:
```json
{
  "v": 1,
  "state": "live",
  "key": "users/42",
  "rev": 17,
  "value_b64": "AQIDBAU=",
  "value_sha256": "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33c1e...",
  "created_at": "2026-08-17T12:34:56.789Z",
  "updated_at": "2026-08-17T12:35:01.234Z",
  "writer_id": "svc-A-1a2b3c"
}
```

Tombstone example:
```json
{
  "v": 1,
  "state": "tombstone",
  "key": "users/42",
  "rev": 18,
  "deleted_at": "2026-08-17T12:36:00.000Z",
  "created_at": "2026-08-01T00:00:00.000Z",
  "updated_at": "2026-08-17T12:36:00.000Z",
  "writer_id": "svc-A-1a2b3c"
}
```

Fields:
- `v`: envelope schema version (1).
- `state`: `"live"` or `"tombstone"`.
- `key`: the application key as stored (post-validation). On read, mismatch with requested key -> ErrCorrupt.
- `rev`: logical revision (uint64, decimal in JSON).
- For `live`: `value_b64` (standard base64) and `value_sha256` (hex lowercase). On GET, we verify the checksum; mismatch -> `ErrCorrupt`.
- `created_at`: RFC3339Nano UTC. Derived from the writer's clock; best-effort only.
- `updated_at`: RFC3339Nano UTC.
- `deleted_at`: RFC3339Nano UTC; present only on tombstones.
- `writer_id`: optional identity string configured in Options.

## 4) Tombstones, Get semantics, and (no) resurrection

Tombstone design:
- Delete() does not issue S3 DELETE because it is not conditional. Instead, we write an updated envelope with `state:"tombstone"`, `rev=prev+1`, clear value fields, and set `deleted_at`.
- Get() treats tombstones as not found for simplicity: returns `ErrNotFound`. The error can be inspected with `errors.As(err, *NotFoundError)` to see `Tombstoned=true` and the last `Revision` and `DeletedAt`.
- GetMeta() returns a `Record` with `State==Tombstone` so callers can observe metadata directly.

Create-after-delete policy:
- Decision: A deleted key CANNOT be re-created until GC has physically removed the tombstone object.
- Rationale: S3 DELETE is unconditional. Allowing resurrection while a tombstone object exists would make a concurrent GC that performs DELETE inherently racy and could delete a freshly resurrected object. By forbidding resurrection from a tombstone, no valid write to the key can happen while the tombstone exists; GC is therefore safe to perform an unconditional DELETE at any time after the retention window (§7).
- Enforced by API: CompareAndSwap and Update refuse to write a live value when current state is tombstone and return `ErrDeleted`. Only Create (If-None-Match: *) can introduce a live value, and it succeeds only when the object is absent.

GC of tombstones:
- Who: Any process may run GC; typically a background task in each replica with jitter to avoid thundering herds.
- How: List() under prefix, page through lexicographically. For each object:
  1) GET and parse. If state==tombstone and `deleted_at + Retention <= now - ClockSkewHint`, mark as eligible.
  2) Optionally, write a "gc-touch" tombstone upgrading `updated_at` to signal liveness (not required for safety).
  3) DELETE the object (unconditional). Because resurrection is forbidden while a tombstone exists, no valid concurrent writer can resurrect between our GET and DELETE.
- When DELETE is safe: after the retention window elapsed to buffer wall clock skew between writers and GC runners. Default retention: 5 minutes. `ClockSkewHint` (default 2m) is added as additional safety.

Create-after-GC race:
- Two clients racing to (re)create a missing key both issue PUT with If-None-Match: *. Exactly one succeeds; others receive 412 -> surfaced as ErrAlreadyExists. Safe by S3 semantics.

## 5) Failure modes and contention

- Crash between read and conditional write (Update/CAS/Delete): safe. The subsequent writer's If-Match fails if state changed; retries proceed.
- Hot-key 412 storm: Under high contention, many CAS attempts will hit 412. We use exponential backoff with full jitter: default Base=10ms, multiplier x2 per retry, cap 500ms, MaxAttempts=8 (configurable). Expected serialization throughput per key is ~1 successful mutation per network RTT; with s3 p95 ~50–150ms, expect 6–15 writes/sec on a single hot key. Backoff dampens request rates from N contenders roughly to O(1) successes per RTT; losers back off.
- Transient 5xx / SlowDown: Retry with the same backoff policy but without re-running UpdateFn unless a new GET is performed. All retries are bounded by context deadlines. Retries add jitter to avoid synchronization.
- Clock skew: All correctness-critical decisions use S3 atomics; timestamps are for observability/GC windows only. We add `ClockSkewHint` to GC retention buffers to tolerate skew.
- Partial-envelope corruption: `value_sha256` is verified on GET. Mismatch -> `ErrCorrupt`. Corruption of metadata fields is surfaced as JSON parse errors wrapped with context; the object is unreadable and must be operator-remediated. Higher layers should treat `ErrCorrupt` as permanent.
- Large values: If `len(value) > MaxValueBytes`, return `ErrTooLarge`.
- Idempotency: Create is idempotent for identical value only if the first succeeded; otherwise 412 -> ErrAlreadyExists. Delete is idempotent when the key is already tombstoned (returns current tombstone record).

## 6) Options and overrides

Construction via functional options (`Option`). Per-call write overrides via `WriteOption`.

- `WithWriterID(string)` – populates `writer_id` for observability.
- `WithMaxValueBytes(int)` – caps value size.
- `WithRetry(RetryPolicy)` – default retry policy for the store.
- `WithHooks(Hooks)` – observability hooks.
- `WithClockSkewHint(time.Duration)` – adjust GC safety margin.
- `WithKeyCodec(KeyCodec)` – provide custom key transform. Default is URL path escaping of segments.

Per-call:
- `WithRetryPolicy(RetryPolicy)` – override retry.
- `WithIncludeTombstone()` – for Update: call `fn` with tombstone record (but still disallows resurrection; any attempt to write live value returns ErrDeleted).

Helper:
```go
// Retry executes op with the policy. Use IsConflict(err) to separate conflicts from 5xx.
func Retry(ctx context.Context, p RetryPolicy, op func(context.Context) error) error

func IsConflict(err error) bool // 412 or ErrConflict
```

## 7) S3 operations per API call (costs) and scalability

Per key (best case, uncontended):
- Create: 1x PUT (If-None-Match: *). Cost: 1 request.
- Get: 1x GET. Cost: 1 request. Tombstone -> 1x GET + ErrNotFound (with NotFoundError info).
- CompareAndSwap: 1x GET + 1x PUT (If-Match: etag). On mismatch: +2 per retry attempt (GET+PUT). Cost: 2 requests per successful uncontended CAS; with c conflicts, ~2*(1+c).
- Update: Same as CAS, plus retries. Library performs read-modify-write loop: GET -> fn -> PUT with If-Match; on 412, backoff and retry.
- Delete: 1x GET + 1x PUT (If-Match) to write tombstone. Idempotent re-delete: 1x GET.
- GC delete (after retention): 1x LIST (amortized) + 1x GET to verify tombstone + 1x DELETE. Since resurrection is forbidden while tombstone exists, DELETE is safe.

Contention behavior:
- Let RTT be end-to-end S3 round-trip (including JSON ser/de). Maximum sustainable per-key mutation throughput is roughly 1/RTT. With RTT=100ms, ~10/s. With N contenders >> 1, losers receive 412 and back off; aggregate client load is bounded by backoff.
- Listing scalability: LIST is lexicographic and strongly consistent; GC workers page with MaxKeys chosen by backend (e.g., 1k..5k) per call. With large keyspaces (10^6 keys), single pass LIST costs ~200–1000 requests depending on page size.

## 8) State machines

Per-key states and transitions (logical):

- Missing -> (Create) -> Live(rev=1)
- Live(rev=r) -> (CompareAndSwap/Update) -> Live(rev=r+1)
- Live(rev=r) -> (Delete) -> Tombstone(rev=r+1)
- Tombstone(rev=r) -> (GC Delete) -> Missing

Invalid/forbidden:
- Tombstone(rev=r) -X-> Live: forbidden by API; CompareAndSwap/Update return ErrDeleted.

S3 operations implementing transitions:
- Create: PUT If-None-Match: * of live envelope.
- CAS/Update: GET -> user fn -> PUT If-Match: etag of last GET.
- Delete: GET -> PUT If-Match: etag with tombstone envelope.
- GC: LIST -> for tombstone entries older than retention -> DELETE (unconditional).

## 9) Observability

- Hooks receive attempts/backoffs/results per operation for integration with logging/metrics.
- Records include WriterID and timestamps for audit/debug.
- Errors are structured and classifiable with `errors.Is/As`.

## 10) Test plan

Unit tests (in-memory backend):
- Create:
  - Create new key -> success, rev=1, state=Live, timestamps set.
  - Create existing live -> ErrAlreadyExists.
  - Create existing tombstone -> ErrAlreadyExists.
  - Value size > MaxValueBytes -> ErrTooLarge.
- Get:
  - Missing -> ErrNotFound (NotFoundError with Tombstoned=false).
  - Live -> returns Record with value checksum verified.
  - Tombstone -> ErrNotFound with NotFoundError{Tombstoned:true, Revision==rev}.
- CompareAndSwap:
  - Success path increments rev, preserves created_at, updates updated_at.
  - Mismatch -> ErrConflict.
  - Against tombstone -> ErrDeleted.
- Update:
  - Basic RMW loops under no contention.
  - No-op update (fn returns same value) leaves revision unchanged and performs no PUT.
  - IncludeTombstone=true invokes fn and disallows resurrection (expect ErrDeleted on write attempt).
- Delete:
  - Live->Tombstone increments rev and sets deleted_at.
  - Idempotent: Delete on tombstone returns existing tombstone.
  - Missing -> ErrNotFound.
- Envelope:
  - JSON marshal/unmarshal round-trips; checksum verified; corrupted value_b64 or wrong sha256 -> ErrCorrupt.
  - Fuzz with random JSON noise around the envelope to ensure robust parsing and error classification.
- Key encoding:
  - Encode/Decode injectivity; reject control chars; non-ASCII segments properly escaped; round-trip from list decode.

Concurrency stress:
- K goroutines (e.g., 64/128) all performing `Update(key, fn)` where `fn` appends a byte or increments a counter value.
- After N successful updates, `Get` revision equals exactly `1+N` (starting from Create).
- Verify that the number of ErrConflict equals roughly total attempts - N; ensure no lost updates.

Chaos tests (fault-injection backend):
- Random 5xx/SlowDown with p=0.1; ensure Retry policy makes progress within context deadlines.
- Inject latency jitter; ensure Hooks observe backoffs.
- Force 412 storms by having a flurry of concurrent writers; ensure throughput ~1/RTT and no livelock.

Fuzz tests:
- Envelope parsing fuzz: generate random JSON with missing/extra fields; ensure robust error returns without panics.
- KeyCodec fuzz: random Unicode strings; ensure validation/escaping path is safe.

Race detector:
- Enable `-race` in tests that drive concurrent Update/CAS on one Store; ensure no data races in client structures.

## 11) Minimal assumptions about sibling components

- `s3backend.Backend` provides GetObject, PutObject (with optional preconditions for If-None-Match/If-Match), DeleteObject, and List with strong consistency guarantees as specified.
- `lru` and `queue` only require a per-key fencing token (`Revision`) and read-modify-write with conflict detection. They do not require resurrection semantics.
- `queue` may use Delete to mark jobs finished; resurrection is handled as a new Create after GC or by using a new key, not by flipping a tombstone back to live.

## 12) Open points / future work

- If future backends support conditional DELETE, we can relax the "no resurrection" rule and allow CAS-from-tombstone safely.
- Optional HEAD to fetch ETag without payload on large values (not needed with small MaxValueBytes default).
- Envelope compression of `value_b64` when payloads approach cap (trade off CPU vs network).
- Batch/list prefetch helpers for GC to reduce GETs prior to DELETE (requires scanning value of `state` without reading full objects; not possible unless we encode state into object metadata or key name).
- Optional checksum of the entire envelope (excluding value) for stronger corruption detection.
```
