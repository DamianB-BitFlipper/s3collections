# s3collections Architecture and Cross‑Cutting Standards (v1)

Status: draft-v1 (module github.com/damianb/s3collections; Go 1.26)
Scope: repo-wide architecture, invariants, APIs, and standards that all packages (s3backend, cas, lru, queue) must follow.
Assumptions: Only the S3 semantics listed in the project brief are available; no third-party deps; testability against in-memory S3 with strong consistency plus chaos.

0. Overview and goals
- Multiple stateless replicas share durable state solely via S3 keys/objects.
- All correctness arguments reduce to per-key linearizable operations driven by S3 conditional PUT and strong read-after-write for GET/LIST.
- Cross-key invariants are best-effort and repaired by background processes; never rely on multi-key atomicity.
- Every public operation is idempotent, context-aware, instrumented, and retried with jittered backoff when safe.
- Deletion safety: unconditional DELETE is safe only for append-only, never-reused keys. For any reusable logical key, prefer tombstones/epochs instead of DELETE (details in §6).


1. Consistency model and vocabulary
1.1 Per-key linearizability
- S3 guarantees per-key atomic PUT, strong read-after-write for GET/LIST, and conditional PUT with If-None-Match: * and If-Match: <etag> that returns 412 on precondition failure.
- We build read-modify-write (RMW) loops as: GET (or HEAD) → compute new value → PUT with If-Match oldETag (or If-None-Match for create). Exactly one concurrent writer can succeed; subsequent GETs/LISTs reflect the last successful write. This yields linearizable semantics for that key.
- We never infer ordering from ETag values. ETags are opaque. We use an explicit body field rev that we increment on every successful CAS to derive fencing tokens within a key.

1.2 CAS envelope (shared body contract)
All reusable logical keys (e.g., CAS “head” objects, lock holders, LRU index nodes, queue head/tail) store a stable JSON envelope that carries revision and optional semantics:

Example JSON body (UTF-8, LF):
{
  "kind": "s3collections:cas:v1",
  "rev": 42,
  "deleted": false,
  "owner": "replica-8f2c3a",
  "lease": {                // optional, see §3.4 clock policy
    "holder": "replica-8f2c3a",
    "notAfterUnixMillis": 1735689600000
  },
  "payload": {              // package-specific; MUST be JSON object if present
    "...": "..."
  }
}

Rules:
- rev: monotonic per key; writers set new.rev = old.rev + 1 when If-Match succeeds. Readers must treat missing rev (legacy) as 0 and set to 1 on first write.
- deleted: logical tombstone. If true, the logical record is absent. Writers MUST NOT resurrect a deleted record without incrementing rev and explicitly clearing deleted=false via CAS. Physical DELETE is optional and constrained (§6).
- lease: best-effort liveness hints only; correctness must not rely on wall clocks. See §3.4 for skew margins.
- payload: component-owned JSON; MUST be deterministic for idempotency where applicable.

1.3 Cross-key invariants are best-effort-with-repair
- S3 has no multi-key transactions and no conditional DELETE; therefore any operation spanning multiple keys is subject to torn updates on crash between steps.
- We state two categories of invariants:
  • Hard per-key invariants: linearizable by CAS on a single key (e.g., a queue shard’s head pointer, a lock holder).
  • Soft cross-key invariants: maintained optimistically and periodically repaired (e.g., “every queue item is indexed by shard lists”).
- Compensation patterns used across packages:
  • Write-intent + finalize: write item under unique content-addressed or UUID key first (immutable), then update index/head pointer by CAS; reapers remove orphan intents.
  • Index-from-log: maintain authoritative append-only records (never reused key names) and reconstruct/repair secondary indexes via LIST+GET scans.
  • Tombstone-first deletion: mark logical delete by CAS setting deleted=true on head; physical DELETE only for never-reused keys.
  • Idempotency tokens: include a client-supplied token in payload; repeated attempts produce the same state.

1.4 S3 key layout vocabulary (versioned)
- Every object managed by this library lives under a versioned root prefix: <root>/v1/<component>/<collection>/...
- Common sharding: <root>/v1/<component>/<collection>/shard-<NNNN>/...
- Never-reused (append-only) objects include a monotonic suffix such as seq-<zero-padded> or a UUID: e.g., .../log/seq-000000000123.json.
- Reusable “head” objects that require CAS use a stable path: .../heads/<keyHash>/<escapedKey>. These are never physically deleted (tombstone instead), unless a new epoch path is chosen.


2. Strict global ordering vs scalable sharded operation
2.1 Single-sequencer (strict global order)
- Design: a single CAS head key whose rev provides a total order for all operations. Each operation increments rev and appends an immutable event at .../log/seq-<rev>.json.
- Pros: simple, total order, easy fencing.
- Cons: hot single key → throughput limited by S3 RTT. One RMW attempt is GET+PUT; with 80–150 ms RTT typical, max steady-state is roughly 4–10 successful ops/sec per key with no contention. Under contention, expected attempts/op increases, further lowering throughput.
- When to use: operations requiring strict total order and very low rate (administrative or reconfiguration paths).

2.2 Sharded ordering (scalable)
- Design: M shards, each with its own CAS head and append-only log. Shard chosen by stable hash of user key or round-robin.
- Guarantees: per-shard linearizable order; cross-shard only a partial order. Consumers MAY merge logs by wall-time as heuristic but must tolerate reordering.
- Throughput: scales ≈ linearly with shard count assuming uniform distribution. If a single shard becomes hot, it inherits single-key limits.
- Required documentation: every component that claims “ordered” or “unique” MUST state whether it uses a global sequencer or per-shard order, its shard count and mapping function, and the consequences for consumers.


3. Repo-wide conventions
3.1 Public API shape and options pattern
- All exported constructors take context and an options struct; options are value types with sensible zero/defaults.
  Example:
  type CommonOptions struct {
      RootPrefix string          // required; e.g., "myapp/prod"
      Metrics    Meter           // optional; nil = no-op
      Logger     Logger          // optional; nil = no-op
      Tracer     Tracer          // optional; nil = no-op
      Retry      RetryPolicy     // optional; zero = DefaultRetry()
      MaxAttempts int            // RMW attempt cap per logical op; 0=default (e.g., 8)
      ClockSkewTolerance time.Duration // 0 = default 1m. Heuristic only.
  }
  Constructors embed/compose CommonOptions and add their own:
  func NewClient(ctx context.Context, b s3backend.Backend, opts CommonOptions) (*Client, error)

- Methods: first parameter is context.Context; they return (T, error) with structured errors (see §3.2). Methods MUST be idempotent with respect to retries/timeouts.
- No global state; all dependencies (backend, logger, metrics) are injected via options.

3.2 Error taxonomy (errors.Is/As friendly)
- Canonical error type with code and unwrap:
  type Code string
  const (
      ErrNotFound       Code = "not_found"
      ErrAlreadyExists  Code = "already_exists"
      ErrPrecondition   Code = "precondition_failed" // 412 on CAS
      ErrConflict       Code = "conflict"            // higher-level semantic conflict
      ErrThrottled      Code = "throttled"           // SlowDown/503
      ErrInternal       Code = "internal"            // 5xx other
      ErrCanceled       Code = "canceled"
      ErrDeadline       Code = "deadline_exceeded"
      ErrCorrupt        Code = "corrupt"             // bad JSON, incompatible kind
      ErrIncompatible   Code = "incompatible"        // version/kind mismatch
  )
  type Error struct {
      Op   string // e.g., "cas.Get", "queue.Enqueue"
      Key  string // optional S3 key
      Code Code
      Err  error
  }
  func (e *Error) Error() string
  func (e *Error) Unwrap() error

- Sentinel helpers:
  var (
      ErrIsNotFound      = &Error{Code: ErrNotFound}
      ErrIsAlreadyExists = &Error{Code: ErrAlreadyExists}
      ErrIsPrecondition  = &Error{Code: ErrPrecondition}
      // ... used with errors.Is(err, ErrIsNotFound)
  )

- Mapping policy:
  • S3 404 → ErrNotFound (GET); for create-if-absent with If-None-Match, 412 means already exists → ErrAlreadyExists (not retryable).
  • 412 on If-Match → ErrPrecondition (retryable within attempt budget).
  • 429/503/SlowDown/5xx → ErrThrottled/ErrInternal (retryable with backoff).
  • Context cancellation/deadline → ErrCanceled/ErrDeadline (not retried).
  • JSON/kind mismatch → ErrCorrupt/ErrIncompatible (not retried, escalate).

3.3 Retry/backoff standard
- RMW loops retry on ErrPrecondition, ErrThrottled, ErrInternal until MaxAttempts or context deadline. Per-attempt jittered backoff is required.
- Default policy (decorrelated jitter per AWS):
  type RetryPolicy struct {
      Base    time.Duration // default 50ms
      Cap     time.Duration // default 2s
      Max     int           // default 8 attempts per logical op
  }
  func DefaultRetry() RetryPolicy { return RetryPolicy{Base:50*time.Millisecond, Cap:2*time.Second, Max:8} }
  Backoff(next): sleep = rand[Base, min(Cap, prev*3)] with full jitter. Always add small random [−5ms,+5ms] to avoid phase-locking.
- CAS conflicts: record attempts in metric s3collections_cas_attempts and log at warn if >50% of attempts fail or attempts > Max/2.

3.4 Clock usage policy
- Wall clocks are used only for TTL/lease heuristics and metrics timestamps; correctness (mutual exclusion, fencing) must use rev and CAS within a single key.
- Default assumed skew tolerance: ±1 minute. Any expiry checks MUST add a safety margin ≥ skewTolerance. Writers set conservative notAfter = now + ttl - skewTolerance; readers treat expiry as: expired if now - skewTolerance > notAfter.
- Never rely on “time order” across keys.

3.5 Context handling
- All public operations accept a caller context. We never create background goroutines without being owned by a Start(ctx) style method or a constructor that returns a closer.
- Honor ctx.Done() quickly in list/scan loops. If a partial multi-step op is canceled mid-flight, rely on idempotency or compensation to remain safe.

3.6 API stability and on-disk format versioning
- Module follows semver. During v0, minor versions may break; from v1 onward, public APIs and on-disk (S3) formats are compatible within a major.
- All S3 paths are versioned under /v1/. Future incompatible changes use /v2/ roots to allow side-by-side migration.
- JSON envelope kind fields carry a version string; readers MUST verify kind and fail with ErrIncompatible if unexpected.


4. Observability standard
4.1 Metrics interfaces (stdlib-only)
  type Label struct{ Key, Value string }
  type Meter interface {
      IncCounter(ctx context.Context, name string, delta float64, labels ...Label)
      ObserveHistogram(ctx context.Context, name string, value float64, labels ...Label)
      SetGauge(ctx context.Context, name string, value float64, labels ...Label)
  }
- Implementations may adapt to Prometheus/OTel in user code by translating calls to their SDKs. A no-op Meter is provided by default.

4.2 Logger interface (leveled, structured)
  type Logger interface {
      Debug(msg string, kv ...any)
      Info(msg string, kv ...any)
      Warn(err error, msg string, kv ...any)
      Error(err error, msg string, kv ...any)
      With(kv ...any) Logger
  }
- Adapters: users can wrap log/slog or their logger by implementing this interface.

4.3 Tracing hooks (optional)
  type Span interface {
      End(err error)
      AddEvent(name string, attrs ...Label)
  }
  type Tracer interface {
      StartSpan(ctx context.Context, name string, attrs ...Label) (context.Context, Span)
  }
- We do not ship an OTel client; users may adapt.

4.4 Required metric names and labels
All components MUST emit the following (names are stable):
- s3collections_latency_seconds (histogram): labels {component, op, outcome} — total logical op latency.
- s3collections_cas_attempts (histogram): labels {component, op} — number of CAS attempts per logical op.
- s3collections_conflicts_total (counter): labels {component, op} — count of 412 preconditions.
- s3collections_retries_total (counter): labels {component, op, reason} — retry loops entered.
- s3collections_list_pages_total (counter): labels {component, op, prefix} — LIST page fetches.
- s3collections_s3_calls_total (counter): labels {op in [get, put, list, delete]} — raw backend calls (optional if backend already instruments).
- s3collections_reaper_runs_total (counter): labels {component, shard}
- s3collections_reaper_tombstones_deleted_total (counter): labels {component}

4.5 Required logging points
- At Info: successful Start/Stop of background loops; shard counts and root prefixes at constructor; periodic reaper summary (every N minutes) with scanned, candidates, deleted.
- At Warn: CAS hot-spotting (attempts > Max/2), repeated throttling, partial repair actions.
- At Error: attempt budget exhausted, JSON/corrupt body, incompatible kind, and any logical invariant violation.
- Debug: per-attempt details may be logged behind sampling or when ctx carries a debug flag.


5. Testing strategy
5.1 In-memory backend contract
- Must emulate exactly the allowed S3 semantics:
  • Strong read-after-write for GET and LIST with lexicographic, paginated LIST.
  • Per-key atomic PUT with If-None-Match and If-Match preconditions; failed preconditions produce 412.
  • DELETE removes the key unconditionally.
  • ETags are opaque values but monotonically increment per key in the in-memory impl (for easier testing); production code MUST NOT depend on ordering of ETags.

5.2 Fault-injection wrapper (chaos)
- A wrapper around s3backend.Backend that can inject:
  • 5xx errors (InternalError), SlowDown/503, random 412s, and added latency (fixed + random jitter) on any operation filtered by prefix/op.
  • Bursty modes: fail p% of calls in windows.
  • Deadline stretching: sleep beyond ctx deadlines to test cancellation.
- API sketch:
  type ChaosRule struct {
      Op         string  // "get", "put", "list", "delete"
      Prefix     string  // match S3 key hasPrefix
      Probability float64
      Inject     func() error // return wrapped *s3backend.Error or context.DeadlineExceeded
      Latency    time.Duration // added per call
  }
  type ChaosBackend struct{ Base s3backend.Backend; Rules []ChaosRule }

5.3 Concurrency-stress harness
- Pattern: spawn N replicas (goroutines) sharing the same Backend and options; each runs operation loops (RMW on hot key(s), enqueue/dequeue items) under time budget. Validate invariants after each op and at the end via LIST+GET scans.
- Always run with -race. CI: go test -race ./...

5.4 Fuzzing
- Targets:
  • FuzzCASEnvelopeRoundTrip: arbitrary payload maps; ensure JSON round-trip and rev/tombstone semantics preserved.
  • FuzzKeyLayout: random roots/collection/shard/key inputs; ensure escaping and lexicographic order properties hold.
  • FuzzBackoff: ensure backoff never exceeds cap and exhibits jitter.

5.5 Integration tests (opt-in)
- Build-tagged with `//go:build s3integration`. Require env vars: S3_ENDPOINT, S3_REGION, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_FORCE_PATH_STYLE=true/false.
- Use a unique root per run: root := fmt.Sprintf("it/%s/", time.Now().UTC().Format(time.RFC3339Nano)). Tests must clean append-only objects they create; reusable heads should remain tombstoned, not physically deleted.


6. GC/reaper shared pattern
6.1 Principles
- Safety over reclamation: never physically DELETE a key that may be concurrently reused under the same name. Prefer logical tombstones.
- Reapers are optional background loops started by the user (e.g., client.StartReaper(ctx)). All replicas may run reapers concurrently; operations must be idempotent.
- Avoid LIST storms: spread work across time and shards; cap QPS per prefix; randomize start offsets.

6.2 Safe deletion taxonomy
- Category A (safe DELETE): append-only, never-reused keys (e.g., log segments named by increasing seq or UUID). DELETE may race with nothing because names are immutable by design.
- Category B (unsafe DELETE): reusable head keys (CAS-managed). MUST NOT be physically deleted unless retired to a new epoch path. Use tombstones (deleted=true). Readers treat tombstones as absence.
- Category C (two-phase retire): if physical deletion of a reusable logical entity is desired, move to a new epoch path first:
  • Step 1: CAS mark old head deleted=true.
  • Step 2: Publish new head at new path (<...>/epoch-<E>/...). All new readers switch by configuration or indirection key (itself a head key).
  • Step 3: Reaper may later DELETE old path’s append-only children (Category A) and leave the tombstoned head (Category B) or, if absolutely necessary, retain tombstone forever to avoid ABA.

6.3 Reaper loop sketch
  type ReaperOptions struct {
      Prefix       string        // root + component + collection
      PageSize     int           // default 1000
      TargetQPS    float64       // default 1.0 per prefix
      Jitter       time.Duration // default ±250ms
      MaxWorkers   int           // default 1–4
  }
  func (c *Client) StartReaper(ctx context.Context, ro ReaperOptions) (stop func())
- Algorithm:
  • For each shard prefix, LIST in lexicographic pages, honoring PageSize.
  • For each candidate object, GET head/body and decide action using component logic.
  • For Category A, DELETE directly.
  • For Category B, skip physical delete; optionally compact large payloads to a minimal tombstone body via CAS.
  • Sleep between pages to meet TargetQPS with jitter; randomize starting shard and page to avoid herds.
- Leader-avoidance: opportunistic ephemeral advisory lock is allowed (a separate head key with short lease) ONLY to reduce duplicate GC work. Correctness must never depend on it.

6.4 Clock skew in GC
- Any expiry fields (e.g., item.notAfter) are evaluated with ±skewTolerance margin (§3.4). Reapers must double-check candidates after the sleep; if close to boundary, defer.


7. Package dependency rules and docs outline
7.1 Dependency DAG
- s3backend: lowest layer; defines Backend interface, in-memory impl, chaos wrapper, and (later) HTTP+SigV4 client. No imports from other repo packages.
- cas: depends only on s3backend and the root package (for CommonOptions, errors, observability).
- lru: depends on cas (and root for options/errors/obs).
- queue: depends on cas (and root for options/errors/obs).
- internal/: allowed for unexported helpers (retry, json, key-escapes); internal must not import public packages to avoid cycles.
- No circular deps.

7.2 Public API namespaces (sketch)
- package s3collections: common types and options
  type CommonOptions ...
  type Error/Code ...
  type Meter/Logger/Tracer/Label ...

- package s3backend: Backend and implementations
  type Backend interface {
      Get(ctx context.Context, key string) (etag string, body []byte, err error)
      Head(ctx context.Context, key string) (etag string, size int64, err error)
      Put(ctx context.Context, key string, body []byte, cond *PutCond) (etag string, err error)
      List(ctx context.Context, prefix, cursor string, limit int) (keys []string, nextCursor string, err error)
      Delete(ctx context.Context, key string) error
  }
  type PutCond struct { IfNoneMatch bool; IfMatchETag string }
  // Errors from Backend MUST be wrapped into s3collections.Error by higher layers.

- package cas: foundational per-key CAS store using the envelope
  // API sketched in 01-cas.md; exposes Get/PutCAS/RMW helpers on head keys and
  // append-only primitives for logs.

- package lru, package queue: build on cas, document sharding/order choices explicitly in their design docs.

7.3 docs/ outline
- docs/design/00-architecture.md (this doc): architecture & standards.
- docs/design/01-cas.md: CAS store design (envelope usage, key layout, RMW API, fencing tokens, compaction, costs, tests).
- docs/design/02-lru.md: Distributed LRU metadata store (sharding, eviction protocol, conflict handling, GC).
- docs/design/03-queue.md: Durable work queue (enqueue/dequeue/ack, visibility, shard mapping, at-least-once guarantees, repair scans, costs, tests).
- docs/design/90-testing.md: Test harnesses, chaos profiles, integration guidance.
- docs/design/99-operations.md: Tuning, metrics SLOs, common failure playbooks.


Appendix A: RMW attempt state machine
States:
- Start(op): record t0, attempts=0
- Read: GET/HEAD key; if 404 and op=create → attempt PUT If-None-Match; if 404 and op=update → return ErrNotFound.
- Compute: fn(oldEnvelope) → newEnvelope, idempotent w.r.t token in payload (optional).
- Write: PUT with If-Match oldETag (or If-None-Match for create). If 412 → Conflict; increment attempts; backoff; goto Read. If 5xx/SlowDown → backoff and retry. If success → Done.
- Done: return newEnvelope, newETag.

Cost model (amortized):
- Zero-contention: 1 GET + 1 PUT per successful op. With RTT 100ms → ~10 ops/sec/key upper bound.
- With k concurrent writers: expected attempts/op ~ geometric with p≈1/k; practical throughput falls roughly inversely with k; prefer sharding for k>2.

Appendix B: Key escaping and lexicographic constraints
- Shard and seq components are zero-padded to fixed width to preserve lexicographic time order.
- User-supplied IDs in paths must be escaped to avoid '/'. We use URL path escaping for path segments. The escaping function MUST be stable and reversible.

Appendix C: Example adapters (sketch only)
// Prometheus (pseudocode)
type PromAdapter struct{ r *prom.Registry }
func (p *PromAdapter) IncCounter(ctx context.Context, name string, d float64, l ...s3collections.Label) { /* map name+labels to prom.CounterVec */ }
// OTel tracer adapter wraps otel.Tracer into Tracer interface.

Open items tracked in this doc’s summary.
