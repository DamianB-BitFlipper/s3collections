# s3collections: Durable Work Queue (package queue)

Status: design proposal for implementation. Scope matches docs/design/03-work-queue.md.


Implementation constraints
- Standard library only; no third-party dependencies.
Assumptions about lower layers (package cas, package s3backend)
- s3backend.Backend exposes S3-like primitives with strong read-after-write consistency for GET and LIST, atomic per-key PUT, conditional PUT (If-None-Match: *; If-Match: <etag>), and DELETE (no If-Match). LIST is lexicographic, strongly consistent, paginated.
- cas.Store builds on s3backend and provides CAS on a single key with a monotonic uint64 revision (derived from ETag). It must support:
  - Get(ctx, key) → (value []byte, etag string, rev uint64, err). If the key does not exist, returns ErrNotFound.
  - Create(ctx, key, value) with If-None-Match semantics → (etag, rev, err)
  - UpdateCAS(ctx, key, matchETag string, newValue) with If-Match semantics → (etag, rev, err)
  - Delete(ctx, key) → err
  - List(ctx, prefix, startAfter, limit) → (keys []string, next string, err) — pass-through to backend
- var ErrNotFound = errors.New("not found")
- cas revisions monotonically increase per key; queue uses rev as a fencing token. ETags are opaque; rev is stable uint64 derived by cas.

At-least-once semantics and fencing
- Jobs are delivered at-least-once. Duplicates can happen due to crash after claim, lease expiry, or requeue during contention. Consumers must be idempotent and/or use the provided fencing token (Fence) to ensure exactly-once effects on their own S3 writes.
- Fence is the job’s current cas revision (uint64) at the moment of claim/renew. Downstream writers can store the latest seen fence and reject stale-fenced writes (Guard helper below).

Public Go API (package queue)

Package types and options

- type Queue struct { /* no exported fields */ }
- func New(store cas.Store, name string, opts ...Option) *Queue
- type Option func(*Options)
- type Options struct {
    // Sharding and discoverability
    Shards uint16 // default 256; max 65535. Hash(jobID)%Shards selects shard when not set explicitly.

    // Leases and timing
    DefaultVisibilityTimeout time.Duration // default 30s
    ClockSkewTolerance      time.Duration // default 2s; used when honoring notBefore and determining lease expiry windows

    // Claim scan budgets
    ClaimPageSize int           // default 128 ready markers per LIST page
    ClaimMaxPages int           // default 4 (max ~512 candidates per Claim)
    ClaimShardProbe int         // default min(Shards, 8); number of distinct shards probed per Claim call

    // Background maintenance
    ReaperInterval     time.Duration // default 5s base, jittered
    GCInterval         time.Duration // default 5m base, jittered
    CompletedRetention time.Duration // default 24h
    DeadRetention      time.Duration // default 168h (7d)

    // Identity / observability
    WorkerID string // default random; embedded into leases
    Hooks    Hooks  // optional callbacks
}
- type Hooks struct {
    Enqueue      func(ctx context.Context, ev Event)
    Claim        func(ctx context.Context, ev Event)
    Renew        func(ctx context.Context, ev Event)
    Complete     func(ctx context.Context, ev Event)
    Retry        func(ctx context.Context, ev Event)
    Dead         func(ctx context.Context, ev Event)
    Reap         func(ctx context.Context, ev Event)
    GC           func(ctx context.Context, ev Event)
}
- type Event struct { Queue string; Shard string; JobID string; Attempts int; Err error; Extra map[string]any; Latency time.Duration }

Job API

- type EnqueueOptions struct {
    IdempotencyKey string        // if set → deterministic JobID
    Shard          *uint16       // if set → exact shard; else hash(jobID) % Shards
    Delay          time.Duration // optional; sets notBefore = now + Delay
}
- func (q *Queue) Enqueue(ctx context.Context, payload []byte, opts EnqueueOptions) (jobID string, existed bool, err error)
  Notes:
  - existed == true iff IdempotencyKey was set and the canonical job object already existed (HTTP 412 on create). In that case, Enqueue is a fast read-after-failed-create that returns the existing JobID.

- type ClaimOptions struct {
    VisibilityTimeout time.Duration // overrides default; min 1s
    // shard selection
    RestrictToShards []uint16 // optional; if empty, the caller scans up to ClaimShardProbe random shards
}
- var ErrEmpty = errors.New("queue empty") // returned when no claimable job is found across probed shards
- func (q *Queue) Claim(ctx context.Context, opts ClaimOptions) (*Job, error)

- type Job struct {
    ID        string
    Queue     string
    Shard     uint16
    Payload   []byte
    Attempts  int           // number of prior successful claims; incremented by 1 on this claim before returning
    Fence     uint64        // cas revision returned by the CAS that created/renewed the lease; not stored in the job object
    Lease     Lease         // current lease snapshot
    NotBefore time.Time
    CreatedAt time.Time

    // unexported: store, keys, lastETag, lastRev, options, etc.
}
- type Lease struct {
    Owner  string
    Expiry time.Time
}
- Errors:
  - var ErrStaleLease = errors.New("lease lost or expired; state changed")
  - var ErrNotLeased = errors.New("job not leased by this worker")

Job methods (all idempotent):
- func (j *Job) Renew(ctx context.Context, extendBy time.Duration) error
  Extends the lease (expiry = max(existing, now) + extendBy). Precondition: still claimed by j.Lease.Owner and compare-and-swap on lastETag. On 412, returns ErrStaleLease.

- func (j *Job) Complete(ctx context.Context) error
  Transitions claimed→completed and clears lease. Safe to call multiple times; if already completed/dead, returns nil.

- type RetryOptions struct { Backoff time.Duration; Reason string }
- func (j *Job) Retry(ctx context.Context, ro RetryOptions) error
  Transitions claimed→pending, sets NotBefore = now + Backoff (Backoff may be zero), increments Attempts on next successful claim. Creates/updates ready marker with visibleTS = NotBefore.
  If Options.MaxAttempts > 0 and (Attempts+1) >= Options.MaxAttempts, Retry stores state=dead instead with the provided reason and creates a dead-letter marker. Idempotent across retries.

- func (j *Job) Dead(ctx context.Context, reason string) error
  Transitions claimed→dead, writes a dead-letter marker; safe to call multiple times.

Dead-letter inspection and repair
- type ListDeadOptions struct { StartAfter string; Limit int; Shards []uint16 }
  // StartAfter/next are opaque cursors; callers must treat them as black boxes and pass next back to continue.
  // If Shards is empty, ListDead scans all shards in ascending order (merged by (deadTS, jobID)) using a multi-shard cursor.
  // If Shards has one element, StartAfter refers to a single-shard key suffix "dead/<deadTS>/<jobID>".
- type DeadItem struct { ID string; Shard uint16; When time.Time; Attempts int; Reason string }
- func (q *Queue) ListDead(ctx context.Context, opts ListDeadOptions) ([]DeadItem, string /*next*/, error)
- func (q *Queue) RequeueDead(ctx context.Context, jobID string, shard uint16) error // sets pending NotBefore=now and recreates ready marker; deletes dead marker

Maintenance
- func (q *Queue) StartReaper(ctx context.Context) // reclaims expired leases; cleans stale markers; safe to run on many replicas
- func (q *Queue) StartGC(ctx context.Context)     // deletes completed jobs older than retention; prunes dead-letter beyond DeadRetention; cleans orphan markers

Fencing helper
- var ErrFenceStale = errors.New("stale fence")
- func Guard(latest, provided uint64) error
  Guard returns ErrFenceStale if provided < latest. Downstream users can persist the max seen fence alongside their durable effects and reject stale writes from duplicate deliveries.

S3 key layout

All keys live under a queue namespace and shard directory. We use 4-hex-digit shard IDs for lexicographic locality.
- Canonical job object (the only source of truth for job state):
  queue/<q>/shard/<hhhh>/jobs/<jobID>
- Ready markers for discoverability (pending jobs only; includes notBefore as a sortable prefix):
  queue/<q>/shard/<hhhh>/ready/<visibleTS>/<jobID>
- Lease-expiry markers to find expired leases without scanning all jobs:
  queue/<q>/shard/<hhhh>/lease/<expiryTS>/<jobID>
- Dead-letter markers for inspection:
  queue/<q>/shard/<hhhh>/dead/<deadTS>/<jobID>

Key component formats
- <hhhh> — 4 lower-hex digits (0000..ffff)
- <visibleTS>, <expiryTS>, <deadTS> — fixed 20-byte decimal microseconds since epoch, zero-padded (lexicographically sortable). Example: 00000016922784000000.
- <jobID> —
  - If IdempotencyKey present: hex(sha1(queueName + "|" + idempotencyKey)) (40 hex chars); deterministic across retries. No timestamp prefix is used for idempotent jobs.
  - Else: <ts>-<random32hex>.

Object/envelope format (JSON stored in canonical job object)

Example value at queue/<q>/shard/00af/jobs/<jobID> (either <ts>-<random> or <sha1>)
{
  "id": "20260817T120230.123456Z-5e8c...",
  "queue": "payments",
  "shard": "00af",
  "state": "pending",            // pending | claimed | completed | dead
  "attempts": 0,                  // incremented on successful claim
  "claims": 0,                    // optional counter of total claims (debug/metrics)
  "createdAt": "2026-08-17T12:02:30.123456Z",
  "notBefore": "2026-08-17T12:05:00Z",
  "lease": {                      // present only when state==claimed
    "owner": "worker-7f2a...",
    "expiry": "2026-08-17T12:10:00Z"
  },
  "reasons": [                    // optional, capped list of recent retry reasons (most recent last)
    {"at": "2026-08-17T12:07:00Z", "reason": "timeout"}
  ],
  "payload": "<base64>",         // raw payload bytes base64-encoded
  "completedAt": "2026-08-17T12:25:00Z", // present only when state==completed
  "dead": {                       // present only when state==dead
    "reason": "max attempts exceeded",
    "at": "2026-08-17T12:30:00Z"
  }
}
Notes:
- payload is stored with the envelope to avoid multi-key payload lookups. Recommended max payload size ≤ 256 KiB.
- history is intentionally omitted to keep objects small; observability hooks can emit structured logs instead.

State machine and transitions (single canonical object; markers are hints)

States: pending → claimed → (completed | dead)
Also: claimed → pending (Retry)

- Enqueue (new job):
  1) Create canonical job object in state=pending with notBefore = now + opts.Delay (or createdAt if no delay). Use If-None-Match:"*".
  2) Create ready marker: ready/<visibleTS>/<jobID> where visibleTS = notBefore (microseconds). Use If-None-Match:"*". Creating the marker even when notBefore is in the future is OK — claimers stop when TS > now + ClockSkewTolerance.

- Claim (pending→claimed):
  1) Choose a shard (or several) and LIST ready markers in lexicographic order.
  2) For each ready marker up to ClaimPageSize*ClaimMaxPages:
     - Parse visibleTS. If visibleTS > now + skewTolerance, break this shard (nothing due yet).
     - Derive job key and GET job object via cas.Get to read envelope and etag/rev.
     - If envelope.state != pending or notBefore > now + skewTolerance: opportunistically DELETE the stale marker and continue (but first backfill a missing lease marker if state==claimed; see below).
     - Attempt cas.UpdateCAS(matchETag, mutate to state=claimed, lease={owner, now+visibility}, and increment attempts and claims).
       • On success: set Job fields (Fence=returnedRev, Lease, Attempts++). CREATE a lease marker lease/<expiryTS>/<jobID> (If-None-Match:"*"), then best-effort DELETE the ready marker. Return the Job.
       • On 412 Precondition Failed: another worker won; continue scanning.
  3) If no job claimed across probed shards, return ErrEmpty.

  Opportunistic backfill when encountering stale ready markers:
  - If we observe ready/<ts>/<jobID> but the canonical job state is claimed, we CREATE the lease marker using the lease.expiry from the envelope if it does not exist, then DELETE the stale ready marker. This guarantees that a successful claim will eventually have a lease marker even if the claimer crashed before writing it.

- Renew (claimed→claimed):
  - cas.UpdateCAS(matchETag, extend lease.expiry = max(expiry, now) + extendBy). On success, CREATE a new lease marker for the new expiry (idempotent) and best-effort DELETE the old lease marker. On 412, return ErrStaleLease. Stale lease markers are harmless; reaper will remove them.

- Complete (claimed→completed):
  - cas.UpdateCAS(matchETag, set state=completed, set completedAt=now, and clear lease). Best-effort DELETE lease marker and any stray ready marker. Idempotent: if already completed/dead, return nil. GC will delete the canonical object later.

- Retry (claimed→pending):
  - cas.UpdateCAS(matchETag, clear lease, set state=pending, set notBefore = now + backoff). Best-effort DELETE lease marker; (re)CREATE ready marker with the new visibleTS.

- Dead (claimed→dead):
  - cas.UpdateCAS(matchETag, set state=dead, set dead.reason and dead.at, clear lease). Best-effort DELETE lease marker; CREATE dead-letter marker dead/<nowTS>/<jobID> (If-None-Match:"*").

Idempotency and deterministic JobID

- If EnqueueOptions.IdempotencyKey != "": jobID = hex(sha1(queueName + "|" + idempotencyKey)).
  - We still prefix jobID with creation timestamp for rough FIFO: <ts>-<hash>.
  - Create canonical object with If-None-Match:"*". If 412, treat as successful de-duplication: existed=true and return the same jobID. No duplicate work is produced.
- Idempotency record retention equals job retention: the canonical job object is the record; no separate idempotency table exists.

Leases, visibility timeouts, and clock skew

- Lease = {owner, expiry} in the envelope. Owner is Options.WorkerID. Fence is the cas revision returned by the successful CAS; it is not stored in the envelope.
- Claim sets expiry = now + VisibilityTimeout. Renew extends to max(expiry, now) + extendBy.
- Any replica may reclaim after expiry. Correctness does NOT rely on wall clock synchronization; fencing on CAS ensures old owners cannot write after losing the lease. Clock skew only affects when a new worker tries to reclaim:
  - skewTolerance bounds: if a worker’s clock is ahead by ≤ ClockSkewTolerance, it may attempt to reclaim slightly early; CAS will fail if the old owner renewed based on the canonical object. If behind by ≤ ClockSkewTolerance, a worker might delay reclaim; liveness is preserved by other replicas.

Discoverability: ready markers vs. single-key scans

- Canonical job object holds the truth; marker keys are hints for fast LIST-based discovery.
- We considered two extremes:
  1) State in key: move jobID across ready/leased prefixes. Rejected — DELETE+CREATE across keys is not atomic, risking duplicates orphans.
  2) Single canonical object only; claimers scan jobs/ and filter. Rejected for cost — a Claim would devolve into O(total jobs) GETs under load.
- Selected hybrid: canonical object + ready markers keyed by visibleTS to make LIST cheap, plus lease markers to let reapers find expirations without full scans. Claimers reconcile disagreements by checking the canonical object and opportunistically cleaning stale markers.

Ordering and sharding

- Default mode: sharded rough FIFO. Shards are selected by hash(jobID) % Shards (unless overridden). Within a shard, ready markers are ordered by visibleTS then jobID lexicographically. This gives rough FIFO per shard; cross-shard ordering is not provided.
- Optional strict global ordering mode (low throughput): a single sequencer object queue/<q>/sequencer stores a CAS-incremented uint64. Enqueue CAS-increments to get the next sequence, then uses that sequence in the jobID prefix. Throughput is limited by the single CAS (tens of enqueues/s). This mode is opt-in via Option SequencerEnabled bool.

API details and exact signatures (Go)

Package doc comment

// Package queue provides a durable, S3-backed at-least-once work queue.
// It uses a single canonical job object mutated by CAS and small marker keys for discovery.

Types and constructors

package queue

type Queue struct { /* unexported */ }

type Option func(*Options)

type Options struct {
    Shards               uint16
    DefaultVisibilityTimeout time.Duration
    ClockSkewTolerance   time.Duration
    ClaimPageSize        int
    ClaimMaxPages        int
    ClaimShardProbe      int

    ReaperInterval       time.Duration
    GCInterval           time.Duration
    CompletedRetention   time.Duration
    DeadRetention        time.Duration
    MaxAttempts         int           // 0 = unlimited; if >0, Retry auto-dead-letters when next claim would exceed this
    ReasonHistory       int           // max number of recent retry reasons to keep in envelope (debug)


    SequencerEnabled     bool // optional strict ordering (low throughput)

    WorkerID string
    Hooks    Hooks
}

type Hooks struct {
    Enqueue, Claim, Renew, Complete, Retry, Dead, Reap, GC func(context.Context, Event)
}

type Event struct {
    Queue   string
    Shard   string
    JobID   string
    Attempts int
    Err     error
    Extra   map[string]any
    Latency time.Duration
}

func New(store cas.Store, name string, opts ...Option) *Queue

Enqueue and Claim

type EnqueueOptions struct {
    IdempotencyKey string
    Shard          *uint16
    Delay          time.Duration
}

func (q *Queue) Enqueue(ctx context.Context, payload []byte, opts EnqueueOptions) (jobID string, existed bool, err error)

var ErrEmpty = errors.New("queue empty")

type ClaimOptions struct {
    VisibilityTimeout time.Duration
    RestrictToShards  []uint16
}

func (q *Queue) Claim(ctx context.Context, opts ClaimOptions) (*Job, error)

Job and methods

type Lease struct {
    Owner  string
    Expiry time.Time
}

type Job struct {
    ID        string
    Queue     string
    Shard     uint16
    Payload   []byte
    Attempts  int
    Fence     uint64
    Lease     Lease
    NotBefore time.Time
    CreatedAt time.Time
}

var (
    ErrStaleLease = errors.New("lease lost or expired; state changed")
    ErrNotLeased  = errors.New("job not leased by this worker")
)

func (j *Job) Renew(ctx context.Context, extendBy time.Duration) error

type RetryOptions struct { Backoff time.Duration; Reason string }

func (j *Job) Retry(ctx context.Context, ro RetryOptions) error

func (j *Job) Complete(ctx context.Context) error

func (j *Job) Dead(ctx context.Context, reason string) error

Dead-letter and maintenance

type ListDeadOptions struct { StartAfter string; Limit int; Shards []uint16 }

type DeadItem struct { ID string; Shard uint16; When time.Time; Attempts int; Reason string }

func (q *Queue) ListDead(ctx context.Context, opts ListDeadOptions) ([]DeadItem, string /*next*/, error)

func (q *Queue) RequeueDead(ctx context.Context, jobID string, shard uint16) error

func (q *Queue) StartReaper(ctx context.Context)

func (q *Queue) StartGC(ctx context.Context)

Fencing helper

var ErrFenceStale = errors.New("stale fence")

func Guard(latest, provided uint64) error

Guard usage example (downstream exactly-once on S3)

// store 'fence' alongside your durable effect object and only allow monotonic increases
func putEffect(ctx context.Context, s cas.Store, key string, body []byte, providedFence uint64) error {
    v, etag, rev, err := s.Get(ctx, key)
    if err != nil && !errors.Is(err, cas.ErrNotFound) { return err }
    var meta struct{ Fence uint64; Body []byte }
    if etag != "" {
        if err := json.Unmarshal(v, &meta); err != nil { return err }
        if err := queue.Guard(meta.Fence, providedFence); err != nil { return err }
    }
    meta.Fence = providedFence
    meta.Body = body
    b, _ := json.Marshal(meta)
    if etag == "" {
        _, _, err = s.Create(ctx, key, b)
        return err
    }
    _, _, err = s.UpdateCAS(ctx, key, etag, b)
    return err
}

Claim algorithm in detail (pseudocode)

Claim(ctx, opts):
- choose shards: if RestrictToShards non-empty, use in order; else randomly pick ClaimShardProbe distinct shards per call (remember a rotating start offset locally for fairness)
- for each shard s in chosen order:
  - page through LIST prefix = queue/<q>/shard/<s>/ready/, up to ClaimMaxPages pages, ClaimPageSize each
  - for each key k = ready/<ts>/<jobID>:
    - if ts > now + skewTol: break this shard (future jobs only)
    - GET jobs/<jobID> via cas.Get
      * if state != pending or notBefore > now + skewTol: best-effort DELETE k (stale); continue
      * mutate := set state=claimed, lease={owner, now+VT}, attempts++
      * CAS(UpdateCAS with matchETag) → on success: create lease marker lease/<expiry>/<jobID>, delete ready marker k, return Job
      * on 412: continue to next marker
- if no claim succeeded, return ErrEmpty

Reaper (StartReaper)

Run in every replica; completely idempotent; jittered loop:
- Expired leases:
  - LIST prefix queue/<q>/shard/<s>/lease/ in ascending order; stop when expiryTS > now + skewTol
  - For each marker lease/<ts>/<jobID> with ts <= now + skewTol:
    * GET jobs/<jobID>
    * If state != claimed: stale marker → DELETE marker
    * Else if lease.expiry > now + skewTol: renewed → DELETE marker (stale)
    * Else: expired → CAS to set state=pending, clear lease, keep Attempts as-is; CREATE ready marker ready/<max(now,notBefore)>/<jobID>; DELETE this lease marker
- Stale ready markers:
  - Opportunistically during Claim and Reap passes: when we see ready/<ts>/<jobID> but the job is not pending or its notBefore doesn’t match ts within skewTol, DELETE the ready marker.
- Dead-letter markers:
  - Ensure presence: when encountering state==dead without a dead marker, CREATE dead/<deadTS>/<jobID>. GC will later prune beyond DeadRetention.


No tombstones

- The queue does not use tombstones for completed jobs. The canonical job object remains until GC deletes it after CompletedRetention. Idempotency records are implicit in the canonical object (when IdempotencyKey is used); there is no separate table to GC.

Retries and backoff at the storage layer

- All backend operations use the cas/store’s retry policy with jittered exponential backoff for transient errors (500, SlowDown). Queue state transitions are designed to be idempotent across retries. The queue layer only loops on expected 412 Precondition Failed races.
GC (StartGC)

- Completed jobs:
  - Periodically LIST jobs/ by shard with a small budget (e.g., 1000 keys per run). For each object:
    * GET envelope headers in batches (Get returns value+etag; we need value to see state and CompletedAt/CreatedAt). If state==completed and now - completedAt >= CompletedRetention, DELETE canonical job object. Also DELETE any stray ready/lease markers.
- Dead-letter retention:
  - LIST dead/ markers in ascending order. For deadTS <= now - DeadRetention: GET job to verify state==dead; if so, DELETE canonical job and DELETE dead marker. If job no longer exists, DELETE marker.
- Orphan markers (no matching job): DELETE on sight.


Transient error handling and idempotence of S3 calls

- PUT/UpdateCAS operations are retried in the cas layer with jittered exponential backoff and timeouts derived from context.Deadline. Since UpdateCAS is conditional on ETag, repeated attempts either succeed once or continue to fail with 412 if the state changed; the queue layer treats 412 as contention, not a transient error.
- Marker creation (ready/lease/dead) uses If-None-Match:"*"; retries are safe. Marker deletion is best-effort; failures are tolerated and cleaned later by reaper/GC.
- LIST pagination preserves correctness under retries because S3 provides a continuation token; our interface models this via next cursors. Claim restarts a shard page scan on retry and remains idempotent (it only commits via a single CAS per candidate).
Cost analysis (approximate S3 calls)

- Enqueue (new): 1 PUT (create canonical) + 1 PUT (ready marker). With IdempotencyKey duplicate: 1 failed-create (412) + 0–1 GET to confirm (optional) + 0 marker update (it already exists).
- Claim (hit): ~1 LIST page (ClaimPageSize), 1 GET (job), 1 conditional PUT (CAS), 1 DELETE (ready marker), 1 PUT (lease marker). Misses add: per contended marker, 1 GET and maybe 1 DELETE (stale marker). Upper bound per Claim call is ClaimMaxPages*ClaimPageSize GETs plus one CAS.
- Renew: 1 conditional PUT (CAS) + 1 PUT (new lease marker) + 1 DELETE (old lease marker) — old may be unknown; we attempt delete by name computed from prior expiry; if unknown, reaper cleans stale.
- Complete: 1 conditional PUT (CAS) + best-effort 1 DELETE (lease marker) + 0–1 DELETE (stale ready marker).
- Retry: 1 conditional PUT (CAS) + 1 PUT (ready marker) + 1 DELETE (lease marker).
- Dead: 1 conditional PUT (CAS) + 1 PUT (dead marker) + 1 DELETE (lease marker).
- Reaper per expired lease: 1 LIST page amortized + 1 GET + 1 CAS + 1 PUT (ready) + 1 DELETE (lease). Stale markers add 1 GET + 1 DELETE.
- GC: bounded by configured budgets; primarily LIST + GET + DELETE for aged items.

Scalability limits and rough numbers

- Claim throughput per shard is bounded by LIST page size and CAS PUT throughput. With ClaimPageSize=128 and average 10% contention, each worker can claim O(100–300) jobs/s if S3 latency is ~10–30 ms and payloads are small.
- Shards: up to 65,535; practical shard count 256–4096. More shards reduce hot-spotting on ready/ prefix but increase GC/reaper scan work.
- Hot-shard mitigation: choose shard = hash(jobID) unless caller pins a shard; add ClaimShardProbe >1 to spread claim load across shards. If one shard is hot, workers will still find work in other shards.
- Optional strict ordering (sequencer) caps enqueue to O(tens) per second due to single-key CAS.
- Marker cardinality: at steady state, ready markers ~= visible pending jobs; lease markers ~= currently leased jobs. Markers are tiny (zero-byte bodies). LIST cost dominates when queues are near-empty; use shard probes and jittered retries.

Failure mode analysis (crash between any two S3 calls)

- Enqueue crash after canonical create but before ready marker: job exists but not discoverable. Recovery: reaper pass or Claim opportunistic cleanup notices missing marker when scanning jobs opportunistically (optional) — to guarantee prompt discoverability, Enqueue retries ready marker creation on next identical Enqueue (idempotent) and Reaper can periodically scan a small budget of recent jobs to backfill missing ready markers.
- Claim crash before CAS: nothing changes.
- Claim crash after CAS (claimed) but before ready marker DELETE: stale ready marker remains. Other claimers GET envelope, see state=claimed and skip; they opportunistically CREATE the lease marker (if missing) from the envelope and then DELETE the stale ready marker. Reaper will find the lease via the marker.
- Claim crash after CAS and after lease marker create but before ready marker DELETE: two markers exist; claimers skip due to state=claimed; Reaper or claimers DELETE stale ready marker.
- Renew crash after CAS but before old lease marker DELETE: duplicate lease markers exist; Reaper sees ts<=now marker, GETs job, sees lease renewed (expiry in future), and DELETEs stale marker.
- Complete crash after CAS but before lease marker DELETE: stale lease marker; Reaper deletes it on sight (state!=claimed).
- Retry crash after CAS but before ready marker create: job is pending but missing ready marker; Reaper/Claim safety sweep backfills marker.
- Dead crash after CAS but before dead marker create: job is dead but missing marker; Reaper backfills marker when it touches the job, and GC can still prune based on canonical state by budgeted scans.
- DELETE is not conditional; accidental deletes must be guarded by CAS on the canonical object state first; library only performs DELETEs on markers and on canonical objects during GC.
- Clock skew: bounded by ClockSkewTolerance; may cause early/late visibility but never violates mutual exclusion due to CAS fencing.
- Reaper stampede: multiple StartReaper instances run with jittered intervals and per-shard backoff. Markers make each action idempotent; work naturally partitions by shard prefix.
- Poison messages: attempts grow on each claim; after user-defined policy (outside minimal API) a worker may call Dead; Reaper does not auto-dead-letter.


Reason history storage

- The envelope keeps a small capped slice reasons[] of {at, reason}. On each Retry or Dead, we append the latest reason and then cap to Options.ReasonHistory (default 8). History is for observability and not required for correctness.

Retry/backoff policy

- attempts increments on each successful claim. Workers should choose backoff as a function of Attempts: e.g., base * 2^(Attempts-1) with jitter. The library does not enforce a policy; Retry accepts a Backoff duration. notBefore is set to now+Backoff; ready marker carries that visibleTS and prevents premature claims (modulo skew tolerance).

Dead-letter inspection

- ListDead lists dead/<ts>/<jobID> markers in ascending ts, across requested shards, returning DeadItem with Reason from the canonical job object. RequeueDead CASes dead→pending, sets notBefore=now, recreates ready marker, and deletes the dead marker. If the job was already requeued or completed, RequeueDead is a no-op and ensures the dead marker is deleted.

Test plan

Unit tests (with in-memory S3 backend and fault-injecting wrapper)
- Enqueue
  - new job: creates canonical + ready marker; idempotency with the same key returns existed=true and does not create duplicates
  - delay: ready marker TS is notBefore; Claim breaks correctly on future TS when skewTol small
- Claim
  - single worker, pending → claimed; marker deleted opportunistically; lease marker created
  - contention: N workers race; exactly one CAS succeeds; losers continue; no duplicate delivery
  - near-empty queue: Claim returns ErrEmpty without excessive GETs beyond budgets
- Renew
  - extends expiry; stale renew (after steal) returns ErrStaleLease
- Complete
  - idempotent; multiple calls ok; no residual markers after reaper
- Retry
  - claimed → pending with future notBefore; ready marker reflects new TS; attempts do not increment until next claim
- Dead
  - claimed → dead; dead marker present; ListDead shows it; RequeueDead re-enqueues

Concurrency-stress
- 32 workers, 8 shards, 1e5 small payload jobs
  - validate at-least-once: count total claims >= total completes; no job completes twice (idempotent Complete)
  - validate fence monotonicity under duplicate deliveries by simulating downstream effects guarded by Guard

Chaos/fault injection
- Inject 500s/SlowDown on PUT/GET/LIST/DELETE; ensure retries with exponential jittered backoff (in cas layer); ensure queue operations remain idempotent
- Force 412 races at high rates; ensure correctness (no lost or duplicated completes)
- Crash points between each pair of S3 calls (simulate by toggling a chaos hook) to validate recovery behaviors listed above

Reaper/GC tests
- Expired leases reclaimed within bounded time; ready markers recreated when missing; stale markers cleaned
- GC removes completed jobs after retention; dead-letter pruned after retention; orphan markers deleted; no accidental deletes of non-completed/non-dead jobs

Fuzz/property tests
- Fuzz JSON envelope encode/decode round-trips under random payloads and reason strings
- Property: for any execution, the canonical object’s state machine only follows allowed transitions

Open edges and parameter defaults
- Choose ClockSkewTolerance default 2s; document that real-world S3 can have sub-second clock skew across clients; 2s is conservative and only impacts visibility, not correctness
- Recommended shard count 256 for 10k–100k qps enqueue/claim workloads; larger for very high concurrency
- Payload size limits: enforce ≤ 256 KiB in Enqueue to keep PUTs fast (documented behavior)

Examples

Enqueue and worker loop (illustrative only; not full code)

q := queue.New(store, "payments")
_, _, _ = q.Enqueue(ctx, payload, queue.EnqueueOptions{IdempotencyKey: orderID})
for {
    job, err := q.Claim(ctx, queue.ClaimOptions{VisibilityTimeout: 45 * time.Second})
    if errors.Is(err, queue.ErrEmpty) { time.Sleep(time.Second); continue }
    if err != nil { log.Printf("claim error: %v", err); continue }

    // do work using job.Payload, pass job.Fence to downstream writes
    if err := process(job.Payload, job.Fence); err != nil {
        _ = job.Retry(ctx, queue.RetryOptions{Backoff: backoffFor(job.Attempts+1), Reason: err.Error()})
        continue
    }
    _ = job.Complete(ctx)
}

