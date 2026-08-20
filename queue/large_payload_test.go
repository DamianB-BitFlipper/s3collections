package queue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
	"github.com/damianb/s3collections/tree"
)

func TestLargePayloadRoundTripAndMetadataOnlyTransitions(t *testing.T) {
	q, mem, _ := testQueue(t, "large-roundtrip", WithInlinePayloadBytes(8), WithMaxPayloadBytes(1<<20))
	ctx := context.Background()
	data := bytes.Repeat([]byte("abcdefgh"), 4096)
	id, existed, err := q.EnqueueReader(ctx, bytes.NewReader(data), int64(len(data)), EnqueueOptions{IdempotencyKey: "large"})
	if err != nil || existed {
		t.Fatalf("enqueue %v %v", existed, err)
	}
	shard := shardForJob(id, q.opts.Shards)
	rec, err := q.store.Get(ctx, jobAppKey(shard, id))
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Value) > 16<<10 {
		t.Fatalf("CAS envelope too large: %d", len(rec.Value))
	}
	env, _ := decodeJob(rec.Value)
	if env.PayloadRef == nil || env.LegacyPayload != "" {
		t.Fatalf("wire=%#v", env)
	}
	job, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if job.Payload != nil {
		t.Fatalf("external payload eagerly loaded: %d", len(job.Payload))
	}
	if job.PayloadSize != int64(len(data)) {
		t.Fatalf("size=%d", job.PayloadSize)
	}
	r, err := job.OpenPayload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("open len=%d err=%v", len(got), err)
	}
	if err = job.Renew(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = job.Retry(ctx, RetryOptions{}); err != nil {
		t.Fatal(err)
	}
	job, err = q.Claim(ctx, ClaimOptions{DeferPayload: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = job.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	// Raw blob exists once under the isolated payload prefix.
	page, err := mem.List(ctx, q.prefix+"payloads/", nil)
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("payload objects=%d err=%v", len(page.Objects), err)
	}
}

func TestEnqueueAutoOffloadsAndInlineCompatibility(t *testing.T) {
	q, _, _ := testQueue(t, "auto-offload", WithInlinePayloadBytes(4), WithMaxPayloadBytes(1024))
	ctx := context.Background()
	small := []byte("1234")
	if _, _, err := q.Enqueue(ctx, small, EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	j, err := q.Claim(ctx, ClaimOptions{})
	if err != nil || !bytes.Equal(j.Payload, small) {
		t.Fatalf("inline=%q %v", j.Payload, err)
	}
	large := []byte("12345")
	if _, _, err = q.Enqueue(ctx, large, EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	j, err = q.Claim(ctx, ClaimOptions{})
	if err != nil || j.Payload != nil {
		t.Fatalf("external payload=%v err=%v", j.Payload, err)
	}
	r, err := j.OpenPayload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(got, large) {
		t.Fatalf("got=%q", got)
	}
}

func TestEnqueueReaderExactLengthAndZero(t *testing.T) {
	q, _, _ := testQueue(t, "reader-length", WithInlinePayloadBytes(1), WithMaxPayloadBytes(1024))
	ctx := context.Background()
	if _, _, err := q.EnqueueReader(ctx, strings.NewReader("short"), 6, EnqueueOptions{}); err == nil {
		t.Fatal("short accepted")
	}
	if _, _, err := q.EnqueueReader(ctx, strings.NewReader("long"), 3, EnqueueOptions{}); err == nil {
		t.Fatal("long accepted")
	}
	if _, _, err := q.EnqueueReader(ctx, bytes.NewReader(nil), 0, EnqueueOptions{}); err != nil {
		t.Fatalf("zero: %v", err)
	}
	j, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := j.OpenPayload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if len(got) != 0 {
		t.Fatalf("zero len=%d", len(got))
	}
}

func TestSameWorkerStaleClaimCannotMutateReclaim(t *testing.T) {
	q, _, clk := testQueue(t, "claim-token", WithDefaultVisibilityTimeout(time.Second))
	ctx := context.Background()
	if _, _, err := q.Enqueue(ctx, []byte("p"), EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	old, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	q.reaperPass(ctx)
	fresh, err := q.Claim(ctx, ClaimOptions{VisibilityTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if old.Lease.Owner != fresh.Lease.Owner || old.Lease.Token == fresh.Lease.Token {
		t.Fatalf("leases old=%#v new=%#v", old.Lease, fresh.Lease)
	}
	if err = old.Complete(ctx); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale complete=%v", err)
	}
	if err = fresh.Complete(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentIdempotentLargeEnqueue(t *testing.T) {
	q, _, _ := testQueue(t, "large-idem", WithInlinePayloadBytes(1), WithMaxPayloadBytes(1024))
	ctx := context.Background()
	const n = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[string]bool{}
	success := 0
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, _, err := q.EnqueueReader(ctx, strings.NewReader("same payload"), 12, EnqueueOptions{IdempotencyKey: "same"})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
				ids[id] = true
			}
		}()
	}
	wg.Wait()
	if success == 0 || len(ids) != 1 {
		t.Fatalf("success=%d ids=%v", success, ids)
	}
	j, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := j.OpenPayload(ctx)
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "same payload" {
		t.Fatalf("got=%q", got)
	}
}

// Ensure wrappers used by large payloads preserve backend capability.
var _ s3backend.Backend = (*s3backend.Memory)(nil)
var _ = s3collections.DefaultRetry

type countingBackend struct {
	*s3backend.Memory
	mu                 sync.Mutex
	blobGets, blobPuts int
}

func (c *countingBackend) Get(ctx context.Context, key string) (*s3backend.Object, error) {
	if strings.Contains(key, "/b/") {
		c.mu.Lock()
		c.blobGets++
		c.mu.Unlock()
	}
	return c.Memory.Get(ctx, key)
}
func (c *countingBackend) Put(ctx context.Context, key string, b []byte, p *s3backend.Preconditions) (string, error) {
	if strings.Contains(key, "/b/") {
		c.mu.Lock()
		c.blobPuts++
		c.mu.Unlock()
	}
	return c.Memory.Put(ctx, key, b, p)
}
func (c *countingBackend) GetStream(ctx context.Context, key string) (*s3backend.StreamObject, error) {
	if strings.Contains(key, "/b/") {
		c.mu.Lock()
		c.blobGets++
		c.mu.Unlock()
	}
	return c.Memory.GetStream(ctx, key)
}
func (c *countingBackend) PutStream(ctx context.Context, key string, r io.Reader, n int64, p *s3backend.Preconditions) error {
	if strings.Contains(key, "/b/") {
		c.mu.Lock()
		c.blobPuts++
		c.mu.Unlock()
	}
	return c.Memory.PutStream(ctx, key, r, n, p)
}
func (c *countingBackend) PutMultipart(ctx context.Context, key string, r io.Reader, n int64, p *s3backend.Preconditions) error {
	if strings.Contains(key, "/b/") {
		c.mu.Lock()
		c.blobPuts++
		c.mu.Unlock()
	}
	return c.Memory.PutMultipart(ctx, key, r, n, p)
}
func (c *countingBackend) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.blobGets, c.blobPuts
}
func TestExternalTransitionsDoNotTouchBlobData(t *testing.T) {
	ctx := context.Background()
	b := &countingBackend{Memory: s3backend.NewMemory()}
	clk := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	q, err := New(b, "no-rewrite", func(o *Options) { o.now = clk.Now; o.WorkerID = "same" }, WithShards(1), WithInlinePayloadBytes(1), WithMaxPayloadBytes(1024))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = q.EnqueueReader(ctx, strings.NewReader("payload"), 7, EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	g0, p0 := b.counts()
	job, err := q.Claim(ctx, ClaimOptions{DeferPayload: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = job.Renew(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = job.Retry(ctx, RetryOptions{}); err != nil {
		t.Fatal(err)
	}
	job, err = q.Claim(ctx, ClaimOptions{DeferPayload: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = job.Dead(ctx, "done"); err != nil {
		t.Fatal(err)
	}
	g1, p1 := b.counts()
	if g1 != g0 || p1 != p0 {
		t.Fatalf("blob I/O changed gets %d->%d puts %d->%d", g0, g1, p0, p1)
	}
}

func TestExternalRetentionReleasesPayloadAndTreeGC(t *testing.T) {
	q, _, clk := testQueue(t, "payload-cleanup", WithShards(1), WithInlinePayloadBytes(1), WithMaxPayloadBytes(1024), WithCompletedRetention(time.Minute), WithPayloadGCGrace(time.Minute))
	ctx := context.Background()
	id, _, err := q.EnqueueReader(ctx, strings.NewReader("cleanup"), 7, EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := q.store.Get(ctx, jobAppKey(0, id))
	if err != nil {
		t.Fatal(err)
	}
	env, err := decodeJob(rec.Value)
	if err != nil || env.PayloadRef == nil {
		t.Fatalf("env=%#v err=%v", env, err)
	}
	ref := *env.PayloadRef
	job, err := q.Claim(ctx, ClaimOptions{DeferPayload: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = job.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)
	q.gcPass(ctx)
	if _, err = q.payloads.GetRef(ctx, ref.OwnerRefName); !errors.Is(err, tree.ErrNotFound) {
		t.Fatalf("owner ref=%v", err)
	}
	for range 3 {
		clk.Advance(2 * time.Minute)
		q.gcPass(ctx)
	}
	if _, err = q.payloads.StatBlob(ctx, ref.BlobRef); !errors.Is(err, tree.ErrNotFound) {
		t.Fatalf("blob=%v", err)
	}
}

func TestMaintenanceResumesPublishingAfterBlobUpload(t *testing.T) {
	q, _, clk := testQueue(t, "resume-publish", WithShards(1), WithInlinePayloadBytes(1), WithMaxPayloadBytes(1024))
	ctx := context.Background()
	data := []byte("resume")
	sum := sha256.Sum256(data)
	blob, err := q.payloads.PutBlob(ctx, hex.EncodeToString(sum[:]), bytes.NewReader(data), tree.WithExpectedBlobSize(int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	id := "resume-job"
	env := newPublishingJobEnvelope(id, q.name, 0, "token", PayloadInfo{ID: blob.Hash, Size: blob.Size}, q.now(), q.now().Add(time.Hour))
	body, _ := encodeJob(env)
	if _, err = q.store.Create(ctx, jobAppKey(0, id), body); err != nil {
		t.Fatal(err)
	}
	q.backfillReadyMarkers(ctx, 0)
	rec, err := q.store.Get(ctx, jobAppKey(0, id))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := decodeJob(rec.Value)
	if got.State != statePending || got.PayloadRef == nil {
		t.Fatalf("env=%#v", got)
	}
	if _, err = q.Claim(ctx, ClaimOptions{}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("delayed claim=%v", err)
	}
	clk.Advance(time.Hour)
	j, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := j.OpenPayload(ctx)
	payload, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(payload, data) {
		t.Fatalf("payload=%q", payload)
	}
}

type repeatReader struct{}

func (repeatReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}
func TestEnqueueReaderHonorsCanceledContext(t *testing.T) {
	q, _, _ := testQueue(t, "reader-cancel", WithMaxPayloadBytes(1<<20))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := q.EnqueueReader(ctx, repeatReader{}, 1<<20, EnqueueOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestLegacyEnvelopeAndDeferredInlineOpen(t *testing.T) {
	q, _, _ := testQueue(t, "legacy-wire", WithShards(1))
	ctx := context.Background()
	now := q.now()
	legacy := []byte(`{"id":"legacy","queue":"legacy-wire","shard":0,"state":"pending","attempts":0,"createdAt":"` + now.Format(time.RFC3339Nano) + `","notBefore":"` + now.Format(time.RFC3339Nano) + `","payload":"bGVnYWN5"}`)
	if _, err := q.store.Create(ctx, jobAppKey(0, "legacy"), legacy); err != nil {
		t.Fatal(err)
	}
	if err := q.createMarker(ctx, q.readyMarkerKey(0, now, "legacy")); err != nil {
		t.Fatal(err)
	}
	j, err := q.Claim(ctx, ClaimOptions{DeferPayload: true})
	if err != nil {
		t.Fatal(err)
	}
	if j.Payload != nil || j.PayloadSize != 6 {
		t.Fatalf("payload=%q size=%d", j.Payload, j.PayloadSize)
	}
	r, err := j.OpenPayload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "legacy" {
		t.Fatalf("got=%q", got)
	}
}
func TestDecodeRejectsConflictingExternalMetadata(t *testing.T) {
	env := newInlineJobEnvelope("x", "q", 0, []byte("x"), time.Now(), time.Now())
	env.PayloadRef = &payloadRefDescriptor{BlobRef: tree.BlobRef{Hash: strings.Repeat("a", 64), Size: 1}, NodeID: strings.Repeat("b", 64), OwnerRefName: "r"}
	body, _ := encodeJob(env)
	if _, err := decodeJob(body); err == nil {
		t.Fatal("conflicting payload accepted")
	}
}

func TestLargePayloadOptionDefaultsAndValidation(t *testing.T) {
	o := Options{}
	applyDefaults(&o)
	if o.MaxPayloadBytes != 500<<20 || o.InlinePayloadBytes != 256<<10 {
		t.Fatalf("defaults max=%d inline=%d", o.MaxPayloadBytes, o.InlinePayloadBytes)
	}
	if _, err := New(s3backend.NewMemory(), "bad", WithMaxPayloadBytes(10), WithInlinePayloadBytes(11)); err == nil {
		t.Fatal("inline > max accepted")
	}
	if _, err := New(s3backend.NewMemory(), "bad2", WithInlinePayloadBytes(481<<10)); err == nil {
		t.Fatal("oversized inline metadata accepted")
	}
}

func TestIdempotentEnqueueTakesOverIncompletePublishing(t *testing.T) {
	q, _, clk := testQueue(t, "takeover", WithShards(1), WithInlinePayloadBytes(1), WithMaxPayloadBytes(1024))
	ctx := context.Background()
	data := []byte("takeover")
	sum := sha256.Sum256(data)
	jobID := "idem-" + idempotencyKeyHash(q.name, "key")
	now := q.now()
	env := newPublishingJobEnvelope(jobID, q.name, 0, "abandoned", PayloadInfo{ID: hex.EncodeToString(sum[:]), Size: int64(len(data))}, now, now.Add(time.Hour))
	body, _ := encodeJob(env)
	if _, err := q.store.Create(ctx, jobAppKey(0, jobID), body); err != nil {
		t.Fatal(err)
	}
	blob, err := q.payloads.PutBlob(ctx, env.PayloadInfo.ID, bytes.NewReader(data), tree.WithExpectedBlobSize(int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	root, err := q.payloads.CommitRoot(ctx, []tree.BlobRef{blob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldRef := ownerRefName(0, jobID, "abandoned")
	if _, err = q.payloads.CreateRef(ctx, oldRef, root); err != nil {
		t.Fatal(err)
	}
	clk.Advance(q.opts.PreparationTimeout + time.Second)
	got, existed, err := q.EnqueueReader(ctx, bytes.NewReader(data), int64(len(data)), EnqueueOptions{IdempotencyKey: "key", Shard: ptrShard(0)})
	if err != nil || existed || got != jobID {
		t.Fatalf("id=%s existed=%v err=%v", got, existed, err)
	}
	if _, err = q.payloads.GetRef(ctx, oldRef); !errors.Is(err, tree.ErrNotFound) {
		t.Fatalf("stale owner ref retained: %v", err)
	}
	if _, err = q.Claim(ctx, ClaimOptions{}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("original delay lost: %v", err)
	}
	clk.Advance(time.Hour)
	j, err := q.Claim(ctx, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := j.OpenPayload(ctx)
	payload, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(payload, data) {
		t.Fatalf("payload=%q", payload)
	}
}
func ptrShard(v uint16) *uint16 { return &v }

func TestReleaseOwnerRefFencesLateCreation(t *testing.T) {
	q, _, _ := testQueue(t, "late-ref")
	ctx := context.Background()
	name := "job/0000/x/token"
	if err := q.releaseOwnerRef(ctx, name, 0); err != nil {
		t.Fatal(err)
	}
	if err := q.releaseOwnerRef(ctx, name, 0); err != nil {
		t.Fatalf("replay: %v", err)
	}
	root, err := q.payloadSentinel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.payloads.CreateRef(ctx, name, root); !errors.Is(err, tree.ErrAlreadyExists) {
		t.Fatalf("late create=%v", err)
	}
}
