//go:build s3integration

package s3backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

// TestHTTPIntegration exercises the full Backend contract against a real S3
// endpoint (AWS S3, MinIO, or another S3-compatible store).
//
// Configuration comes from the environment (see docs/reference.md):
//
//	S3_ENDPOINT          e.g. https://s3.us-east-1.amazonaws.com or http://localhost:9000
//	S3_REGION            e.g. us-east-1
//	S3_BUCKET            an existing bucket the credentials may write
//	S3_ACCESS_KEY
//	S3_SECRET_KEY
//	S3_SESSION_TOKEN     optional
//	S3_FORCE_PATH_STYLE  "true" for path-style addressing (MinIO)
//
// The test skips when any required variable is unset. All objects are
// created under a unique per-run Prefix and deleted on cleanup.
func TestHTTPIntegration(t *testing.T) {
	cfg := HTTPConfig{
		Endpoint:     os.Getenv("S3_ENDPOINT"),
		Region:       os.Getenv("S3_REGION"),
		Bucket:       os.Getenv("S3_BUCKET"),
		AccessKey:    os.Getenv("S3_ACCESS_KEY"),
		SecretKey:    os.Getenv("S3_SECRET_KEY"),
		SessionToken: os.Getenv("S3_SESSION_TOKEN"),
		PathStyle:    os.Getenv("S3_FORCE_PATH_STYLE") == "true",
	}
	if cfg.Endpoint == "" || cfg.Region == "" || cfg.Bucket == "" ||
		cfg.AccessKey == "" || cfg.SecretKey == "" {
		t.Skip("S3_ENDPOINT/S3_REGION/S3_BUCKET/S3_ACCESS_KEY/S3_SECRET_KEY not set")
	}
	// Use a unique root per run.
	cfg.Prefix = fmt.Sprintf("it/%s-%d/", time.Now().UTC().Format(time.RFC3339Nano), os.Getpid())

	c, err := NewHTTPClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Best-effort cleanup of everything under the run prefix.
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer ccancel()
		token := ""
		for {
			page, err := c.List(cctx, "", &ListOptions{ContinuationToken: token})
			if err != nil {
				t.Logf("cleanup list: %v", err)
				return
			}
			for _, o := range page.Objects {
				if err := c.Delete(cctx, o.Key); err != nil {
					t.Logf("cleanup delete %q: %v", o.Key, err)
				}
			}
			if !page.IsTruncated {
				return
			}
			token = page.NextContinuationToken
		}
	})

	// Optional large-object capabilities used by package tree.
	streamData := bytes.Repeat([]byte("stream-range-"), 4096)
	if err := c.PutStream(ctx, "stream", bytes.NewReader(streamData), int64(len(streamData)), &Preconditions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	st, err := c.Stat(ctx, "stream")
	if err != nil || st.Size != int64(len(streamData)) {
		t.Fatalf("stat=%#v err=%v", st, err)
	}
	ranged, err := c.GetRange(ctx, "stream", 7, 1234)
	if err != nil {
		t.Fatal(err)
	}
	gotRange, err := io.ReadAll(ranged.Body)
	_ = ranged.Body.Close()
	if err != nil || !bytes.Equal(gotRange, streamData[7:7+1234]) {
		t.Fatalf("range len=%d err=%v", len(gotRange), err)
	}
	streamed, err := c.GetStream(ctx, "stream")
	if err != nil {
		t.Fatal(err)
	}
	gotStream, err := io.ReadAll(streamed.Body)
	_ = streamed.Body.Close()
	if err != nil || !bytes.Equal(gotStream, streamData) {
		t.Fatalf("stream len=%d err=%v", len(gotStream), err)
	}
	multipartData := bytes.Repeat([]byte("multipart-"), 4096)
	if err = c.PutMultipart(ctx, "multipart", bytes.NewReader(multipartData), int64(len(multipartData)), nil); err != nil {
		t.Fatal(err)
	}
	if got, e := c.Get(ctx, "multipart"); e != nil || !bytes.Equal(got.Body, multipartData) {
		t.Fatalf("multipart len=%d err=%v", len(got.Body), e)
	}

	// Get of a missing key: ErrNotFound.
	if _, err := c.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// Conditional create.
	etag1, err := c.Put(ctx, "obj", []byte("v1"), &Preconditions{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	if etag1 == "" {
		t.Fatal("empty etag from Put")
	}
	if _, err := c.Put(ctx, "obj", []byte("v2"), &Preconditions{IfNoneMatch: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("create-if-absent on existing key: want ErrPreconditionFailed, got %v", err)
	}

	// Read back.
	obj, err := c.Get(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Body) != "v1" || obj.ETag != etag1 || obj.ModTime.IsZero() {
		t.Fatalf("bad Get: %+v etag %q", obj, etag1)
	}

	// Compare-and-swap: wrong etag fails, right etag wins.
	if _, err := c.Put(ctx, "obj", []byte("v2"), &Preconditions{IfMatchETag: "wrong"}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("CAS with wrong etag: want ErrPreconditionFailed, got %v", err)
	}
	etag2, err := c.Put(ctx, "obj", []byte("v2"), &Preconditions{IfMatchETag: etag1})
	if err != nil {
		t.Fatal(err)
	}
	if etag2 == etag1 {
		t.Fatal("etag must change on every write")
	}
	// If-Match on a missing key fails.
	if _, err := c.Put(ctx, "absent", []byte("x"), &Preconditions{IfMatchETag: etag2}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("If-Match on missing key: want ErrPreconditionFailed, got %v", err)
	}

	// Keys needing escaping.
	weird := "weird/a b+c&=#雪"
	if _, err := c.Put(ctx, weird, []byte("w"), nil); err != nil {
		t.Fatal(err)
	}
	if obj, err := c.Get(ctx, weird); err != nil || string(obj.Body) != "w" {
		t.Fatalf("escaped-key round trip: %v", err)
	}

	// Listing with pagination.
	for i := 0; i < 5; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("p/%02d", i), []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	token := ""
	for {
		page, err := c.List(ctx, "p/", &ListOptions{MaxKeys: 2, ContinuationToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Objects {
			got = append(got, o.Key)
		}
		if !page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
	}
	if len(got) != 5 || got[0] != "p/00" || got[4] != "p/04" {
		t.Fatalf("bad listing: %v", got)
	}
	// Prefix must not leak into listed keys, and the weird key shows up
	// under its own prefix.
	page, err := c.List(ctx, "weird/", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != weird {
		t.Fatalf("bad weird listing: %+v", page.Objects)
	}

	// Delete is idempotent.
	if err := c.Delete(ctx, "obj"); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "obj"); err != nil {
		t.Fatalf("delete of missing key must not error: %v", err)
	}
	if _, err := c.Get(ctx, "obj"); !errors.Is(err, ErrNotFound) {
		t.Fatal("key still present after delete")
	}
}
