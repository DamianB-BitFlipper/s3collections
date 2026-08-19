package s3backend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---- Memory streaming tests ----

func TestMemoryGetStreamRoundTrip(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	data := []byte("hello streaming world")
	if _, err := m.Put(ctx, "k", data, nil); err != nil {
		t.Fatal(err)
	}
	so, err := m.GetStream(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	defer so.Body.Close()
	if so.Size != int64(len(data)) {
		t.Fatalf("size: got %d want %d", so.Size, len(data))
	}
	got, err := io.ReadAll(so.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("body: got %q want %q", got, data)
	}
}

func TestMemoryGetStreamNotFound(t *testing.T) {
	m := NewMemory()
	_, err := m.GetStream(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestMemoryPutStreamRoundTrip(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	data := []byte("streamed put data")
	err := m.PutStream(ctx, "k", bytes.NewReader(data), int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := m.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(obj.Body, data) {
		t.Fatalf("body: got %q want %q", obj.Body, data)
	}
}

func TestMemoryPutStreamSizeMismatch(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	err := m.PutStream(ctx, "k", bytes.NewReader([]byte("short")), 100, nil)
	if err == nil {
		t.Fatal("expected size mismatch error")
	}
}

func TestMemoryPutStreamPreconditions(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	pre := &Preconditions{IfNoneMatch: true}
	data := []byte("v1")
	if err := m.PutStream(ctx, "k", bytes.NewReader(data), int64(len(data)), pre); err != nil {
		t.Fatal(err)
	}
	if err := m.PutStream(ctx, "k", bytes.NewReader(data), int64(len(data)), pre); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed, got %v", err)
	}
}

func TestMemoryGetRange(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	data := []byte("0123456789")
	if _, err := m.Put(ctx, "k", data, nil); err != nil {
		t.Fatal(err)
	}
	so, err := m.GetRange(ctx, "k", 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer so.Body.Close()
	got, err := io.ReadAll(so.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "23456" {
		t.Fatalf("range: got %q want %q", got, "23456")
	}
}

func TestMemoryGetRangeClampsToEnd(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	data := []byte("0123456789")
	if _, err := m.Put(ctx, "k", data, nil); err != nil {
		t.Fatal(err)
	}
	so, err := m.GetRange(ctx, "k", 5, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer so.Body.Close()
	got, _ := io.ReadAll(so.Body)
	if string(got) != "56789" {
		t.Fatalf("clamped range: got %q want %q", got, "56789")
	}
}

func TestMemoryGetRangeValidation(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, err := m.Put(ctx, "k", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetRange(ctx, "k", -1, 1); err == nil {
		t.Fatal("negative offset must error")
	}
	if _, err := m.GetRange(ctx, "k", 0, 0); err == nil {
		t.Fatal("zero length must error")
	}
	if _, err := m.GetRange(ctx, "k", 0, -1); err == nil {
		t.Fatal("negative length must error")
	}
}

func TestMemoryGetRangeNotFound(t *testing.T) {
	m := NewMemory()
	_, err := m.GetRange(context.Background(), "missing", 0, 1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// ---- HTTP streaming tests ----

func TestHTTPGetStreamRoundTrip(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	data := []byte("streaming over http")
	if _, err := c.Put(ctx, "k", data, nil); err != nil {
		t.Fatal(err)
	}
	so, err := c.GetStream(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	defer so.Body.Close()
	if so.ETag == "" {
		t.Fatal("etag must be set")
	}
	got, err := io.ReadAll(so.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("body: got %q want %q", got, data)
	}
}

func TestHTTPGetStreamNotFound(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	_, err := c.GetStream(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestHTTPGetStreamErrorMapping(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	_, err := c.GetStream(context.Background(), "fail/internal")
	if !IsRetryable(err) {
		t.Fatalf("500 must be retryable: %v", err)
	}
}

func TestHTTPPutStreamRoundTrip(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	data := []byte("putstream data over http")
	err := c.PutStream(ctx, "k", bytes.NewReader(data), int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Verify the data was stored correctly on the fake.
	obj, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(obj.Body, data) {
		t.Fatalf("body: got %q want %q", obj.Body, data)
	}
	// Verify the request was signed with a real payload hash (not UNSIGNED-PAYLOAD).
	rq := fake.lastRequest()
	sha := rq.Header.Get("X-Amz-Content-Sha256")
	if sha == "" || sha == "UNSIGNED-PAYLOAD" {
		t.Fatalf("payload hash must be a real SHA-256, got %q", sha)
	}
	// Verify the request was signed with a real payload hash (not UNSIGNED-PAYLOAD).
	// Content-Length should have been set by the client.
	if rq.Header.Get("X-Amz-Content-Sha256") == "UNSIGNED-PAYLOAD" {
		t.Fatal("PutStream must not use UNSIGNED-PAYLOAD")
	}
}

func TestHTTPPutStreamPreconditions(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	data := []byte("v1")
	pre := &Preconditions{IfNoneMatch: true}
	if err := c.PutStream(ctx, "k", bytes.NewReader(data), int64(len(data)), pre); err != nil {
		t.Fatal(err)
	}
	if err := c.PutStream(ctx, "k", bytes.NewReader(data), int64(len(data)), pre); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed, got %v", err)
	}
}

func TestHTTPPutStreamSizeMismatch(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	err := c.PutStream(ctx, "k", bytes.NewReader([]byte("short")), 100, nil)
	if err == nil {
		t.Fatal("expected size mismatch error")
	}
}

func TestHTTPGetRangeRoundTrip(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	data := []byte("0123456789abcdef")
	if _, err := c.Put(ctx, "k", data, nil); err != nil {
		t.Fatal(err)
	}
	so, err := c.GetRange(ctx, "k", 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer so.Body.Close()
	got, err := io.ReadAll(so.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "3456" {
		t.Fatalf("range: got %q want %q", got, "3456")
	}
	// Verify the Range header was sent.
	rq := fake.lastRequest()
	if got := rq.Header.Get("Range"); got != "bytes=3-6" {
		t.Fatalf("Range header: got %q want %q", got, "bytes=3-6")
	}
}

func TestHTTPGetRangeNotFound(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	_, err := c.GetRange(context.Background(), "missing", 0, 1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestHTTPGetRangeValidation(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()
	if _, err := c.Put(ctx, "k", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetRange(ctx, "k", -1, 1); err == nil {
		t.Fatal("negative offset must error")
	}
	if _, err := c.GetRange(ctx, "k", 0, 0); err == nil {
		t.Fatal("zero length must error")
	}
}

// ---- Chaos streaming tests ----

func TestChaosGetStreamInjectsErrors(t *testing.T) {
	mem := NewMemory()
	ctx := context.Background()
	if _, err := mem.Put(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	c := NewChaos(mem, ChaosConfig{Rand: newRand(7), ErrorRate: 0.5})
	var sawErr, sawOK bool
	for i := 0; i < 50; i++ {
		so, err := c.GetStream(ctx, "k")
		if err != nil {
			sawErr = true
			if !IsRetryable(err) {
				t.Fatalf("chaos error must be retryable: %v", err)
			}
			continue
		}
		sawOK = true
		so.Body.Close()
	}
	if !sawErr || !sawOK {
		t.Fatalf("expected both errors and successes; err=%v ok=%v", sawErr, sawOK)
	}
}

func TestChaosPutStreamAmbiguousWrite(t *testing.T) {
	mem := NewMemory()
	c := NewChaos(mem, ChaosConfig{Rand: newRand(3), AmbiguousWriteRate: 1.0})
	ctx := context.Background()
	err := c.PutStream(ctx, "k", bytes.NewReader([]byte("v")), 1, nil)
	if err == nil {
		t.Fatal("expected ambiguous error")
	}
	obj, gerr := mem.Get(ctx, "k")
	if gerr != nil || string(obj.Body) != "v" {
		t.Fatal("ambiguous write must still be applied")
	}
}

func TestChaosGetRange(t *testing.T) {
	mem := NewMemory()
	ctx := context.Background()
	if _, err := mem.Put(ctx, "k", []byte("0123456789"), nil); err != nil {
		t.Fatal(err)
	}
	c := NewChaos(mem, ChaosConfig{})
	so, err := c.GetRange(ctx, "k", 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer so.Body.Close()
	got, _ := io.ReadAll(so.Body)
	if string(got) != "23456" {
		t.Fatalf("range: got %q want %q", got, "23456")
	}
}

// ---- Interface assertion tests ----

func TestStreamingInterfaceAssertions(t *testing.T) {
	var b Backend
	b = NewMemory()
	if _, ok := b.(StreamBackend); !ok {
		t.Error("Memory must satisfy StreamBackend")
	}
	if _, ok := b.(RangeBackend); !ok {
		t.Error("Memory must satisfy RangeBackend")
	}
	b = NewChaos(NewMemory(), ChaosConfig{})
	if _, ok := b.(StreamBackend); !ok {
		t.Error("Chaos must satisfy StreamBackend")
	}
	if _, ok := b.(RangeBackend); !ok {
		t.Error("Chaos must satisfy RangeBackend")
	}
	// HTTPClient assertions are compile-time via var _ in stream.go.
}

// newRand is a test helper to avoid importing math/rand in every test.
func newRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

func TestMemoryStat(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	if _, err := m.Put(ctx, "stat", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	st, err := m.Stat(ctx, "stat")
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 5 || st.ETag == "" {
		t.Fatalf("stat=%#v", st)
	}
	if _, err := m.Stat(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestHTTPStat(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()
	if _, err := c.Put(ctx, "stat", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	st, err := c.Stat(ctx, "stat")
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 5 || st.ETag == "" || st.ModTime.IsZero() {
		t.Fatalf("stat=%#v", st)
	}
	if _, err := c.Stat(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
	_ = time.Second
}

func TestHTTPGetRangeRejectsUnexpectedShortResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-2/10")
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abc"))
	}))
	defer srv.Close()
	c := newTestClient(t, srv, "")
	o, err := c.GetRange(context.Background(), "k", 0, 5)
	if o != nil {
		if o.Body != nil {
			o.Body.Close()
		}
		t.Fatal("unexpected object")
	}
	var be *Error
	if !errors.As(err, &be) || be.Code != "InvalidResponse" {
		t.Fatalf("err=%v", err)
	}
}

func TestHTTPGetStreamRejectsMissingOrMalformedLengthAndPartial(t *testing.T) {
	cases := []struct {
		name   string
		status int
		length string
	}{{"missing", 200, ""}, {"partial", 206, "3"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.length != "" {
					w.Header().Set("Content-Length", tc.length)
				}
				w.WriteHeader(tc.status)
				if tc.name == "missing" {
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				}
				_, _ = w.Write([]byte("abc"))
			}))
			defer srv.Close()
			c := newTestClient(t, srv, "")
			o, err := c.GetStream(context.Background(), "k")
			if o != nil {
				if o.Body != nil {
					o.Body.Close()
				}
				t.Fatal("unexpected body")
			}
			var be *Error
			if !errors.As(err, &be) || be.Code != "InvalidResponse" {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
func TestHTTPGetStreamDetectsShortAndLongBodies(t *testing.T) {
	shortSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte("abc"))
	}))
	defer shortSrv.Close()
	c := newTestClient(t, shortSrv, "")
	o, err := c.GetStream(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(o.Body)
	o.Body.Close()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short err=%v", err)
	}
	long := &exactLengthReadCloser{r: io.NopCloser(bytes.NewReader([]byte("abcd"))), remaining: 3}
	b, err := io.ReadAll(long)
	if err == nil || string(b) != "abc" {
		t.Fatalf("long=%q err=%v", b, err)
	}
}
