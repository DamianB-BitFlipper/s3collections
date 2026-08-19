package s3backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestMemoryStatAndMultipartCapabilities(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	data := []byte("multipart-memory")
	if err := m.PutMultipart(ctx, "k", bytes.NewReader(data), int64(len(data)), &Preconditions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	st, err := m.Stat(ctx, "k")
	if err != nil || st.Size != int64(len(data)) || st.ETag == "" {
		t.Fatalf("stat=%#v err=%v", st, err)
	}
	if _, ok := any(m).(MultipartBackend); !ok {
		t.Fatal("Memory must advertise MultipartBackend")
	}
}
func TestHTTPStatAndMultipartClaim(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()
	data := []byte("head me")
	if _, err := c.Put(ctx, "k", data, nil); err != nil {
		t.Fatal(err)
	}
	st, err := c.Stat(ctx, "k")
	if err != nil || st.Size != int64(len(data)) || st.ETag == "" {
		t.Fatalf("stat=%#v err=%v", st, err)
	}
	if _, ok := any(c).(MultipartBackend); !ok {
		t.Fatal("HTTPClient must advertise implemented portable multipart")
	}
	if _, err = c.Stat(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing=%v", err)
	}
}
func TestHTTPRangeMayEndAtEOF(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()
	if _, err := c.Put(ctx, "k", []byte("0123456789"), nil); err != nil {
		t.Fatal(err)
	}
	o, err := c.GetRange(ctx, "k", 7, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Body.Close()
	body, err := io.ReadAll(o.Body)
	if err != nil || string(body) != "789" || o.Size != 3 {
		t.Fatalf("body=%q size=%d err=%v", body, o.Size, err)
	}
}
func TestPutStreamRejectsExtraAndRangeOverflow(t *testing.T) {
	m := NewMemory()
	if err := m.PutStream(context.Background(), "k", bytes.NewReader([]byte("extra")), 4, nil); err == nil {
		t.Fatal("extra byte accepted")
	}
	if _, err := m.GetRange(context.Background(), "k", math.MaxInt64, 2); err == nil {
		t.Fatal("overflow range accepted")
	}
}

func TestHTTPPutMultipartPortableProtocol(t *testing.T) {
	var mu sync.Mutex
	parts := map[int][]byte{}
	aborted := false
	completed := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut && q.Get("uploadId") == "u1":
			n, _ := strconv.Atoi(q.Get("partNumber"))
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			parts[n] = b
			mu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, n))
		case r.Method == http.MethodPost && q.Get("uploadId") == "u1":
			b, _ := io.ReadAll(r.Body)
			if !bytes.Contains(b, []byte("<PartNumber>1</PartNumber>")) {
				t.Errorf("complete body=%s", b)
			}
			completed = true
			io.WriteString(w, "<CompleteMultipartUploadResult><ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>")
		case r.Method == http.MethodDelete && q.Get("uploadId") == "u1":
			aborted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	c := newTestClient(t, srv, "")
	data := bytes.Repeat([]byte("x"), int(portableMultipartPartSize+17))
	if err := c.PutMultipart(context.Background(), "big", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	if !completed || aborted {
		t.Fatalf("completed=%v aborted=%v", completed, aborted)
	}
	got := append(append([]byte(nil), parts[1]...), parts[2]...)
	if !bytes.Equal(got, data) {
		t.Fatalf("uploaded=%d want=%d", len(got), len(data))
	}
}

func TestHTTPPutMultipartAbortsOnPartFailure(t *testing.T) {
	aborted := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.Method == http.MethodPost && q.Has("uploads") {
			io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>")
			return
		}
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, "<Error><Code>InternalError</Code><Message>fail</Message></Error>")
			return
		}
		if r.Method == http.MethodDelete && q.Get("uploadId") == "u1" {
			aborted = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := newTestClient(t, srv, "")
	err := c.PutMultipart(context.Background(), "big", bytes.NewReader([]byte("abc")), 3, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !aborted {
		t.Fatal("multipart was not aborted")
	}
}

func TestHTTPPutMultipartTreatsEmbeddedCompleteErrorAsFailure(t *testing.T) {
	aborted := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut:
			w.Header().Set("ETag", `"p1"`)
		case r.Method == http.MethodPost && q.Get("uploadId") == "u1":
			io.WriteString(w, "<Error><Code>InternalError</Code><Message>retry me</Message></Error>")
		case r.Method == http.MethodDelete:
			aborted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	c := newTestClient(t, srv, "")
	err := c.PutMultipart(context.Background(), "big", bytes.NewReader([]byte("abc")), 3, nil)
	var be *Error
	if !errors.As(err, &be) || be.Code != "InternalError" || !be.Retryable {
		t.Fatalf("err=%v", err)
	}
	if !aborted {
		t.Fatal("not aborted")
	}
}

func TestHTTPPutMultipartRejectsTrailingBytes(t *testing.T) {
	completed, aborted := false, false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut:
			w.Header().Set("ETag", `"p1"`)
		case r.Method == http.MethodPost && q.Get("uploadId") == "u1":
			completed = true
			io.WriteString(w, "<CompleteMultipartUploadResult><ETag>done</ETag></CompleteMultipartUploadResult>")
		case r.Method == http.MethodDelete:
			aborted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	c := newTestClient(t, srv, "")
	err := c.PutMultipart(context.Background(), "big", bytes.NewReader([]byte("abcd")), 3, nil)
	if err == nil {
		t.Fatal("trailing byte accepted")
	}
	if completed || !aborted {
		t.Fatalf("completed=%v aborted=%v", completed, aborted)
	}
}

func TestHTTPRangeBodyLengthIsEnforced(t *testing.T) {
	for _, tc := range []struct{ name, body string }{{"short", "abc"}, {"long", "abcdef"}} {
		t.Run(tc.name, func(t *testing.T) {
			rt := roundTripFunc(func(*http.Request) (*http.Response, error) {
				h := make(http.Header)
				h.Set("Content-Range", "bytes 0-4/10")
				h.Set("Content-Length", "5")
				return &http.Response{StatusCode: http.StatusPartialContent, Status: "206 Partial Content", Header: h, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			c := captureClient(t, true, &rt)
			o, err := c.GetRange(context.Background(), "k", 0, 5)
			if err != nil {
				t.Fatal(err)
			}
			defer o.Body.Close()
			if _, err = io.ReadAll(o.Body); err == nil {
				t.Fatal("malformed range body accepted")
			}
		})
	}
}
