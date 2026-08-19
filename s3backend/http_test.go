package s3backend

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 is an in-memory S3 subset for tests: conditional PUT, GET, DELETE,
// and ListObjectsV2 with pagination. It speaks path-style URLs
// (/<bucket>/<key>) and returns S3-shaped XML errors.
type fakeS3 struct {
	mu       sync.Mutex
	bucket   string
	objects  map[string]fakeS3Object
	version  uint64
	requests []fakeS3Request
}

type fakeS3Object struct {
	body    []byte
	etag    string
	modTime time.Time
}

type fakeS3Request struct {
	Method        string
	Path          string
	Query         url.Values
	Header        http.Header
	ContentLength int64
}

func newFakeS3() *fakeS3 {
	return &fakeS3{bucket: "test-bucket", objects: make(map[string]fakeS3Object)}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, fakeS3Request{
		Method:        r.Method,
		Path:          r.URL.Path,
		Query:         r.URL.Query(),
		Header:        r.Header.Clone(),
		ContentLength: r.ContentLength,
	})

	rest := strings.TrimPrefix(r.URL.Path, "/")
	if rest != f.bucket && !strings.HasPrefix(rest, f.bucket+"/") {
		f.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}
	key := strings.TrimPrefix(strings.TrimPrefix(rest, f.bucket), "/")
	if key == "" {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			f.list(w, r)
			return
		}
		f.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed on bucket")
		return
	}
	// Fault-injection keys for error-mapping tests.
	switch key {
	case "fail/slowdown":
		f.writeError(w, http.StatusServiceUnavailable, "SlowDown", "Please reduce your request rate.")
		return
	case "fail/internal":
		f.writeError(w, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.")
		return
	case "fail/plain":
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "boom")
		return
	}
	switch r.Method {
	case http.MethodPut:
		f.put(w, r, key)
	case http.MethodGet:
		f.get(w, r, key)
	case http.MethodHead:
		f.head(w, key)
	case http.MethodDelete:
		f.delete(w, key)
	default:
		f.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method")
	}
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, key string) {
	body, _ := io.ReadAll(r.Body)
	cur, exists := f.objects[key]
	if r.Header.Get("If-None-Match") == "*" && exists {
		f.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold")
		return
	}
	ifMatch := r.Header.Get("If-Match")
	if ifMatch != "" && (!exists || unquoteETag(ifMatch) != cur.etag) {
		f.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold")
		return
	}
	f.version++
	etag := fmt.Sprintf("%016x", f.version)
	stored := make([]byte, len(body))
	copy(stored, body)
	f.objects[key] = fakeS3Object{body: stored, etag: etag, modTime: time.Now().UTC()}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) head(w http.ResponseWriter, key string) {
	o, ok := f.objects[key]
	if !ok {
		f.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	w.Header().Set("ETag", `"`+o.etag+`"`)
	w.Header().Set("Last-Modified", o.modTime.Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.Itoa(len(o.body)))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) get(w http.ResponseWriter, r *http.Request, key string) {
	o, ok := f.objects[key]
	if !ok {
		f.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	w.Header().Set("ETag", `"`+o.etag+`"`)
	w.Header().Set("Last-Modified", o.modTime.Format(http.TimeFormat))
	// Support Range requests (bytes=start-end).
	if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
		if strings.HasPrefix(rangeHdr, "bytes=") {
			spec := rangeHdr[6:]
			dash := strings.IndexByte(spec, '-')
			if dash > 0 {
				start, err1 := strconv.ParseInt(spec[:dash], 10, 64)
				end, err2 := strconv.ParseInt(spec[dash+1:], 10, 64)
				if err1 == nil && err2 == nil && start >= 0 && end >= start && start < int64(len(o.body)) {
					if end >= int64(len(o.body)) {
						end = int64(len(o.body)) - 1
					}
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.body)))
					w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
					w.WriteHeader(http.StatusPartialContent)
					w.Write(o.body[start : end+1])
					return
				}
			}
		}
	}
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(o.body)), 10))
	w.WriteHeader(http.StatusOK)
	w.Write(o.body)
}

func (f *fakeS3) delete(w http.ResponseWriter, key string) {
	delete(f.objects, key)
	w.WriteHeader(http.StatusNoContent)
}

// listResultXML mirrors the production decode struct but carries the S3
// namespace, to emit realistic documents.
type listResultXML struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	XMLNS                 string   `xml:"xmlns,attr"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	MaxKeys               int      `xml:"MaxKeys"`
	KeyCount              int      `xml:"KeyCount"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	Contents              []struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	} `xml:"Contents"`
}

func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	startAfter := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		startAfter = tok
	}
	maxKeys := 1000
	if s := q.Get("max-keys"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxKeys = n
		}
	}
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var result listResultXML
	result.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
	result.Name = f.bucket
	result.Prefix = prefix
	result.MaxKeys = maxKeys
	if len(keys) > maxKeys {
		result.IsTruncated = true
		keys = keys[:maxKeys]
		result.NextContinuationToken = keys[len(keys)-1]
	}
	result.KeyCount = len(keys)
	for _, k := range keys {
		o := f.objects[k]
		c := struct {
			Key          string `xml:"Key"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
		}{
			Key:          k,
			LastModified: o.modTime.Format(time.RFC3339),
			ETag:         `"` + o.etag + `"`,
			Size:         int64(len(o.body)),
		}
		result.Contents = append(result.Contents, c)
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

func (f *fakeS3) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+
		`<Code>%s</Code><Message>%s</Message><Resource>/%s</Resource><RequestId>req-1</RequestId>`+
		`</Error>`, code, message, f.bucket)
}

func (f *fakeS3) hasKey(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func (f *fakeS3) lastRequest() fakeS3Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

func (f *fakeS3) numRequests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// newTestClient returns an HTTPClient talking to the fake over path-style
// addressing (the fake's loopback host cannot do virtual-hosted style).
func newTestClient(t *testing.T, srv *httptest.Server, prefix string) *HTTPClient {
	t.Helper()
	c, err := NewHTTPClient(HTTPConfig{
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		Bucket:    "test-bucket",
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		PathStyle: true,
		Prefix:    prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHTTPPutGetRoundTrip(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	etag, err := c.Put(ctx, "a/b", []byte("hello"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(etag, `"`) || etag == "" {
		t.Fatalf("etag must be returned unquoted, got %q", etag)
	}
	obj, err := c.Get(ctx, "a/b")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Body) != "hello" || obj.ETag != etag || obj.Key != "a/b" {
		t.Fatalf("bad object: %+v (etag %q)", obj, etag)
	}
	if obj.ModTime.IsZero() {
		t.Fatal("ModTime must come from Last-Modified header")
	}
	// Every request must be signed.
	for i, rq := range fake.requests {
		if got := rq.Header.Get("Authorization"); !strings.HasPrefix(got, "AWS4-HMAC-SHA256 ") {
			t.Fatalf("request %d missing/invalid Authorization header: %q", i, got)
		}
	}
}

func TestHTTPKeyEscaping(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	key := "weird/a b+c%25&=?#雪"
	if _, err := c.Put(ctx, key, []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	if !fake.hasKey(key) {
		t.Fatalf("fake must store the decoded key %q; has %v", key, fake.objects)
	}
	obj, err := c.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Body) != "v" {
		t.Fatalf("bad body %q", obj.Body)
	}
	page, err := c.List(ctx, "weird/", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != key {
		t.Fatalf("bad list: %+v", page.Objects)
	}
	if err := c.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if fake.hasKey(key) {
		t.Fatal("key not deleted")
	}
}

func TestHTTPPutPreconditions(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	// Create-if-absent.
	if _, err := c.Put(ctx, "k", []byte("1"), &Preconditions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(ctx, "k", []byte("2"), &Preconditions{IfNoneMatch: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed, got %v", err)
	}
	obj, _ := c.Get(ctx, "k")
	if string(obj.Body) != "1" {
		t.Fatal("failed If-None-Match write still mutated the object")
	}

	// Compare-and-swap on ETag.
	if _, err := c.Put(ctx, "k", []byte("2"), &Preconditions{IfMatchETag: "wrong"}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed, got %v", err)
	}
	etag2, err := c.Put(ctx, "k", []byte("2"), &Preconditions{IfMatchETag: obj.ETag})
	if err != nil {
		t.Fatal(err)
	}
	if etag2 == obj.ETag {
		t.Fatal("ETag must change on every write")
	}
	// The If-Match header must be quoted on the wire.
	if got := fake.lastRequest().Header.Get("If-Match"); got != `"`+obj.ETag+`"` {
		t.Fatalf("If-Match header must be quoted, got %q", got)
	}
	// If-Match on a missing key fails.
	if _, err := c.Put(ctx, "absent", []byte("x"), &Preconditions{IfMatchETag: etag2}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("want ErrPreconditionFailed for missing key, got %v", err)
	}
}

func TestHTTPDeleteIdempotent(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	if _, err := c.Put(ctx, "k", []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete of missing key must not error: %v", err)
	}
	if _, err := c.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatal("key still present after delete")
	}
}

func TestHTTPGetNotFound(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	_, err := c.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if IsRetryable(err) {
		t.Fatal("ErrNotFound must not be retryable")
	}
}

func TestHTTPListPagination(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("p/%02d", i), []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Put(ctx, "other/1", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	var got []string
	token := ""
	pages := 0
	for {
		page, err := c.List(ctx, "p/", &ListOptions{MaxKeys: 3, ContinuationToken: token})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, o := range page.Objects {
			got = append(got, o.Key)
			if strings.Contains(o.ETag, `"`) {
				t.Fatalf("list etags must be unquoted, got %q", o.ETag)
			}
			if o.Size != 1 || o.ModTime.IsZero() {
				t.Fatalf("bad ObjectInfo: %+v", o)
			}
		}
		if !page.IsTruncated {
			if page.NextContinuationToken != "" {
				t.Fatal("final page must not carry a continuation token")
			}
			break
		}
		token = page.NextContinuationToken
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(got) != 7 || got[0] != "p/00" || got[6] != "p/06" {
		t.Fatalf("bad listing: %v", got)
	}
	if pages != 3 {
		t.Fatalf("want 3 pages of 3,3,1; got %d", pages)
	}

	// StartAfter variant.
	page, err := c.List(ctx, "p/", &ListOptions{StartAfter: "p/03"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 3 || page.Objects[0].Key != "p/04" {
		t.Fatalf("bad StartAfter listing: %+v", page.Objects)
	}

	// Empty listing.
	page, err = c.List(ctx, "nope/", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 0 || page.IsTruncated {
		t.Fatalf("want empty page, got %+v", page)
	}
}

func TestHTTPListMaxKeysCapped(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")

	if _, err := c.List(context.Background(), "", &ListOptions{MaxKeys: 5000}); err != nil {
		t.Fatal(err)
	}
	if got := fake.lastRequest().Query.Get("max-keys"); got != "1000" {
		t.Fatalf("MaxKeys must be capped at 1000, sent %q", got)
	}
	// Zero MaxKeys sends no parameter (server default).
	if _, err := c.List(context.Background(), "", nil); err != nil {
		t.Fatal(err)
	}
	if got := fake.lastRequest().Query.Get("max-keys"); got != "" {
		t.Fatalf("zero MaxKeys must omit the parameter, sent %q", got)
	}
}

func TestHTTPPrefix(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "tenant-a/")
	ctx := context.Background()

	if _, err := c.Put(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	if !fake.hasKey("tenant-a/k") {
		t.Fatal("Prefix must be prepended to the physical key")
	}
	obj, err := c.Get(ctx, "k")
	if err != nil || string(obj.Body) != "v" {
		t.Fatalf("Get through prefix: %v", err)
	}
	// A second logical backend on the same fake bucket must not see it.
	other := newTestClient(t, srv, "tenant-b/")
	page, err := other.List(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 0 {
		t.Fatalf("prefix isolation violated: %+v", page.Objects)
	}
	page, err = c.List(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "k" {
		t.Fatalf("Prefix must be stripped from listed keys: %+v", page.Objects)
	}
	// StartAfter is an app-level key: it must be mapped to the physical
	// key space.
	if _, err := c.Put(ctx, "k2", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	page, err = c.List(ctx, "", &ListOptions{StartAfter: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "k2" {
		t.Fatalf("bad StartAfter with prefix: %+v", page.Objects)
	}
	if got := fake.lastRequest().Query.Get("start-after"); got != "tenant-a/k" {
		t.Fatalf("start-after must be mapped to physical key space, sent %q", got)
	}
}

func TestHTTPErrorMapping(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	// 503 SlowDown XML body -> retryable *Error with parsed code/message.
	_, err := c.Get(ctx, "fail/slowdown")
	var serr *Error
	if !errors.As(err, &serr) {
		t.Fatalf("want *Error, got %T: %v", err, err)
	}
	if serr.StatusCode != 503 || serr.Code != "SlowDown" || !serr.Retryable {
		t.Fatalf("bad mapping: %+v", serr)
	}
	if !IsRetryable(err) {
		t.Fatal("SlowDown must be retryable")
	}
	if !strings.Contains(serr.Message, "reduce your request rate") {
		t.Fatalf("message not parsed from XML: %q", serr.Message)
	}

	// 500 InternalError -> retryable.
	if _, err := c.Get(ctx, "fail/internal"); !IsRetryable(err) {
		t.Fatalf("500 must be retryable: %v", err)
	}

	// Non-XML error body still maps to a retryable *Error.
	_, err = c.Get(ctx, "fail/plain")
	if !errors.As(err, &serr) || serr.StatusCode != 500 || !serr.Retryable {
		t.Fatalf("non-XML 500 must map to retryable *Error: %v", err)
	}
	if serr.Message != "boom" {
		t.Fatalf("want raw body as message, got %q", serr.Message)
	}

	// Retryable errors surface on every op, not just Get.
	if _, err := c.Put(ctx, "fail/slowdown", []byte("x"), &Preconditions{IfNoneMatch: true}); !IsRetryable(err) {
		t.Fatalf("503 on Put must be retryable: %v", err)
	}
}

func TestHTTPKeyValidation(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx := context.Background()

	for _, key := range []string{"", "bad\x01key", "bad\x7fkey", "line\nbreak"} {
		if _, err := c.Get(ctx, key); err == nil {
			t.Fatalf("Get(%q) must fail validation", key)
		}
		if _, err := c.Put(ctx, key, []byte("v"), nil); err == nil {
			t.Fatalf("Put(%q) must fail validation", key)
		}
		if err := c.Delete(ctx, key); err == nil {
			t.Fatalf("Delete(%q) must fail validation", key)
		}
	}
	if _, err := c.List(ctx, "bad\x02prefix", nil); err == nil {
		t.Fatal("List with control-char prefix must fail validation")
	}
	if n := fake.numRequests(); n != 0 {
		t.Fatalf("validation must happen client-side; fake saw %d requests", n)
	}
	// Validation errors are not retryable.
	_, err := c.Get(ctx, "")
	if IsRetryable(err) {
		t.Fatal("validation error must not be retryable")
	}
}

// roundTripFunc adapts a function to http.RoundTripper for no-network tests
// of request construction.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func captureClient(t *testing.T, pathStyle bool, rt *roundTripFunc) *HTTPClient {
	t.Helper()
	c, err := NewHTTPClient(HTTPConfig{
		Endpoint:  "https://s3.example.com",
		Region:    "eu-west-1",
		Bucket:    "photos",
		AccessKey: "AK",
		SecretKey: "SK",
		PathStyle: pathStyle,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return (*rt)(r)
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func okResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"ETag": {`"0123abcd"`}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestHTTPVirtualHostedAddressing(t *testing.T) {
	var got *http.Request
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		return okResponse(""), nil
	})
	c := captureClient(t, false, &rt)
	if _, err := c.Get(context.Background(), "dir/file.txt"); err != nil {
		t.Fatal(err)
	}
	if got.URL.Host != "photos.s3.example.com" {
		t.Fatalf("virtual-hosted host: %q", got.URL.Host)
	}
	if got.URL.Path != "/dir/file.txt" {
		t.Fatalf("virtual-hosted path: %q", got.URL.Path)
	}
	if got.Host != "" && got.Host != "photos.s3.example.com" {
		t.Fatalf("request Host: %q", got.Host)
	}
}

func TestHTTPPathStyleAddressing(t *testing.T) {
	var got *http.Request
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		return okResponse(""), nil
	})
	c := captureClient(t, true, &rt)
	if _, err := c.Get(context.Background(), "dir/file.txt"); err != nil {
		t.Fatal(err)
	}
	if got.URL.Host != "s3.example.com" {
		t.Fatalf("path-style host: %q", got.URL.Host)
	}
	if got.URL.Path != "/photos/dir/file.txt" {
		t.Fatalf("path-style path: %q", got.URL.Path)
	}
}

func TestHTTPListAddressing(t *testing.T) {
	var got *http.Request
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		return okResponse(`<?xml version="1.0"?><ListBucketResult></ListBucketResult>`), nil
	})
	c := captureClient(t, true, &rt)
	if _, err := c.List(context.Background(), "pre/", nil); err != nil {
		t.Fatal(err)
	}
	if got.URL.Path != "/photos" {
		t.Fatalf("list must target the bucket root, got %q", got.URL.Path)
	}
	q := got.URL.Query()
	if q.Get("list-type") != "2" || q.Get("prefix") != "pre/" {
		t.Fatalf("bad list query: %q", got.URL.RawQuery)
	}
}

func TestHTTPSigningHeaders(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c, err := NewHTTPClient(HTTPConfig{
		Endpoint:     srv.URL,
		Region:       "ap-southeast-2",
		Bucket:       "test-bucket",
		AccessKey:    "AKIDEXAMPLE",
		SecretKey:    "secret",
		SessionToken: "token-123",
		PathStyle:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(context.Background(), "k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	rq := fake.lastRequest()
	auth := rq.Header.Get("Authorization")
	wantPrefix := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"
	if !strings.HasPrefix(auth, wantPrefix) {
		t.Fatalf("Authorization prefix: %q", auth)
	}
	if !strings.Contains(auth, "/ap-southeast-2/s3/aws4_request") {
		t.Fatalf("Authorization scope: %q", auth)
	}
	if !strings.Contains(auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token") {
		t.Fatalf("signed headers: %q", auth)
	}
	if !strings.Contains(auth, "Signature=") {
		t.Fatalf("signature missing: %q", auth)
	}
	if rq.Header.Get("X-Amz-Security-Token") != "token-123" {
		t.Fatalf("session token header: %q", rq.Header.Get("X-Amz-Security-Token"))
	}
	if _, err := time.Parse("20060102T150405Z", rq.Header.Get("X-Amz-Date")); err != nil {
		t.Fatalf("bad X-Amz-Date: %v", err)
	}
	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got := rq.Header.Get("X-Amz-Content-Sha256"); got == "" || got == emptySHA256 {
		t.Fatalf("payload hash must be the body hash, got %q", got)
	}
}

func TestHTTPContextCanceled(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Get(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if _, err := c.Put(ctx, "a", []byte("v"), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if err := c.Delete(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if _, err := c.List(ctx, "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestNewHTTPClientValidation(t *testing.T) {
	good := HTTPConfig{
		Endpoint:  "https://s3.us-east-1.amazonaws.com",
		Region:    "us-east-1",
		Bucket:    "b",
		AccessKey: "AK",
		SecretKey: "SK",
	}
	if _, err := NewHTTPClient(good); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*HTTPConfig){
		"empty endpoint":   func(c *HTTPConfig) { c.Endpoint = "" },
		"bad scheme":       func(c *HTTPConfig) { c.Endpoint = "ftp://x" },
		"no host":          func(c *HTTPConfig) { c.Endpoint = "https://" },
		"empty region":     func(c *HTTPConfig) { c.Region = "" },
		"empty bucket":     func(c *HTTPConfig) { c.Bucket = "" },
		"empty access key": func(c *HTTPConfig) { c.AccessKey = "" },
		"empty secret key": func(c *HTTPConfig) { c.SecretKey = "" },
		"control prefix":   func(c *HTTPConfig) { c.Prefix = "a\x01" },
	}
	for name, mutate := range cases {
		cfg := good
		mutate(&cfg)
		if _, err := NewHTTPClient(cfg); err == nil {
			t.Fatalf("%s: want error", name)
		}
	}
}

// TestHTTPConcurrentUse exercises the client from many goroutines so the
// race detector can catch accidental shared state.
func TestHTTPConcurrentUse(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := newTestClient(t, srv, "shared/")
	ctx := context.Background()

	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("w/%02d", i)
			if _, err := c.Put(ctx, key, []byte(fmt.Sprint(i)), &Preconditions{IfNoneMatch: true}); err != nil {
				t.Error(err)
				return
			}
			obj, err := c.Get(ctx, key)
			if err != nil {
				t.Error(err)
				return
			}
			if string(obj.Body) != fmt.Sprint(i) {
				t.Errorf("key %q: body %q", key, obj.Body)
			}
			if _, err := c.List(ctx, "w/", nil); err != nil {
				t.Error(err)
			}
			if err := c.Delete(ctx, key); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
}

// TestSigV4AWSExample pins the signer to the worked example from the AWS
// documentation "Signature Version 4 signing process": a GET of
// https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08 signed at
// 20150830T123600Z with the example credentials.
func TestSigV4AWSExample(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet,
		"https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	signer := sigV4Signer{
		accessKey: "AKIDEXAMPLE",
		secretKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
		region:    "us-east-1",
		service:   "iam",
	}
	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	signer.sign(req, emptySHA256, time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC))

	const wantAuth = "AWS4-HMAC-SHA256 " +
		"Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, " +
		"SignedHeaders=content-type;host;x-amz-date, " +
		"Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
	if got := req.Header.Get("Authorization"); got != wantAuth {
		t.Fatalf("Authorization mismatch:\n got: %s\nwant: %s", got, wantAuth)
	}
	if got := req.Header.Get("X-Amz-Date"); got != "20150830T123600Z" {
		t.Fatalf("X-Amz-Date: %q", got)
	}
}

// TestSigV4DeriveKey pins the signing-key derivation to the worked example
// from the AWS documentation "Examples of how to derive a signing key for
// Signature Version 4" (date 20120215, region us-east-1, service iam).
func TestSigV4DeriveKey(t *testing.T) {
	key := sigV4DeriveKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20120215", "us-east-1", "iam")
	const want = "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
	if got := fmt.Sprintf("%x", key); got != want {
		t.Fatalf("signing key mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestSigV4CanonicalQuery(t *testing.T) {
	// Strict encoding: space is %20 (never '+'), reserved bytes escaped,
	// parameters sorted by encoded name then value. The input is a raw
	// (already wire-encoded) query; it round-trips unchanged when properly
	// encoded.
	got := sigV4CanonicalQuery("z=last&a%20b=m%2Bn&a%20b=x%2Fy")
	const want = "a%20b=m%2Bn&a%20b=x%2Fy&z=last"
	if got != want {
		t.Fatalf("canonical query:\n got: %s\nwant: %s", got, want)
	}
	// A wire '+' decodes to a space (matching what the server sees) and is
	// re-encoded strictly as %20.
	if got := sigV4CanonicalQuery("b=x&a=two+words"); got != "a=two%20words&b=x" {
		t.Fatalf("plus normalization: %q", got)
	}
	if got := sigV4CanonicalQuery(""); got != "" {
		t.Fatalf("empty query: %q", got)
	}
	// sigV4EncodeQuery output is stable under the parse/re-encode round
	// trip, so the signed string always equals the sent string.
	values := url.Values{"prefix": {"a b/c"}, "continuation-token": {"tok/+="}, "list-type": {"2"}}
	encoded := sigV4EncodeQuery(values)
	if got := sigV4CanonicalQuery(encoded); got != encoded {
		t.Fatalf("round trip unstable: %q -> %q", encoded, got)
	}
}
