package s3backend

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// HTTPConfig configures NewHTTPClient.
type HTTPConfig struct {
	// Endpoint is the S3 endpoint URL, e.g.
	// "https://s3.us-east-1.amazonaws.com" or "http://localhost:9000" for
	// MinIO. Required; the scheme must be http or https.
	Endpoint string
	// Region is the SigV4 signing region, e.g. "us-east-1". Required.
	Region string
	// Bucket is the bucket all operations are bound to. Required.
	Bucket string
	// AccessKey and SecretKey are the static credentials used for signing.
	// Required.
	AccessKey string
	SecretKey string
	// SessionToken is an optional STS session token sent as
	// X-Amz-Security-Token and included in the signature.
	SessionToken string
	// PathStyle selects path-style addressing (endpoint/bucket/key) instead
	// of the default virtual-hosted-style (bucket.endpoint/key). Use it for
	// MinIO and most S3-compatible stores, or when the bucket name contains
	// dots and TLS certificate matching would fail.
	PathStyle bool
	// HTTPClient issues the requests. Nil installs a default client with
	// sane timeouts and connection pooling.
	HTTPClient *http.Client
	// Prefix is an optional key prefix prepended to every key, allowing many
	// logical backends to share one bucket. List results have it stripped
	// again, so it is invisible to callers.
	Prefix string
}

// HTTPClient is a Backend backed by a real S3 (or S3-compatible) HTTP API,
// signed with AWS Signature Version 4. It is safe for concurrent use.
//
// HTTPClient performs no retries: transient failures are reported as
// *Error with Retryable set, and callers (e.g. package cas) retry per their
// configured policy.
type HTTPClient struct {
	cfg      HTTPConfig
	client   *http.Client
	endpoint *url.URL
	signer   sigV4Signer
	basePath string // optional path component of Endpoint, without trailing "/"
}

var _ Backend = (*HTTPClient)(nil)

// NewHTTPClient validates cfg and returns an HTTPClient bound to the
// configured endpoint, region, and bucket.
func NewHTTPClient(cfg HTTPConfig) (*HTTPClient, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("s3backend: HTTPConfig.Endpoint is required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3backend: invalid Endpoint: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("s3backend: Endpoint scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("s3backend: Endpoint %q has no host", cfg.Endpoint)
	}
	if cfg.Region == "" {
		return nil, errors.New("s3backend: HTTPConfig.Region is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3backend: HTTPConfig.Bucket is required")
	}
	if cfg.AccessKey == "" {
		return nil, errors.New("s3backend: HTTPConfig.AccessKey is required")
	}
	if cfg.SecretKey == "" {
		return nil, errors.New("s3backend: HTTPConfig.SecretKey is required")
	}
	if hasControlChars(cfg.Prefix) {
		return nil, errors.New("s3backend: HTTPConfig.Prefix contains control characters")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = defaultS3HTTPClient()
	}
	return &HTTPClient{
		cfg:      cfg,
		client:   client,
		endpoint: u,
		signer: sigV4Signer{
			accessKey:    cfg.AccessKey,
			secretKey:    cfg.SecretKey,
			sessionToken: cfg.SessionToken,
			region:       cfg.Region,
			service:      "s3",
		},
		basePath: strings.TrimSuffix(u.Path, "/"),
	}, nil
}

// defaultS3HTTPClient returns the client used when HTTPConfig.HTTPClient is
// nil: pooled connections, dial/TLS/response-header timeouts, and no
// redirect following (a 301 on a mutating request must surface as an error,
// not be silently re-sent as a GET).
func defaultS3HTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   32,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Get implements Backend.Get. A 404 response maps to ErrNotFound; the
// returned ETag is unquoted and ModTime comes from the Last-Modified header.
func (c *HTTPClient) Get(ctx context.Context, key string) (*Object, error) {
	if err := validateObjectKey("Get", key); err != nil {
		return nil, err
	}
	req, err := c.newRequest(ctx, http.MethodGet, c.keyURL(key), nil, nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, transportError(ctx, "Get", key, err)
	}
	defer drainAndClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("get %q: %w", key, ErrNotFound)
	case resp.StatusCode/100 != 2:
		return nil, decodeS3Error("Get", key, resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, transportError(ctx, "Get", key, err)
	}
	modTime, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return &Object{
		Key:     key,
		Body:    body,
		ETag:    unquoteETag(resp.Header.Get("ETag")),
		ModTime: modTime,
	}, nil
}

// Put implements Backend.Put. Preconditions map to the If-None-Match: * and
// If-Match: <etag> request headers; a 412 response maps to
// ErrPreconditionFailed. On success the new (unquoted) ETag is returned.
func (c *HTTPClient) Put(ctx context.Context, key string, body []byte, pre *Preconditions) (string, error) {
	if err := validateObjectKey("Put", key); err != nil {
		return "", err
	}
	var hdr http.Header
	if pre != nil {
		hdr = http.Header{}
		if pre.IfNoneMatch {
			hdr.Set("If-None-Match", "*")
		}
		if pre.IfMatchETag != "" {
			hdr.Set("If-Match", quoteETag(pre.IfMatchETag))
		}
	}
	req, err := c.newRequest(ctx, http.MethodPut, c.keyURL(key), body, hdr, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", transportError(ctx, "Put", key, err)
	}
	defer drainAndClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusPreconditionFailed:
		return "", fmt.Errorf("put %q: %w", key, ErrPreconditionFailed)
	case resp.StatusCode/100 != 2:
		return "", decodeS3Error("Put", key, resp)
	}
	return unquoteETag(resp.Header.Get("ETag")), nil
}

// Delete implements Backend.Delete. Deleting a missing key is not an error:
// any 2xx or 404 response is success.
func (c *HTTPClient) Delete(ctx context.Context, key string) error {
	if err := validateObjectKey("Delete", key); err != nil {
		return err
	}
	req, err := c.newRequest(ctx, http.MethodDelete, c.keyURL(key), nil, nil, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return transportError(ctx, "Delete", key, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return decodeS3Error("Delete", key, resp)
}

// maxListKeys is the S3 protocol limit for a single ListObjectsV2 page.
const maxListKeys = 1000

// List implements Backend.List using ListObjectsV2. MaxKeys is capped at
// 1000 (the S3 protocol limit). When HTTPConfig.Prefix is set it is applied
// to the list prefix and StartAfter, and stripped from returned keys.
func (c *HTTPClient) List(ctx context.Context, prefix string, opts *ListOptions) (*ListPage, error) {
	if hasControlChars(prefix) {
		return nil, &Error{Op: "List", Key: prefix, Code: "InvalidArgument",
			Message: "prefix contains control characters"}
	}
	query := url.Values{}
	query.Set("list-type", "2")
	query.Set("prefix", c.cfg.Prefix+prefix)
	if opts != nil {
		switch {
		case opts.MaxKeys > maxListKeys:
			query.Set("max-keys", strconv.Itoa(maxListKeys))
		case opts.MaxKeys > 0:
			query.Set("max-keys", strconv.Itoa(opts.MaxKeys))
		}
		if opts.ContinuationToken != "" {
			query.Set("continuation-token", opts.ContinuationToken)
		} else if opts.StartAfter != "" {
			if hasControlChars(opts.StartAfter) {
				return nil, &Error{Op: "List", Key: opts.StartAfter, Code: "InvalidArgument",
					Message: "start-after key contains control characters"}
			}
			query.Set("start-after", c.cfg.Prefix+opts.StartAfter)
		}
	}
	req, err := c.newRequest(ctx, http.MethodGet, c.bucketURL(), nil, nil, query)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, transportError(ctx, "List", prefix, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		return nil, decodeS3Error("List", prefix, resp)
	}
	var result listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, &Error{Op: "List", Key: prefix, Code: "InvalidResponse",
			Message: "cannot decode ListObjectsV2 response: " + err.Error()}
	}
	page := &ListPage{
		IsTruncated:           result.IsTruncated,
		NextContinuationToken: result.NextContinuationToken,
	}
	for _, obj := range result.Contents {
		modTime, _ := time.Parse(time.RFC3339, obj.LastModified)
		page.Objects = append(page.Objects, ObjectInfo{
			Key:     strings.TrimPrefix(obj.Key, c.cfg.Prefix),
			ETag:    unquoteETag(obj.ETag),
			Size:    obj.Size,
			ModTime: modTime,
		})
	}
	return page, nil
}

// bucketURL builds the URL for bucket-level operations (List).
func (c *HTTPClient) bucketURL() *url.URL {
	u := &url.URL{Scheme: c.endpoint.Scheme}
	if c.cfg.PathStyle {
		u.Host = c.endpoint.Host
		u.Path = c.basePath + "/" + c.cfg.Bucket
		u.RawPath = c.basePath + "/" + sigV4Escape(c.cfg.Bucket)
	} else {
		u.Host = c.cfg.Bucket + "." + c.endpoint.Host
		u.Path = "/"
	}
	return u
}

// keyURL builds the URL for the object at key (an app-level key; the
// configured Prefix is applied). Path carries the decoded key and RawPath
// its strict SigV4-escaped form, so the wire bytes match the signed bytes.
func (c *HTTPClient) keyURL(key string) *url.URL {
	full := c.cfg.Prefix + key
	u := &url.URL{Scheme: c.endpoint.Scheme}
	if c.cfg.PathStyle {
		u.Host = c.endpoint.Host
		u.Path = c.basePath + "/" + c.cfg.Bucket + "/" + full
		u.RawPath = c.basePath + "/" + sigV4Escape(c.cfg.Bucket) + "/" + sigV4EscapePath(full)
	} else {
		u.Host = c.cfg.Bucket + "." + c.endpoint.Host
		u.Path = "/" + full
		u.RawPath = "/" + sigV4EscapePath(full)
	}
	return u
}

// newRequest builds, payload-hashes, and SigV4-signs one request.
func (c *HTTPClient) newRequest(ctx context.Context, method string, u *url.URL, body []byte, header http.Header, query url.Values) (*http.Request, error) {
	if query != nil {
		u.RawQuery = sigV4EncodeQuery(query)
	}
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return nil, fmt.Errorf("s3backend: build %s request: %w", method, err)
	}
	for name, values := range header {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	payloadHash := hexSHA256(body)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	c.signer.sign(req, payloadHash, time.Now())
	return req, nil
}

// listBucketResult is the subset of the ListObjectsV2 XML response the
// Backend contract needs.
type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	} `xml:"Contents"`
}

// s3ErrorResponse is the S3 XML error document.
type s3ErrorResponse struct {
	XMLName  xml.Name `xml:"Error"`
	Code     string   `xml:"Code"`
	Message  string   `xml:"Message"`
	Resource string   `xml:"Resource"`
}

// decodeS3Error maps a non-2xx response to *Error, parsing the S3 XML error
// body when present. 500/503 status codes and the SlowDown/RequestTimeout
// codes are marked Retryable.
func decodeS3Error(op, key string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var xerr s3ErrorResponse
	code, message := "", ""
	if len(body) > 0 && xml.Unmarshal(body, &xerr) == nil {
		code, message = xerr.Code, xerr.Message
	}
	if code == "" {
		code = http.StatusText(resp.StatusCode)
		if code == "" {
			code = "Unknown"
		}
	}
	if message == "" {
		if message = strings.TrimSpace(string(body)); message == "" {
			message = resp.Status
		}
	}
	return &Error{
		Op:         op,
		Key:        key,
		StatusCode: resp.StatusCode,
		Code:       code,
		Message:    message,
		Retryable:  isRetryableS3Error(resp.StatusCode, code),
	}
}

// isRetryableS3Error reports whether an S3 status/code pair denotes a
// transient failure worth retrying.
func isRetryableS3Error(status int, code string) bool {
	if code == "SlowDown" || code == "RequestTimeout" {
		return true
	}
	return status == http.StatusInternalServerError || status == http.StatusServiceUnavailable
}

// transportError maps a transport-level failure. Context cancellation and
// deadline errors propagate as-is; anything else is a retryable *Error.
func transportError(ctx context.Context, op, key string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return &Error{Op: op, Key: key, Code: "TransportError", Message: err.Error(), Retryable: true}
}

// drainAndClose drains (bounded, so the connection can be reused) and closes
// a response body.
func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<20))
	_ = body.Close()
}

// validateObjectKey rejects empty keys and keys with control characters
// before any request is issued.
func validateObjectKey(op, key string) error {
	if key == "" {
		return &Error{Op: op, Code: "InvalidArgument", Message: "key must not be empty"}
	}
	if hasControlChars(key) {
		return &Error{Op: op, Key: key, Code: "InvalidArgument",
			Message: "key contains control characters"}
	}
	return nil
}

// hasControlChars reports whether s contains bytes < 0x20 or 0x7f.
func hasControlChars(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}

// unquoteETag strips an optional weak-validator prefix and surrounding
// quotes from an ETag header value.
func unquoteETag(etag string) string {
	etag = strings.TrimPrefix(etag, "W/")
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}

// quoteETag wraps an ETag in double quotes for conditional request headers,
// unless it is already quoted.
func quoteETag(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag
	}
	return `"` + etag + `"`
}
