package s3backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func (c *HTTPClient) GetStream(ctx context.Context, key string) (*StreamObject, error) {
	if err := validateObjectKey("GetStream", key); err != nil {
		return nil, err
	}
	req, err := c.newRequest(ctx, http.MethodGet, c.keyURL(key), nil, nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, transportError(ctx, "GetStream", key, err)
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		drainAndClose(resp.Body)
		return nil, fmt.Errorf("getstream %q: %w", key, ErrNotFound)
	case resp.StatusCode != http.StatusOK:
		defer drainAndClose(resp.Body)
		if resp.StatusCode/100 != 2 {
			return nil, decodeS3Error("GetStream", key, resp)
		}
		return nil, &Error{Op: "GetStream", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "expected 200 OK"}
	}
	v := resp.Header.Get("Content-Length")
	size, parseErr := strconv.ParseInt(v, 10, 64)
	if parseErr != nil || size < 0 {
		defer drainAndClose(resp.Body)
		return nil, &Error{Op: "GetStream", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "missing or invalid Content-Length"}
	}
	mt, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return &StreamObject{Key: key, Body: &exactLengthReadCloser{r: resp.Body, remaining: size}, ETag: unquoteETag(resp.Header.Get("ETag")), ModTime: mt, Size: size}, nil
}

// PutStream uses a signed, length-delimited standard S3 PUT. The input is
// spooled to disk to compute SigV4's payload hash without retaining a large
// object in memory. No provider-specific write-offset API is used.
func (c *HTTPClient) PutStream(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) error {
	if err := validateObjectKey("PutStream", key); err != nil {
		return err
	}
	if size < 0 {
		return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: "size must be non-negative"}
	}
	if r == nil {
		return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: "nil reader"}
	}
	f, err := os.CreateTemp("", "s3backend-putstream-*")
	if err != nil {
		return &Error{Op: "PutStream", Key: key, Code: "SpoolError", Message: err.Error()}
	}
	name := f.Name()
	defer func() { _ = f.Close(); _ = os.Remove(name) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r, size+1))
	if err != nil {
		return &Error{Op: "PutStream", Key: key, Code: "ReadError", Message: err.Error()}
	}
	if n != size {
		return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: fmt.Sprintf("reader yielded %d bytes, expected %d", n, size)}
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return &Error{Op: "PutStream", Key: key, Code: "SpoolError", Message: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.keyURL(key).String(), f)
	if err != nil {
		return fmt.Errorf("s3backend: build PutStream request: %w", err)
	}
	if pre != nil {
		if pre.IfNoneMatch {
			req.Header.Set("If-None-Match", "*")
		}
		if pre.IfMatchETag != "" {
			req.Header.Set("If-Match", quoteETag(pre.IfMatchETag))
		}
	}
	req.ContentLength = size
	payloadHash := hex.EncodeToString(h.Sum(nil))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	c.signer.sign(req, payloadHash, time.Now())
	resp, err := c.client.Do(req)
	if err != nil {
		return transportError(ctx, "PutStream", key, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode == http.StatusPreconditionFailed {
		return fmt.Errorf("putstream %q: %w", key, ErrPreconditionFailed)
	}
	if resp.StatusCode/100 != 2 {
		return decodeS3Error("PutStream", key, resp)
	}
	return nil
}

func (c *HTTPClient) GetRange(ctx context.Context, key string, offset, length int64) (*StreamObject, error) {
	if err := validateObjectKey("GetRange", key); err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 || offset > math.MaxInt64-(length-1) {
		return nil, &Error{Op: "GetRange", Key: key, Code: "InvalidArgument", Message: "invalid range"}
	}
	requestedEnd := offset + length - 1
	req, err := c.newRequest(ctx, http.MethodGet, c.keyURL(key), nil, nil, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, requestedEnd))
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, transportError(ctx, "GetRange", key, err)
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		drainAndClose(resp.Body)
		return nil, fmt.Errorf("getrange %q: %w", key, ErrNotFound)
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		defer drainAndClose(resp.Body)
		return nil, decodeS3Error("GetRange", key, resp)
	case resp.StatusCode != http.StatusPartialContent:
		defer drainAndClose(resp.Body)
		if resp.StatusCode/100 != 2 {
			return nil, decodeS3Error("GetRange", key, resp)
		}
		return nil, &Error{Op: "GetRange", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "range request did not return 206"}
	}
	start, end, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
	expectedEnd := requestedEnd
	if total > 0 && total-1 < expectedEnd {
		expectedEnd = total - 1
	}
	if !ok || total <= 0 || start < 0 || end < start || end >= total || start != offset || end != expectedEnd {
		defer drainAndClose(resp.Body)
		return nil, &Error{Op: "GetRange", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "unexpected Content-Range: " + resp.Header.Get("Content-Range")}
	}
	size := end - start + 1
	v := resp.Header.Get("Content-Length")
	n, e := strconv.ParseInt(v, 10, 64)
	if e != nil || n < 0 || size <= 0 || n != size {
		defer drainAndClose(resp.Body)
		return nil, &Error{Op: "GetRange", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "Content-Length does not match Content-Range"}
	}
	mt, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return &StreamObject{Key: key, Body: &exactLengthReadCloser{r: resp.Body, remaining: size}, ETag: unquoteETag(resp.Header.Get("ETag")), ModTime: mt, Size: size}, nil
}

func parseContentRange(v string) (start, end, total int64, ok bool) {
	if !strings.HasPrefix(v, "bytes ") {
		return 0, 0, 0, false
	}
	v = strings.TrimPrefix(v, "bytes ")
	slash := strings.IndexByte(v, '/')
	dash := strings.IndexByte(v, '-')
	if dash <= 0 || slash <= dash+1 {
		return 0, 0, 0, false
	}
	var e error
	if start, e = strconv.ParseInt(v[:dash], 10, 64); e != nil {
		return 0, 0, 0, false
	}
	if end, e = strconv.ParseInt(v[dash+1:slash], 10, 64); e != nil {
		return 0, 0, 0, false
	}
	if v[slash+1:] != "*" {
		if total, e = strconv.ParseInt(v[slash+1:], 10, 64); e != nil {
			return 0, 0, 0, false
		}
	} else {
		total = -1
	}
	return start, end, total, true
}

type exactLengthReadCloser struct {
	r         io.ReadCloser
	remaining int64
	done      bool
}

func (r *exactLengthReadCloser) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n > 0 {
			return 0, errors.New("s3backend: range body exceeds declared length")
		}
		if err == io.EOF {
			r.done = true
			return 0, io.EOF
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	if err == io.EOF && r.remaining != 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}
func (r *exactLengthReadCloser) Close() error { return r.r.Close() }

func (c *HTTPClient) Stat(ctx context.Context, key string) (*StatObject, error) {
	if err := validateObjectKey("Stat", key); err != nil {
		return nil, err
	}
	req, err := c.newRequest(ctx, http.MethodHead, c.keyURL(key), nil, nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, transportError(ctx, "Stat", key, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("stat %q: %w", key, ErrNotFound)
	}
	if resp.StatusCode/100 != 2 {
		return nil, decodeS3Error("Stat", key, resp)
	}
	v := resp.Header.Get("Content-Length")
	size, parseErr := strconv.ParseInt(v, 10, 64)
	if parseErr != nil || size < 0 {
		return nil, &Error{Op: "Stat", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "missing or invalid Content-Length"}
	}
	mt, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return &StatObject{Key: key, ETag: unquoteETag(resp.Header.Get("ETag")), ModTime: mt, Size: size}, nil
}
