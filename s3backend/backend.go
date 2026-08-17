// Package s3backend defines the minimal S3 storage contract that all
// s3collections data structures are built on, plus an in-memory
// implementation with strong S3 semantics for tests and a fault-injection
// wrapper for chaos testing.
//
// The contract mirrors the exact S3 guarantees the library relies on:
//   - Strong read-after-write consistency for Get and List.
//   - Atomic per-key Put.
//   - Conditional writes on Put: create-if-absent (If-None-Match: *) and
//     compare-and-swap on ETag (If-Match).
//   - Unconditional per-key Delete (S3 has no conditional delete).
//   - Strongly consistent, paginated, lexicographic prefix listing.
//
// Nothing else is assumed: no multi-key transactions, no TTL, no events.
package s3backend

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Sentinel errors. Use errors.Is to match; they are always wrapped with
// operation context.
var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("s3backend: not found")
	// ErrPreconditionFailed is returned by Put when IfNoneMatch or
	// IfMatchETag does not hold (S3: 412 Precondition Failed).
	ErrPreconditionFailed = errors.New("s3backend: precondition failed")
)

// Error describes a non-sentinel backend failure, typically a transport or
// service-side error such as 500 InternalError or 503 SlowDown.
type Error struct {
	Op         string // e.g. "Get", "Put", "Delete", "List"
	Key        string // object key or list prefix, if applicable
	StatusCode int    // HTTP-like status code, e.g. 500, 503
	Code       string // S3-style code, e.g. "InternalError", "SlowDown"
	Message    string
	Retryable  bool
}

func (e *Error) Error() string {
	return fmt.Sprintf("s3backend: %s %q: status %d %s: %s", e.Op, e.Key, e.StatusCode, e.Code, e.Message)
}

// IsRetryable reports whether err is a transient backend error worth
// retrying. Sentinel errors (ErrNotFound, ErrPreconditionFailed) and
// context errors are not retryable in the backend sense.
func IsRetryable(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Retryable
	}
	return false
}

// Object is a stored object returned by Get.
type Object struct {
	Key  string
	Body []byte
	// ETag is an opaque token that changes on every write to the key.
	// Callers must treat it as uninterpreted.
	ETag string
	// ModTime is the backend-side last-modified time.
	ModTime time.Time
}

// ObjectInfo is the per-entry metadata returned by List.
type ObjectInfo struct {
	Key     string
	ETag    string
	Size    int64
	ModTime time.Time
}

// Preconditions expresses the conditional-write guards on a Put.
// The zero value is an unconditional write.
type Preconditions struct {
	// IfNoneMatch, when true, fails the Put with ErrPreconditionFailed if
	// the key already exists (S3: If-None-Match: *).
	IfNoneMatch bool
	// IfMatchETag, when non-empty, fails the Put with
	// ErrPreconditionFailed unless the key exists and its current ETag
	// equals this value (S3: If-Match: <etag>).
	IfMatchETag string
}

// ListOptions controls a single List call.
type ListOptions struct {
	// StartAfter lists keys lexicographically greater than this key.
	StartAfter string
	// ContinuationToken resumes a previous listing (mutually exclusive
	// with StartAfter; takes precedence).
	ContinuationToken string
	// MaxKeys bounds the page size. Zero means the backend default (1000).
	MaxKeys int
}

// ListPage is one page of a prefix listing.
type ListPage struct {
	Objects []ObjectInfo
	// IsTruncated reports whether more keys remain.
	IsTruncated bool
	// NextContinuationToken resumes the listing when IsTruncated is true.
	NextContinuationToken string
}

// Backend is the storage contract every s3collections structure uses.
// Implementations must provide strong read-after-write consistency and
// honor Put preconditions atomically with the write.
type Backend interface {
	// Get returns the object at key, or an error wrapping ErrNotFound.
	Get(ctx context.Context, key string) (*Object, error)
	// Put atomically checks pre and writes body, returning the new ETag.
	// A failed precondition yields an error wrapping ErrPreconditionFailed
	// and leaves the previous object untouched.
	Put(ctx context.Context, key string, body []byte, pre *Preconditions) (etag string, err error)
	// Delete removes key. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// List returns one page of keys under prefix in lexicographic order.
	List(ctx context.Context, prefix string, opts *ListOptions) (*ListPage, error)
}
