// Package storage defines the storage abstraction layer for s3collections:
// key/value access with serializable transactions, streaming blob storage,
// and pluggable backends (in-memory, SlateDB, AWS S3).
package storage

import (
	"context"
	"errors"
	"io"
	"sync"
)

// Sentinel errors returned by storage engines.
var (
	// ErrNotFound is returned when a key does not exist.
	ErrNotFound = errors.New("storage: not found")
	// ErrConflict is returned when a transaction cannot be applied
	// without violating serializability.
	ErrConflict = errors.New("storage: conflict")
	// ErrClosed is returned when an operation is attempted on a closed engine.
	ErrClosed = errors.New("storage: closed")
	// ErrFenced is returned when a SlateDB writer has lost ownership.
	ErrFenced = errors.New("storage: writer fenced")
	// ErrSlateDBUnavailable is returned when the SlateDB engine is requested
	// from a binary built without the slatedb build tag.
	ErrSlateDBUnavailable = errors.New("storage: slatedb unavailable")
	// ErrSizeMismatch is returned when the number of bytes read during a
	// blob Put does not match the declared size.
	ErrSizeMismatch = errors.New("storage: size mismatch")
)

// Entry is a single key/value pair.
type Entry struct {
	Key   string
	Value []byte
}

// ScanOptions controls a range scan.
type ScanOptions struct {
	// Prefix restricts results to keys with this prefix.
	Prefix string
	// StartAfter makes the scan begin strictly after this key.
	StartAfter string
	// Limit caps the number of returned entries; <= 0 means no limit.
	Limit int
	// Reverse returns entries in descending key order.
	Reverse bool
}

// Tx is a serializable transaction. Changes are applied atomically when the
// enclosing Transaction callback returns nil; otherwise they roll back.
type Tx interface {
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Delete(key string) error
	Scan(opts ScanOptions) ([]Entry, error)
}

// KV provides context-aware key/value operations.
type KV interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	Scan(ctx context.Context, opts ScanOptions) ([]Entry, error)
	// Transaction runs fn in a serializable transaction. If fn returns an
	// error, all changes roll back.
	Transaction(ctx context.Context, fn func(Tx) error) error
	Close() error
}

// BlobInfo describes a stored blob.
type BlobInfo struct {
	Key  string
	Size int64
	// ETag is an opaque content identifier for the blob (e.g. an S3 ETag
	// or a content hash for in-memory backends). It may be empty.
	ETag string
}

// BlobStore stores large objects with streaming access.
type BlobStore interface {
	// Put streams r into the store under key. size is the exact number of
	// bytes that r will yield; pass < 0 when the length is unknown. When
	// size >= 0, implementations must return ErrSizeMismatch if the reader
	// yields a different number of bytes.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// Open returns the full blob as a stream.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// OpenRange returns the byte range [start, end) of the blob.
	OpenRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)
	// Stat returns metadata for a blob without reading it.
	Stat(ctx context.Context, key string) (BlobInfo, error)
	// Delete removes a blob. It is not an error to delete a missing blob.
	Delete(ctx context.Context, key string) error
	Close() error
}

// Engine is a complete storage backend: a KV store for metadata plus a blob
// store for large values.
type Engine struct {
	Metadata  KV
	Blobs     BlobStore
	closeOnce sync.Once
	closeErr  error
}

// NewEngine builds an Engine from a KV store and a blob store. Both must be
// non-nil.
func NewEngine(kv KV, blobs BlobStore) (*Engine, error) {
	if kv == nil {
		return nil, errors.New("storage: nil KV")
	}
	if blobs == nil {
		return nil, errors.New("storage: nil BlobStore")
	}
	return &Engine{Metadata: kv, Blobs: blobs}, nil
}

// Close closes both backing stores. If both fail, the first error is
// returned and the second is joined onto it.
func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		var kvErr, blobErr error
		if e.Metadata != nil {
			kvErr = e.Metadata.Close()
		}
		if e.Blobs != nil {
			blobErr = e.Blobs.Close()
		}
		if errors.Is(kvErr, ErrClosed) {
			kvErr = nil
		}
		if errors.Is(blobErr, ErrClosed) {
			blobErr = nil
		}
		e.closeErr = errors.Join(kvErr, blobErr)
	})
	return e.closeErr
}
