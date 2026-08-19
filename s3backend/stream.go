// Package s3backend optional streaming, range, stat, and multipart capabilities.
//
// The interfaces in this file are additive to Backend. A backend advertises
// only the capabilities it actually implements; consumers must type-assert.
package s3backend

import (
	"context"
	"io"
	"time"
)

// StreamObject is an open object body. The caller must close Body.
type StreamObject struct {
	Key     string
	Body    io.ReadCloser
	ETag    string
	ModTime time.Time
	Size    int64
}

// StreamBackend supports body streaming without a whole-object byte slice.
type StreamBackend interface {
	GetStream(ctx context.Context, key string) (*StreamObject, error)
	PutStream(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) error
}

// RangeBackend supports half-open byte range reads [offset, offset+length).
type RangeBackend interface {
	GetRange(ctx context.Context, key string, offset, length int64) (*StreamObject, error)
}

// StatObject is object metadata returned without the body.
type StatObject struct {
	Key     string
	ETag    string
	ModTime time.Time
	Size    int64
}

// StatBackend supports metadata-only object reads (HEAD on S3).
type StatBackend interface {
	Stat(ctx context.Context, key string) (*StatObject, error)
}

// MultipartBackend is an optional high-level, portable multipart capability.
// The implementation owns part sizing, retries, completion, and abort. It
// must not use provider-specific append/write-offset extensions. An
// implementation may reject conditional multipart when its S3 provider
// cannot enforce completion preconditions; callers can then fall back to a
// conditional StreamBackend.PutStream.
type MultipartBackend interface {
	PutMultipart(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) error
}

var (
	_ StreamBackend    = (*HTTPClient)(nil)
	_ RangeBackend     = (*HTTPClient)(nil)
	_ StatBackend      = (*HTTPClient)(nil)
	_ StreamBackend    = (*Memory)(nil)
	_ RangeBackend     = (*Memory)(nil)
	_ StatBackend      = (*Memory)(nil)
	_ MultipartBackend = (*Memory)(nil)
	_ StreamBackend    = (*Chaos)(nil)
	_ RangeBackend     = (*Chaos)(nil)
	_ StatBackend      = (*Chaos)(nil)
)
