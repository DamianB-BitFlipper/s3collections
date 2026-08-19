package s3backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
)

type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

func (m *Memory) GetStream(ctx context.Context, key string) (*StreamObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("getstream %q: %w", key, ErrNotFound)
	}
	body := append([]byte(nil), o.body...)
	return &StreamObject{Key: key, Body: nopCloser{bytes.NewReader(body)}, ETag: o.etag, ModTime: o.modTime, Size: int64(len(body))}, nil
}

func (m *Memory) PutStream(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if size < 0 || size > int64(math.MaxInt) {
		return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: "invalid size"}
	}
	if r == nil {
		return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: "nil reader"}
	}
	body, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return &Error{Op: "PutStream", Key: key, Code: "ReadError", Message: err.Error()}
	}
	if int64(len(body)) != size {
		return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: fmt.Sprintf("reader yielded %d bytes, expected %d", len(body), size)}
	}
	_, err = m.Put(ctx, key, body, pre)
	return err
}

func (m *Memory) GetRange(ctx context.Context, key string, offset, length int64) (*StreamObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 || offset > math.MaxInt64-length {
		return nil, &Error{Op: "GetRange", Key: key, Code: "InvalidArgument", Message: "invalid range"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("getrange %q: %w", key, ErrNotFound)
	}
	if offset >= int64(len(o.body)) {
		return nil, &Error{Op: "GetRange", Key: key, StatusCode: 416, Code: "InvalidRange", Message: "range starts beyond object"}
	}
	end := offset + length
	if end > int64(len(o.body)) {
		end = int64(len(o.body))
	}
	body := append([]byte(nil), o.body[offset:end]...)
	return &StreamObject{Key: key, Body: nopCloser{bytes.NewReader(body)}, ETag: o.etag, ModTime: o.modTime, Size: int64(len(body))}, nil
}

func (m *Memory) Stat(ctx context.Context, key string) (*StatObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("stat %q: %w", key, ErrNotFound)
	}
	return &StatObject{Key: key, ETag: o.etag, ModTime: o.modTime, Size: int64(len(o.body))}, nil
}

// PutMultipart gives tests and in-memory users the same high-level semantic
// capability as a real standard multipart implementation.
func (m *Memory) PutMultipart(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) error {
	return m.PutStream(ctx, key, r, size, pre)
}
