package s3backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
)

func (c *Chaos) GetStream(ctx context.Context, key string) (*StreamObject, error) {
	if err := c.pre(ctx, "GetStream", key); err != nil {
		return nil, err
	}
	if b, ok := c.backend.(StreamBackend); ok {
		return b.GetStream(ctx, key)
	}
	o, err := c.backend.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &StreamObject{Key: o.Key, Body: nopCloser{bytes.NewReader(o.Body)}, ETag: o.ETag, ModTime: o.ModTime, Size: int64(len(o.Body))}, nil
}
func (c *Chaos) PutStream(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) error {
	if size < 0 || r == nil {
		return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: "invalid reader or size"}
	}
	if err := c.pre(ctx, "PutStream", key); err != nil {
		return err
	}
	var err error
	if b, ok := c.backend.(StreamBackend); ok {
		err = b.PutStream(ctx, key, r, size, pre)
	} else {
		body, e := io.ReadAll(io.LimitReader(r, size+1))
		if e != nil {
			return e
		}
		if int64(len(body)) != size {
			return &Error{Op: "PutStream", Key: key, Code: "InvalidArgument", Message: "size mismatch"}
		}
		_, err = c.backend.Put(ctx, key, body, pre)
	}
	if err == nil && c.roll(c.cfg.AmbiguousWriteRate) {
		return fmt.Errorf("putstream %q applied but reported failed: %w", key, c.injectError("PutStream", key))
	}
	return err
}
func (c *Chaos) GetRange(ctx context.Context, key string, offset, length int64) (*StreamObject, error) {
	if err := c.pre(ctx, "GetRange", key); err != nil {
		return nil, err
	}
	if b, ok := c.backend.(RangeBackend); ok {
		return b.GetRange(ctx, key, offset, length)
	}
	if offset < 0 || length <= 0 || offset > math.MaxInt64-(length-1) {
		return nil, &Error{Op: "GetRange", Key: key, Code: "InvalidArgument", Message: "invalid range"}
	}
	o, err := c.backend.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if offset >= int64(len(o.Body)) {
		return nil, &Error{Op: "GetRange", Key: key, StatusCode: 416, Code: "InvalidRange", Message: "range starts beyond object"}
	}
	end := offset + length
	if end > int64(len(o.Body)) {
		end = int64(len(o.Body))
	}
	body := append([]byte(nil), o.Body[offset:end]...)
	return &StreamObject{Key: o.Key, Body: nopCloser{bytes.NewReader(body)}, ETag: o.ETag, ModTime: o.ModTime, Size: int64(len(body))}, nil
}
func (c *Chaos) Stat(ctx context.Context, key string) (*StatObject, error) {
	if err := c.pre(ctx, "Stat", key); err != nil {
		return nil, err
	}
	if b, ok := c.backend.(StatBackend); ok {
		return b.Stat(ctx, key)
	}
	o, err := c.backend.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &StatObject{Key: o.Key, ETag: o.ETag, ModTime: o.ModTime, Size: int64(len(o.Body))}, nil
}

func (c *Chaos) PutMultipart(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) error {
	if size < 0 || r == nil {
		return &Error{Op: "PutMultipart", Key: key, Code: "InvalidArgument", Message: "invalid reader or size"}
	}
	if err := c.pre(ctx, "PutMultipart", key); err != nil {
		return err
	}
	var err error
	if b, ok := c.backend.(MultipartBackend); ok {
		err = b.PutMultipart(ctx, key, r, size, pre)
	} else if b, ok := c.backend.(StreamBackend); ok {
		err = b.PutStream(ctx, key, r, size, pre)
	} else {
		body, e := io.ReadAll(io.LimitReader(r, size+1))
		if e != nil {
			return e
		}
		if int64(len(body)) != size {
			return &Error{Op: "PutMultipart", Key: key, Code: "InvalidArgument", Message: "size mismatch"}
		}
		_, err = c.backend.Put(ctx, key, body, pre)
	}
	if err == nil && c.roll(c.cfg.AmbiguousWriteRate) {
		return fmt.Errorf("putmultipart %q applied but reported failed: %w", key, c.injectError("PutMultipart", key))
	}
	return err
}

var _ MultipartBackend = (*Chaos)(nil)
