// memory.go provides in-memory implementations of KV and BlobStore. They
// are concurrency-safe and suitable for tests and single-process use.
package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"sync"
)

// MemoryKV is an in-memory KV store. Transactions are serializable: each
// transaction runs against a stable snapshot under an exclusive lock, and
// its writes are committed atomically when the callback returns nil.
type MemoryKV struct {
	mu     sync.Mutex
	data   map[string][]byte
	closed bool
}

// NewMemoryKV returns an empty in-memory KV store.
func NewMemoryKV() *MemoryKV {
	return &MemoryKV{data: make(map[string][]byte)}
}

func (m *MemoryKV) checkLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.closed {
		return ErrClosed
	}
	return nil
}

// Get returns a copy of the value for key, or ErrNotFound.
func (m *MemoryKV) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return nil, err
	}
	v, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.Clone(v), nil
}

// Put stores a copy of value under key.
func (m *MemoryKV) Put(ctx context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return err
	}
	m.data[key] = bytes.Clone(value)
	return nil
}

// Delete removes key. Deleting a missing key is not an error.
func (m *MemoryKV) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return err
	}
	delete(m.data, key)
	return nil
}

// Scan returns the entries matching opts in key order (descending when
// opts.Reverse is set).
func (m *MemoryKV) Scan(ctx context.Context, opts ScanOptions) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return nil, err
	}
	return scanEntries(m.data, opts), nil
}

// scanEntries filters a key/value map by opts. Callers must hold the lock.
func scanEntries(data map[string][]byte, opts ScanOptions) []Entry {
	keys := make([]string, 0, len(data))
	for k := range data {
		if opts.Prefix != "" && !strings.HasPrefix(k, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" {
			if (!opts.Reverse && k <= opts.StartAfter) || (opts.Reverse && k >= opts.StartAfter) {
				continue
			}
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	if opts.Limit > 0 && len(keys) > opts.Limit {
		keys = keys[:opts.Limit]
	}
	out := make([]Entry, 0, len(keys))
	for _, k := range keys {
		out = append(out, Entry{Key: k, Value: bytes.Clone(data[k])})
	}
	return out
}

// Transaction runs fn in a serializable transaction. fn sees a stable
// snapshot; its writes are buffered and committed atomically when fn
// returns nil, and discarded otherwise.
func (m *MemoryKV) Transaction(ctx context.Context, fn func(Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return err
	}
	tx := &memoryTx{
		snap:    m.data, // read-only view; commit writes go to pending
		pending: make(map[string]*[]byte),
	}
	if err := fn(tx); err != nil {
		return err // roll back: pending writes are discarded
	}
	for k, v := range tx.pending {
		if v == nil {
			delete(m.data, k)
		} else {
			m.data[k] = *v
		}
	}
	return nil
}

// memoryTx buffers writes in pending; a nil *[]byte marks a deletion.
// Reads see pending writes overlaid on the snapshot.
type memoryTx struct {
	snap    map[string][]byte
	pending map[string]*[]byte
}

func (t *memoryTx) Get(key string) ([]byte, error) {
	if v, ok := t.pending[key]; ok {
		if v == nil {
			return nil, ErrNotFound
		}
		return bytes.Clone(*v), nil
	}
	v, ok := t.snap[key]
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.Clone(v), nil
}

func (t *memoryTx) Put(key string, value []byte) error {
	v := bytes.Clone(value)
	t.pending[key] = &v
	return nil
}

func (t *memoryTx) Delete(key string) error {
	t.pending[key] = nil
	return nil
}

func (t *memoryTx) Scan(opts ScanOptions) ([]Entry, error) {
	// Merge the snapshot with pending writes into a combined view.
	view := make(map[string][]byte, len(t.snap)+len(t.pending))
	for k, v := range t.snap {
		view[k] = v
	}
	for k, v := range t.pending {
		if v == nil {
			delete(view, k)
		} else {
			view[k] = *v
		}
	}
	return scanEntries(view, opts), nil
}

// Close marks the store closed. Later operations return ErrClosed.
func (m *MemoryKV) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.closed = true
	return nil
}

// MemoryBlobStore is an in-memory blob store.
type MemoryBlobStore struct {
	mu     sync.Mutex
	blobs  map[string]memoryBlob
	closed bool
}

type memoryBlob struct {
	data []byte
	etag string
}

// NewMemoryBlobStore returns an empty in-memory blob store.
func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{blobs: make(map[string]memoryBlob)}
}

func (m *MemoryBlobStore) checkLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.closed {
		return ErrClosed
	}
	return nil
}

// Put streams r into memory. When size >= 0 the number of bytes read must
// equal size exactly, otherwise ErrSizeMismatch is returned and the blob is
// not stored.
func (m *MemoryBlobStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if size >= 0 {
		data, err := readExactSize(ctx, r, size)
		if err != nil {
			return err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if err := m.checkLocked(ctx); err != nil {
			return err
		}
		m.blobs[key] = newMemoryBlob(data)
		return nil
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return err
	}
	m.blobs[key] = newMemoryBlob(data)
	return nil
}

func newMemoryBlob(data []byte) memoryBlob {
	sum := md5.Sum(data)
	return memoryBlob{data: data, etag: hex.EncodeToString(sum[:])}
}

// readExactSize streams through a bounded buffer. MemoryBlobStore must retain
// the final object by definition, but it never allocates solely from an
// untrusted declared size before bytes arrive.
func readExactSize(ctx context.Context, r io.Reader, size int64) ([]byte, error) {
	if size < 0 {
		return nil, ErrSizeMismatch
	}
	var buf bytes.Buffer
	if size < 32<<20 {
		buf.Grow(int(size))
	}
	limited := io.LimitReader(&contextReader{ctx: ctx, r: r}, size+1)
	n, err := io.Copy(&buf, limited)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, ErrSizeMismatch
	}
	return buf.Bytes(), nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// Open returns a reader over the full blob.
func (m *MemoryBlobStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return nil, err
	}
	b, ok := m.blobs[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

// OpenRange returns a reader over the byte range [start, end) of the blob.
func (m *MemoryBlobStore) OpenRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return nil, err
	}
	b, ok := m.blobs[key]
	if !ok {
		return nil, ErrNotFound
	}
	size := int64(len(b.data))
	if start < 0 || end < start || end > size {
		return nil, ErrSizeMismatch
	}
	return io.NopCloser(bytes.NewReader(b.data[start:end])), nil
}

// Stat returns metadata for a blob.
func (m *MemoryBlobStore) Stat(ctx context.Context, key string) (BlobInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return BlobInfo{}, err
	}
	b, ok := m.blobs[key]
	if !ok {
		return BlobInfo{}, ErrNotFound
	}
	return BlobInfo{Key: key, Size: int64(len(b.data)), ETag: b.etag}, nil
}

// Delete removes a blob. Deleting a missing blob is not an error.
func (m *MemoryBlobStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return err
	}
	delete(m.blobs, key)
	return nil
}

// Close marks the store closed. Later operations return ErrClosed.
func (m *MemoryBlobStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.closed = true
	return nil
}
