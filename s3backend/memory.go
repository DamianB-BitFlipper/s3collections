package s3backend

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is an in-memory Backend with strong S3 semantics: every mutation is
// immediately visible to all readers, preconditions are checked atomically
// under a single lock, and listings are strongly consistent snapshots.
//
// It is intended for unit and concurrency tests of s3collections structures
// and for library consumers' own tests. It is not a general S3 emulator: it
// implements only the Backend contract.
type Memory struct {
	mu      sync.Mutex
	objects map[string]memoryObject
	// version is a global counter guaranteeing unique, ever-changing ETags.
	version uint64
	// now overrides the clock for tests; nil means time.Now().UTC().
	now func() time.Time
}

type memoryObject struct {
	body    []byte
	etag    string
	modTime time.Time
}

// NewMemory returns an empty in-memory backend.
func NewMemory() *Memory {
	return &Memory{objects: make(map[string]memoryObject)}
}

// SetClock fixes the backend clock, for deterministic tests.
func (m *Memory) SetClock(now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
}

func (m *Memory) clockNow() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now().UTC()
}

// Len reports the number of stored objects. Test helper.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objects)
}

func (m *Memory) Get(ctx context.Context, key string) (*Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("get %q: %w", key, ErrNotFound)
	}
	body := make([]byte, len(o.body))
	copy(body, o.body)
	return &Object{Key: key, Body: body, ETag: o.etag, ModTime: o.modTime}, nil
}

func (m *Memory) Put(ctx context.Context, key string, body []byte, pre *Preconditions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.objects[key]
	if pre != nil {
		if pre.IfNoneMatch && exists {
			return "", fmt.Errorf("put %q: if-none-match: %w", key, ErrPreconditionFailed)
		}
		if pre.IfMatchETag != "" && (!exists || cur.etag != pre.IfMatchETag) {
			return "", fmt.Errorf("put %q: if-match: %w", key, ErrPreconditionFailed)
		}
	}
	m.version++
	etag := fmt.Sprintf("%016x", m.version)
	stored := make([]byte, len(body))
	copy(stored, body)
	m.objects[key] = memoryObject{body: stored, etag: etag, modTime: m.clockNow()}
	return etag, nil
}

func (m *Memory) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

const defaultMaxKeys = 1000

func (m *Memory) List(ctx context.Context, prefix string, opts *ListOptions) (*ListPage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var startAfter string
	maxKeys := defaultMaxKeys
	if opts != nil {
		startAfter = opts.StartAfter
		if opts.ContinuationToken != "" {
			// Continuation tokens are opaque and strictly validated, like
			// real S3: only tokens previously issued by List are accepted.
			// This deliberately catches callers that synthesize tokens from
			// keys (valid only against lenient fakes, never against S3).
			k, err := decodeContinuationToken(opts.ContinuationToken)
			if err != nil {
				return nil, err
			}
			startAfter = k
		}
		if opts.MaxKeys > 0 {
			maxKeys = opts.MaxKeys
		}
	}
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		if k >= prefix && len(k) >= len(prefix) && k[:len(prefix)] == prefix && k > startAfter {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	page := &ListPage{}
	if len(keys) > maxKeys {
		page.IsTruncated = true
		keys = keys[:maxKeys]
		page.NextContinuationToken = encodeContinuationToken(keys[len(keys)-1])
	}
	for _, k := range keys {
		o := m.objects[k]
		page.Objects = append(page.Objects, ObjectInfo{Key: k, ETag: o.etag, Size: int64(len(o.body)), ModTime: o.modTime})
	}
	return page, nil
}

// continuationTokenPrefix marks opaque continuation tokens issued by List.
const continuationTokenPrefix = "ctok-v1."

func encodeContinuationToken(lastKey string) string {
	return continuationTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(lastKey))
}

func decodeContinuationToken(tok string) (string, error) {
	if !strings.HasPrefix(tok, continuationTokenPrefix) {
		return "", &Error{Op: "List", StatusCode: 400, Code: "InvalidContinuationToken",
			Message: "continuation token was not issued by this backend (tokens are opaque; never synthesize them from keys)", Retryable: false}
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(tok, continuationTokenPrefix))
	if err != nil {
		return "", &Error{Op: "List", StatusCode: 400, Code: "InvalidContinuationToken",
			Message: "malformed continuation token", Retryable: false}
	}
	return string(raw), nil
}
