package lru

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/damianb/s3collections/storage"
)

// Sentinel errors returned by the LRU store.
var (
	// ErrNotFound is returned when a key does not exist in the store.
	ErrNotFound = errors.New("lru: not found")
	// ErrClosed is returned when an operation is attempted on a closed store.
	ErrClosed = errors.New("lru: closed")
	// ErrInvalidOptions is returned by New for inconsistent options.
	ErrInvalidOptions = errors.New("lru: invalid options")
	// ErrEvictorRunning is returned when StartEvictor is called twice.
	ErrEvictorRunning = errors.New("lru: evictor already running")
)

// EntryMeta is the mutable metadata stored for each entry.
type EntryMeta struct {
	// SizeBytes is the logical size of the entry, counted against
	// Options.CapacityBytes.
	SizeBytes int64
	// CreatedAt is when the entry was first created. Zero is replaced
	// with the current time on Set.
	CreatedAt time.Time
	// LastAccessAt drives eviction order. Zero is replaced with the
	// current time on Set; Touch refreshes it.
	LastAccessAt time.Time
}

// Entry is a stored item: its key, metadata, and revision.
type Entry struct {
	Key  string
	Meta EntryMeta
	// Revision starts at 1 on creation and is bumped transactionally on
	// every Set and Touch.
	Revision uint64
}

// Options configures a Store. Zero values select defaults.
type Options struct {
	// Prefix namespaces all keys in the underlying KV. Default "lru/".
	Prefix string
	// ShardCount is the number of key shards under Prefix. Default 16.
	ShardCount int
	// CapacityBytes is a soft cap on total SizeBytes; <= 0 disables it.
	CapacityBytes int64
	// CapacityItems is a soft cap on the number of entries; <= 0 disables it.
	CapacityItems int
	// TouchOnGet refreshes LastAccessAt (and bumps the revision) on Get.
	TouchOnGet bool
	// EvictorWorkers is the number of parallel delete workers used by the
	// evictor. Default 1. Victim selection is always deterministic.
	EvictorWorkers int
	// EvictorInterval is how often the evictor runs. Default 1 minute.
	EvictorInterval time.Duration
}

// Stats is a snapshot of store utilization.
type Stats struct {
	// Items is the number of stored entries.
	Items int
	// SizeBytes is the sum of EntryMeta.SizeBytes across all entries.
	SizeBytes int64
	// CapacityBytes echoes the configured soft byte cap.
	CapacityBytes int64
	// CapacityItems echoes the configured soft item cap.
	CapacityItems int
}

// record is the compact JSON form persisted in the KV.
type record struct {
	R uint64 `json:"r"` // revision
	S int64  `json:"s"` // size bytes
	C int64  `json:"c"` // created at, unix nanos
	A int64  `json:"a"` // last access at, unix nanos
}

const defaultPrefix = "lru/"

// Store is an LRU metadata store backed by a storage.KV.
type Store struct {
	kv   storage.KV
	opts Options

	mu      sync.Mutex
	closed  bool
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// New creates a Store on kv. It never takes ownership of kv: Close does
// not close it.
func New(kv storage.KV, opts Options) (*Store, error) {
	if kv == nil {
		return nil, fmt.Errorf("%w: nil KV", ErrInvalidOptions)
	}
	if opts.ShardCount < 0 || opts.ShardCount > 256 {
		return nil, fmt.Errorf("%w: ShardCount must be in [0,256]", ErrInvalidOptions)
	}
	if opts.ShardCount == 0 {
		opts.ShardCount = 16
	}
	if opts.Prefix == "" {
		opts.Prefix = defaultPrefix
	} else if !strings.HasSuffix(opts.Prefix, "/") {
		opts.Prefix += "/"
	}
	if opts.EvictorWorkers <= 0 {
		opts.EvictorWorkers = 1
	}
	if opts.EvictorInterval <= 0 {
		opts.EvictorInterval = time.Minute
	}
	return &Store{kv: kv, opts: opts}, nil
}

// shard returns the two-hex-digit shard bucket for key.
func (s *Store) shard(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return fmt.Sprintf("%02x", h.Sum32()%uint32(s.opts.ShardCount))
}

func (s *Store) fullKey(key string) string {
	return s.opts.Prefix + s.shard(key) + "/" + key
}

func (s *Store) transaction(ctx context.Context, fn func(storage.Tx) error) error {
	const maxAttempts = 16
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.kv.Transaction(ctx, fn)
		if !errors.Is(err, storage.ErrConflict) {
			return err
		}
	}
	return storage.ErrConflict
}

func (s *Store) checkOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

func encode(r record) []byte {
	b, _ := json.Marshal(r)
	return b
}

func decode(b []byte) (record, error) {
	var r record
	err := json.Unmarshal(b, &r)
	return r, err
}

func toEntry(key string, r record) Entry {
	return Entry{
		Key: key,
		Meta: EntryMeta{
			SizeBytes:    r.S,
			CreatedAt:    time.Unix(0, r.C).UTC(),
			LastAccessAt: time.Unix(0, r.A).UTC(),
		},
		Revision: r.R,
	}
}

// Get returns the entry for key. With TouchOnGet enabled it also
// refreshes LastAccessAt and bumps the revision transactionally; a
// conflict during the touch does not fail the read.
func (s *Store) Get(ctx context.Context, key string) (Entry, error) {
	if err := s.checkOpen(); err != nil {
		return Entry{}, err
	}
	b, err := s.kv.Get(ctx, s.fullKey(key))
	if errors.Is(err, storage.ErrNotFound) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, err
	}
	r, err := decode(b)
	if err != nil {
		return Entry{}, fmt.Errorf("lru: corrupt record for %q: %w", key, err)
	}
	if s.opts.TouchOnGet {
		now := time.Now().UnixNano()
		r.A = now
		r.R++
		_ = s.transaction(ctx, func(tx storage.Tx) error {
			cur, err := tx.Get(s.fullKey(key))
			if errors.Is(err, storage.ErrNotFound) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			cr, err := decode(cur)
			if err != nil {
				return err
			}
			cr.A = now
			cr.R++
			r = cr
			return tx.Put(s.fullKey(key), encode(cr))
		})
	}
	return toEntry(key, r), nil
}

// Set stores meta under key. Zero timestamps default to now; CreatedAt is
// preserved across updates of an existing key. The revision is bumped
// transactionally.
func (s *Store) Set(ctx context.Context, key string, meta EntryMeta) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	fk := s.fullKey(key)
	return s.transaction(ctx, func(tx storage.Tx) error {
		r := record{S: meta.SizeBytes, C: meta.CreatedAt.UnixNano(), A: meta.LastAccessAt.UnixNano()}
		if r.C == 0 {
			r.C = now
		}
		if r.A == 0 {
			r.A = now
		}
		cur, err := tx.Get(fk)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			r.R = 1
		case err != nil:
			return err
		default:
			cr, err := decode(cur)
			if err != nil {
				return fmt.Errorf("lru: corrupt record for %q: %w", key, err)
			}
			r.R = cr.R + 1
			if meta.CreatedAt.IsZero() {
				r.C = cr.C // preserve original creation time
			}
		}
		return tx.Put(fk, encode(r))
	})
}

// Touch refreshes LastAccessAt to now and bumps the revision
// transactionally. It returns ErrNotFound for missing keys.
func (s *Store) Touch(ctx context.Context, key string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	fk := s.fullKey(key)
	return s.transaction(ctx, func(tx storage.Tx) error {
		cur, err := tx.Get(fk)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		r, err := decode(cur)
		if err != nil {
			return fmt.Errorf("lru: corrupt record for %q: %w", key, err)
		}
		r.A = now
		r.R++
		return tx.Put(fk, encode(r))
	})
}

// Delete removes key. Deleting a missing key returns ErrNotFound.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	fk := s.fullKey(key)
	return s.transaction(ctx, func(tx storage.Tx) error {
		if _, err := tx.Get(fk); errors.Is(err, storage.ErrNotFound) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		return tx.Delete(fk)
	})
}

// scanAll returns every live entry in deterministic key order.
func (s *Store) scanAll(ctx context.Context) ([]Entry, error) {
	kvs, err := s.kv.Scan(ctx, storage.ScanOptions{Prefix: s.opts.Prefix})
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(kvs))
	for _, kv := range kvs {
		if len(kv.Key) < len(s.opts.Prefix)+3 || kv.Key[len(s.opts.Prefix)+2] != '/' {
			return nil, fmt.Errorf("lru: corrupt entry key %q", kv.Key)
		}
		r, err := decode(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("lru: corrupt record at %q: %w", kv.Key, err)
		}
		// User key is everything after "<prefix><shard>/".
		userKey := kv.Key[len(s.opts.Prefix)+3:]
		entries = append(entries, toEntry(userKey, r))
	}
	return entries, nil
}

// Len returns the number of stored entries.
func (s *Store) Len(ctx context.Context) (int, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	entries, err := s.scanAll(ctx)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

// Stats returns current utilization and the configured soft caps.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	if err := s.checkOpen(); err != nil {
		return Stats{}, err
	}
	entries, err := s.scanAll(ctx)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{Items: len(entries), CapacityBytes: s.opts.CapacityBytes, CapacityItems: s.opts.CapacityItems}
	for _, e := range entries {
		st.SizeBytes += e.Meta.SizeBytes
	}
	return st, nil
}

// victims selects entries to evict, oldest LastAccessAt first with ties
// broken by key, until both soft caps are satisfied.
func (s *Store) victims(entries []Entry) []Entry {
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].Meta.LastAccessAt, entries[j].Meta.LastAccessAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return entries[i].Key < entries[j].Key
	})
	var total int64
	for _, e := range entries {
		total += e.Meta.SizeBytes
	}
	count := len(entries)
	var out []Entry
	for _, e := range entries {
		overBytes := s.opts.CapacityBytes > 0 && total > s.opts.CapacityBytes
		overItems := s.opts.CapacityItems > 0 && count > s.opts.CapacityItems
		if !overBytes && !overItems {
			break
		}
		out = append(out, e)
		total -= e.Meta.SizeBytes
		count--
	}
	return out
}

// evictOnce scans the store and deletes victims until under soft caps.
func (s *Store) evictOnce(ctx context.Context) error {
	entries, err := s.scanAll(ctx)
	if err != nil {
		return err
	}
	victims := s.victims(entries)
	if len(victims) == 0 {
		return nil
	}
	jobs := make(chan Entry)
	errCh := make(chan error, len(victims))
	var wg sync.WaitGroup
	workers := s.opts.EvictorWorkers
	if workers > len(victims) {
		workers = len(victims)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for victim := range jobs {
				fk := s.fullKey(victim.Key)
				err := s.transaction(ctx, func(tx storage.Tx) error {
					raw, err := tx.Get(fk)
					if errors.Is(err, storage.ErrNotFound) {
						return nil
					}
					if err != nil {
						return err
					}
					current, err := decode(raw)
					if err != nil {
						return err
					}
					if current.R != victim.Revision || current.A != victim.Meta.LastAccessAt.UnixNano() {
						return nil
					}
					return tx.Delete(fk)
				})
				if err != nil {
					errCh <- err
				}
			}
		}()
	}
sendLoop:
	for _, victim := range victims {
		select {
		case jobs <- victim:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

// StartEvictor starts the background evictor goroutine. It returns
// ErrEvictorRunning if already started and ErrClosed after Close. The
// supplied context may also stop the evictor.
func (s *Store) StartEvictor(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.started {
		return ErrEvictorRunning
	}
	s.started = true
	ectx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(s.opts.EvictorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ectx.Done():
				return
			case <-ticker.C:
				_ = s.evictOnce(ectx)
			}
		}
	}()
	return nil
}

// Close stops the store's own goroutines (the evictor) and marks the
// store closed. It never closes the underlying KV.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.closed = true
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}
