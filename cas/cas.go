// Package cas implements a versioned, compare-and-swap record store on top
// of the storage.KV abstraction. It provides optimistic concurrency via
// per-record revisions, soft deletes with tombstones, and a garbage
// collector that reclaims expired tombstones.
package cas

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/damianb/s3collections/storage"
)

// State describes the lifecycle state of a Record.
type State string

const (
	// StateLive marks a record that holds a current value.
	StateLive State = "live"
	// StateTombstone marks a deleted record whose metadata is retained
	// until garbage collection reclaims it.
	StateTombstone State = "tombstone"
)

// Sentinel errors returned by the CAS store.
var (
	// ErrNotFound is returned when a key has no record, or only an
	// expired tombstone, in the store.
	ErrNotFound = errors.New("cas: not found")
	// ErrExists is returned by Create when a live record already exists
	// for the key.
	ErrExists = errors.New("cas: already exists")
	// ErrDeleted is returned when an operation requires a live record
	// but the record is a tombstone.
	ErrDeleted = errors.New("cas: record is deleted")
	// ErrRevisionMismatch is returned when an expected revision does not
	// match the stored revision.
	ErrRevisionMismatch = errors.New("cas: revision mismatch")
	// ErrConflict is an alias used for stale revisions and exhausted storage conflicts.
	ErrConflict = ErrRevisionMismatch
	// ErrValueTooLarge is returned when a value exceeds the configured
	// maximum value size.
	ErrValueTooLarge = errors.New("cas: value too large")
	// ErrEmptyKey is returned when a key is empty.
	ErrEmptyKey = errors.New("cas: empty key")
)

// Record is a versioned key/value entry.
type Record struct {
	Key       string    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
	Revision  uint64    `json:"revision"`
	State     State     `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// DeletedAt is set when the record becomes a tombstone.
	DeletedAt time.Time `json:"deleted_at,omitempty"`
}

// Option configures a Store.
type Option func(*Store)

// WithMaxValueBytes caps the size of values accepted by mutating
// operations. n <= 0 disables the limit; the default is 512 KiB.
func WithMaxValueBytes(n int64) Option {
	return func(s *Store) { s.maxValueBytes = n }
}

// WithTombstoneRetention sets how long tombstones are kept before GC may
// remove them. d <= 0 means tombstones are kept forever (the default).
func WithTombstoneRetention(d time.Duration) Option {
	return func(s *Store) { s.retention = d }
}

// WithAllowResurrection allows Create to revive a tombstoned key.
func WithAllowResurrection(allow bool) Option {
	return func(s *Store) { s.allowResurrection = allow }
}

// Store is a versioned record store backed by a storage.KV.
type Store struct {
	kv                storage.KV
	prefix            string
	maxValueBytes     int64
	retention         time.Duration
	allowResurrection bool
	now               func() time.Time
}

// New creates a Store on kv. All record keys are namespaced under prefix.
func New(kv storage.KV, prefix string, opts ...Option) *Store {
	s := &Store{
		kv:            kv,
		prefix:        prefix,
		maxValueBytes: 512 << 10,
		now:           func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// encodeKey maps an arbitrary user key to a storage-safe key using
// URL-safe base64 without padding.
func (s *Store) storagePrefix() string {
	return "cas/" + base64.RawURLEncoding.EncodeToString([]byte(s.prefix)) + "/"
}

func (s *Store) encodeKey(key string) string {
	return s.storagePrefix() + base64.RawURLEncoding.EncodeToString([]byte(key))
}

// decodeKey maps a storage key back to the user key.
func (s *Store) decodeKey(storageKey string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(storageKey[len(s.storagePrefix()):])
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (s *Store) checkValue(value []byte) error {
	if s.maxValueBytes > 0 && int64(len(value)) > s.maxValueBytes {
		return fmt.Errorf("%w: %d > %d", ErrValueTooLarge, len(value), s.maxValueBytes)
	}
	return nil
}

func getRecord(tx storage.Tx, storageKey string) (Record, bool, error) {
	raw, err := tx.Get(storageKey)
	if errors.Is(err, storage.ErrNotFound) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	var rec Record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return Record{}, false, fmt.Errorf("cas: decode record: %w", err)
	}
	return rec, true, nil
}

func putRecord(tx storage.Tx, storageKey string, rec Record) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("cas: encode record: %w", err)
	}
	return tx.Put(storageKey, raw)
}

// transact runs fn in a transaction, retrying on serialization conflicts.
func (s *Store) transact(ctx context.Context, fn func(storage.Tx) error) error {
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
	return ErrConflict
}

// Create inserts a new live record with revision 1. It fails with
// ErrExists if a live record exists for key, and with ErrDeleted if the
// record is a tombstone and resurrection is not allowed.
func (s *Store) Create(ctx context.Context, key string, value []byte) (Record, error) {
	if key == "" {
		return Record{}, ErrEmptyKey
	}
	if err := s.checkValue(value); err != nil {
		return Record{}, err
	}
	skey := s.encodeKey(key)
	var out Record
	err := s.transact(ctx, func(tx storage.Tx) error {
		rec, found, err := getRecord(tx, skey)
		if err != nil {
			return err
		}
		now := s.now()
		if found {
			if rec.State == StateLive {
				return ErrExists
			}
			if !s.allowResurrection {
				return ErrDeleted
			}
			// Resurrect: keep history, bump revision.
			out = Record{
				Key:       key,
				Value:     clone(value),
				Revision:  rec.Revision + 1,
				State:     StateLive,
				CreatedAt: rec.CreatedAt,
				UpdatedAt: now,
			}
		} else {
			out = Record{
				Key:       key,
				Value:     clone(value),
				Revision:  1,
				State:     StateLive,
				CreatedAt: now,
				UpdatedAt: now,
			}
		}
		return putRecord(tx, skey, out)
	})
	if err != nil {
		return Record{}, err
	}
	return out, nil
}

// Get returns the live record for key, including its value.
func (s *Store) Get(ctx context.Context, key string) (Record, error) {
	return s.get(ctx, key, true)
}

// GetMeta returns record metadata for live records and tombstones. The
// returned Record has Value cleared.
func (s *Store) GetMeta(ctx context.Context, key string) (Record, error) {
	return s.get(ctx, key, false)
}

func (s *Store) get(ctx context.Context, key string, withValue bool) (Record, error) {
	if key == "" {
		return Record{}, ErrEmptyKey
	}
	raw, err := s.kv.Get(ctx, s.encodeKey(key))
	if errors.Is(err, storage.ErrNotFound) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	var rec Record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return Record{}, fmt.Errorf("cas: decode record: %w", err)
	}
	if rec.State == StateTombstone && withValue {
		return Record{}, ErrNotFound
	}
	if !withValue {
		rec.Value = nil
	}
	return rec, nil
}

// CompareAndSwap replaces the value of the live record at key only if its
// current revision equals expect. It returns the updated record.
func (s *Store) CompareAndSwap(ctx context.Context, key string, expect uint64, value []byte) (Record, error) {
	if key == "" {
		return Record{}, ErrEmptyKey
	}
	if err := s.checkValue(value); err != nil {
		return Record{}, err
	}
	skey := s.encodeKey(key)
	var out Record
	err := s.transact(ctx, func(tx storage.Tx) error {
		rec, found, err := getRecord(tx, skey)
		if err != nil {
			return err
		}
		if !found || rec.State == StateTombstone {
			return ErrNotFound
		}
		if rec.Revision != expect {
			return ErrRevisionMismatch
		}
		if equal(rec.Value, value) {
			// No-op update: do not bump the revision.
			out = rec
			return nil
		}
		out = rec
		out.Value = clone(value)
		out.Revision = rec.Revision + 1
		out.UpdatedAt = s.now()
		return putRecord(tx, skey, out)
	})
	if err != nil {
		return Record{}, err
	}
	return out, nil
}

// Update applies fn to the current live record at key and stores the
// returned value. fn may run more than once when a transaction conflicts and
// must not perform external side effects. If fn returns a value identical to the current one, the
// revision is not bumped.
func (s *Store) Update(ctx context.Context, key string, fn func(ctx context.Context, rec Record) ([]byte, error)) (Record, error) {
	if key == "" {
		return Record{}, ErrEmptyKey
	}
	if fn == nil {
		return Record{}, errors.New("cas: nil update function")
	}
	skey := s.encodeKey(key)
	var out Record
	err := s.transact(ctx, func(tx storage.Tx) error {
		rec, found, err := getRecord(tx, skey)
		if err != nil {
			return err
		}
		if !found || rec.State == StateTombstone {
			return ErrNotFound
		}
		newValue, err := fn(ctx, rec)
		if err != nil {
			return err
		}
		if err := s.checkValue(newValue); err != nil {
			return err
		}
		if equal(rec.Value, newValue) {
			// No-op update: do not bump the revision.
			out = rec
			return nil
		}
		out = rec
		out.Value = clone(newValue)
		out.Revision = rec.Revision + 1
		out.UpdatedAt = s.now()
		return putRecord(tx, skey, out)
	})
	if err != nil {
		return Record{}, err
	}
	return out, nil
}

// Delete converts the live record at key into a tombstone, but only if
// its current revision equals expect. A zero expect deletes any revision.
func (s *Store) Delete(ctx context.Context, key string, expect uint64) (Record, error) {
	if key == "" {
		return Record{}, ErrEmptyKey
	}
	skey := s.encodeKey(key)
	var out Record
	err := s.transact(ctx, func(tx storage.Tx) error {
		rec, found, err := getRecord(tx, skey)
		if err != nil {
			return err
		}
		if !found || rec.State == StateTombstone {
			return ErrNotFound
		}
		if expect != 0 && rec.Revision != expect {
			return ErrRevisionMismatch
		}
		now := s.now()
		out = rec
		out.Value = nil
		out.State = StateTombstone
		out.Revision = rec.Revision + 1
		out.UpdatedAt = now
		out.DeletedAt = now
		return putRecord(tx, skey, out)
	})
	if err != nil {
		return Record{}, err
	}
	return out, nil
}

// ListOptions controls a List call.
type ListOptions struct {
	// Prefix restricts results to records whose key has this prefix.
	Prefix string
	// StartAfter makes the listing begin strictly after this user key.
	StartAfter string
	// Limit caps the number of returned records; <= 0 means no limit.
	Limit int
	// IncludeTombstones includes tombstoned records in the result.
	IncludeTombstones bool
}

// Page is a page of records from List.
type Page struct {
	Records []Record
	// NextStartAfter is the user key to pass as ListOptions.StartAfter to
	// fetch the next page. It is empty when the listing is exhausted.
	NextStartAfter string
}

// List returns records in ascending byte-lexicographic user-key order.
func (s *Store) List(ctx context.Context, opts ListOptions) (Page, error) {
	entries, err := s.kv.Scan(ctx, storage.ScanOptions{Prefix: s.storagePrefix()})
	if err != nil {
		return Page{}, err
	}
	records := make([]Record, 0, len(entries))
	for _, entry := range entries {
		userKey, err := s.decodeKey(entry.Key)
		if err != nil {
			return Page{}, fmt.Errorf("cas: decode key: %w", err)
		}
		if opts.Prefix != "" && !hasPrefix(userKey, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" && userKey <= opts.StartAfter {
			continue
		}
		var record Record
		if err := json.Unmarshal(entry.Value, &record); err != nil {
			return Page{}, fmt.Errorf("cas: decode record: %w", err)
		}
		if record.State == StateTombstone && !opts.IncludeTombstones {
			continue
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	page := Page{}
	if opts.Limit > 0 && len(records) > opts.Limit {
		page.Records = records[:opts.Limit]
		page.NextStartAfter = page.Records[len(page.Records)-1].Key
		return page, nil
	}
	page.Records = records
	return page, nil
}

func lastKey(recs []Record) string {
	if len(recs) == 0 {
		return ""
	}
	return recs[len(recs)-1].Key
}

// GC removes tombstones whose DeletedAt is older than the configured
// retention. It returns the number of tombstones removed. If no positive
// retention is configured, GC removes nothing and returns 0.
func (s *Store) GC(ctx context.Context) (int, error) {
	if s.retention <= 0 {
		return 0, nil
	}
	entries, err := s.kv.Scan(ctx, storage.ScanOptions{Prefix: s.storagePrefix()})
	if err != nil {
		return 0, err
	}
	cutoff := s.now().Add(-s.retention)
	var expired []string
	for _, e := range entries {
		var rec Record
		if err := json.Unmarshal(e.Value, &rec); err != nil {
			return 0, fmt.Errorf("cas: decode record: %w", err)
		}
		if rec.State == StateTombstone && !rec.DeletedAt.IsZero() && rec.DeletedAt.Before(cutoff) {
			expired = append(expired, e.Key)
		}
	}
	removed := 0
	for _, skey := range expired {
		didRemove := false
		err := s.transact(ctx, func(tx storage.Tx) error {
			didRemove = false
			rec, found, err := getRecord(tx, skey)
			if err != nil {
				return err
			}
			if !found {
				return nil // already gone
			}
			// Re-check expiry inside the transaction: the record may have
			// been resurrected since the scan.
			if rec.State != StateTombstone || rec.DeletedAt.IsZero() || !rec.DeletedAt.Before(cutoff) {
				return nil
			}
			if err := tx.Delete(skey); err != nil {
				return err
			}
			didRemove = true
			return nil
		})
		if err != nil {
			return removed, err
		}
		if didRemove {
			removed++
		}
	}
	return removed, nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
