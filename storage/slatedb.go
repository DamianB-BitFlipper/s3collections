//go:build slatedb

package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// SlateDBConfig configures a SlateDB-backed KV store.
type SlateDBConfig struct {
	// Path is the database path within the object store (for file://
	// stores, a directory relative to the store root).
	Path string
	// ObjectStoreURL is an object store URL understood by SlateDB, such as
	// "memory:///", "file:///", or "s3://bucket". When empty,
	// ObjectStoreFromEnv is used instead.
	ObjectStoreURL string
	// EnvFile optionally points to a .env file with object store
	// credentials; only used when ObjectStoreURL is empty.
	EnvFile string
}

// slateDBKV adapts a SlateDB database to the KV interface.
type slateDBKV struct {
	mu     sync.RWMutex
	db     *slatedb.Db
	store  *slatedb.ObjectStore
	closed bool
}

// NewSlateDBKV opens (creating if needed) a SlateDB database and returns it
// as a KV store. Requires the slatedb build tag and cgo.
func NewSlateDBKV(cfg SlateDBConfig) (KV, error) {
	var (
		store *slatedb.ObjectStore
		err   error
	)
	if cfg.ObjectStoreURL != "" {
		store, err = slatedb.ObjectStoreResolve(cfg.ObjectStoreURL)
	} else {
		var envFile *string
		if cfg.EnvFile != "" {
			envFile = &cfg.EnvFile
		}
		store, err = slatedb.ObjectStoreFromEnv(envFile)
	}
	if err != nil {
		return nil, err
	}

	builder := slatedb.NewDbBuilder(cfg.Path, store)
	db, err := builder.Build()
	builder.Destroy()
	if err != nil {
		store.Destroy()
		return nil, err
	}
	return &slateDBKV{db: db, store: store}, nil
}

// OpenSlateDB is the canonical SlateDB KV constructor.
func OpenSlateDB(cfg SlateDBConfig) (KV, error) { return NewSlateDBKV(cfg) }

// mapErr translates SlateDB errors into storage sentinel errors.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var closedErr *slatedb.ErrorClosed
	if errors.As(err, &closedErr) {
		if closedErr.Reason == slatedb.CloseReasonFenced {
			return errors.Join(ErrFenced, err)
		}
		return errors.Join(ErrClosed, err)
	}
	var txnErr *slatedb.ErrorTransaction
	if errors.As(err, &txnErr) {
		return errors.Join(ErrConflict, err)
	}
	return err
}

func (s *slateDBKV) beginOperation(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	return s.mu.RUnlock, nil
}

func (s *slateDBKV) Get(ctx context.Context, key string) ([]byte, error) {
	done, err := s.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	v, err := s.db.Get([]byte(key))
	if err != nil {
		return nil, mapErr(err)
	}
	if v == nil {
		return nil, ErrNotFound
	}
	return bytes.Clone(*v), nil
}

func (s *slateDBKV) Put(ctx context.Context, key string, value []byte) error {
	done, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	h, err := s.db.Put([]byte(key), value)
	if err != nil {
		return mapErr(err)
	}
	h.Destroy()
	return nil
}

func (s *slateDBKV) Delete(ctx context.Context, key string) error {
	done, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	h, err := s.db.Delete([]byte(key))
	if err != nil {
		return mapErr(err)
	}
	h.Destroy()
	return nil
}

// keyRange builds a SlateDB range for a prefix and an exclusive continuation
// bound. StartAfter follows iteration order: ascending resumes above the key,
// descending resumes below it.
func keyRange(prefix, startAfter string, reverse bool) slatedb.KeyRange {
	kr := slatedb.KeyRange{}
	if prefix != "" {
		start := []byte(prefix)
		kr.Start = &start
		kr.StartInclusive = true
		if upper := prefixSuccessor(prefix); upper != nil {
			kr.End = upper
			kr.EndInclusive = false
		}
	}
	if startAfter != "" {
		bound := []byte(startAfter)
		if reverse {
			kr.End = &bound
			kr.EndInclusive = false
		} else {
			kr.Start = &bound
			kr.StartInclusive = false
		}
	}
	return kr
}

// prefixSuccessor returns the smallest byte string strictly greater than
// every string with the given prefix, or nil when no such string exists
// (prefix is empty or all 0xFF).
func prefixSuccessor(prefix string) *[]byte {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xFF {
			out := make([]byte, i+1)
			copy(out, b[:i+1])
			out[i]++
			return &out
		}
	}
	return nil
}

func (s *slateDBKV) Scan(ctx context.Context, opts ScanOptions) ([]Entry, error) {
	done, err := s.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.scanWith(func(kr slatedb.KeyRange) (*slatedb.DbIterator, error) {
		return s.db.ScanWithOptions(kr, scanOpts(opts.Reverse))
	}, opts)
}

func scanOpts(reverse bool) slatedb.ScanOptions {
	so := slatedb.ScanOptions{DurabilityFilter: slatedb.DurabilityLevelMemory}
	if reverse {
		ord := slatedb.IterationOrderDescending
		so.Order = &ord
	}
	return so
}

func (s *slateDBKV) scanWith(open func(slatedb.KeyRange) (*slatedb.DbIterator, error), opts ScanOptions) ([]Entry, error) {
	kr := keyRange(opts.Prefix, opts.StartAfter, opts.Reverse)
	iter, err := open(kr)
	if err != nil {
		return nil, mapErr(err)
	}
	defer iter.Destroy()

	out := []Entry{}
	for {
		kv, err := iter.Next()
		if err != nil {
			return nil, mapErr(err)
		}
		if kv == nil {
			break
		}
		key := string(kv.Key)
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" {
			if (!opts.Reverse && key <= opts.StartAfter) || (opts.Reverse && key >= opts.StartAfter) {
				continue
			}
		}
		out = append(out, Entry{Key: key, Value: bytes.Clone(kv.Value)})
		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}
	}
	return out, nil
}

// Transaction runs fn inside a SlateDB serializable-snapshot transaction.
// When fn returns an error the transaction is rolled back.
func (s *slateDBKV) Transaction(ctx context.Context, fn func(Tx) error) error {
	done, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	txn, err := s.db.Begin(slatedb.IsolationLevelSerializableSnapshot)
	if err != nil {
		return mapErr(err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = txn.Rollback()
		}
		txn.Destroy()
	}()
	stx := &slateDBTx{txn: txn}
	if err := fn(stx); err != nil {
		_ = txn.Rollback()
		finished = true
		return err
	}
	h, err := txn.Commit()
	finished = true
	if err != nil {
		return mapErr(err)
	}
	if h != nil {
		h.Destroy()
	}
	return nil
}

// slateDBTx adapts a SlateDB transaction to the Tx interface.
type slateDBTx struct {
	txn *slatedb.DbTransaction
}

func (t *slateDBTx) Get(key string) ([]byte, error) {
	v, err := t.txn.Get([]byte(key))
	if err != nil {
		return nil, mapErr(err)
	}
	if v == nil {
		return nil, ErrNotFound
	}
	return bytes.Clone(*v), nil
}

func (t *slateDBTx) Put(key string, value []byte) error {
	return mapErr(t.txn.Put([]byte(key), value))
}

func (t *slateDBTx) Delete(key string) error {
	return mapErr(t.txn.Delete([]byte(key)))
}

func (t *slateDBTx) Scan(opts ScanOptions) ([]Entry, error) {
	kv := &slateDBKV{}
	return kv.scanWith(func(kr slatedb.KeyRange) (*slatedb.DbIterator, error) {
		if opts.Reverse {
			return t.txn.ScanWithOptions(kr, scanOpts(true))
		}
		return t.txn.Scan(kr)
	}, opts)
}

func (s *slateDBKV) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.closed = true
	err := s.db.Shutdown()
	s.db.Destroy()
	s.store.Destroy()
	return mapErr(err)
}
