package cas

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

// Store is a versioned compare-and-swap store over S3.
type Store struct {
	backend s3backend.Backend
	prefix  string
	opts    Options
}

// New creates a Store backed by b with the given object key prefix.
// Prefix may be empty or end with '/'.
func New(b s3backend.Backend, prefix string, opts ...Option) (*Store, error) {
	if b == nil {
		return nil, errors.New("cas: nil backend")
	}
	if strings.Contains(prefix, "//") {
		return nil, errors.New("cas: prefix contains back-to-back slashes")
	}
	var o Options
	for _, opt := range opts {
		opt(&o)
	}
	applyDefaults(&o)
	if o.KeyCodec == nil {
		o.KeyCodec = newPrefixKeyCodec(prefix)
	}
	o.Meter = s3collections.MeterOrNoop(o.Meter)
	o.Logger = s3collections.LoggerOrNoop(o.Logger)
	o.Tracer = s3collections.TracerOrNoop(o.Tracer)
	return &Store{
		backend: b,
		prefix:  prefix,
		opts:    o,
	}, nil
}

// objectKey returns the full S3 object key for an application key.
func (s *Store) objectKey(appKey string) (string, error) {
	return s.opts.KeyCodec.Encode(appKey)
}

// readRecord fetches and parses the envelope for appKey.
// It returns the parsed envelope, backend object metadata, and a public Record.
func (s *Store) readRecord(ctx context.Context, appKey string) (*envelope, *s3backend.Object, Record, error) {
	objKey, err := s.objectKey(appKey)
	if err != nil {
		return nil, nil, Record{}, err
	}
	obj, err := s.backend.Get(ctx, objKey)
	if err != nil {
		return nil, nil, Record{}, classifyError(opGet, err)
	}
	e, err := parseEnvelope(obj.Body, appKey)
	if err != nil {
		s.recordCorrupt(ctx)
		return nil, nil, Record{}, err
	}
	rec := recordFromEnvelope(e, obj.ETag)
	return e, obj, rec, nil
}

// Create creates a new live record only if no object exists.
func (s *Store) Create(ctx context.Context, key string, value []byte, opts ...WriteOption) (Record, error) {
	start := time.Now()
	wopts := applyWriteDefaults(opts, &s.opts)
	defer s.recordAttempts(ctx, opCreate, 1)

	if len(value) > s.opts.MaxValueBytes {
		s.observeLatency(ctx, opCreate, start, outcomeError)
		return Record{}, ErrTooLarge
	}
	objKey, err := s.objectKey(key)
	if err != nil {
		s.observeLatency(ctx, opCreate, start, outcomeError)
		return Record{}, err
	}
	now := time.Now().UTC()
	body, err := marshalEnvelope(key, 1, Live, value, now, now, time.Time{}, s.opts.WriterID)
	if err != nil {
		s.observeLatency(ctx, opCreate, start, outcomeError)
		return Record{}, err
	}

	var rec Record
	err = s.runWithRetry(ctx, opCreate, *wopts.Retry, func(ctx context.Context) error {
		etag, err := s.backend.Put(ctx, objKey, body, &s3backend.Preconditions{IfNoneMatch: true})
		if err != nil {
			return classifyError(opCreate, err)
		}
		rec = Record{
			Key:       key,
			Value:     value,
			Revision:  1,
			State:     Live,
			CreatedAt: now,
			UpdatedAt: now,
			WriterID:  s.opts.WriterID,
			ETag:      etag,
		}
		return nil
	})
	if err != nil {
		s.observeLatency(ctx, opCreate, start, outcomeFor(err))
		return Record{}, err
	}
	s.observeLatency(ctx, opCreate, start, outcomeSuccess)
	return rec, nil
}

// Get returns the live record for key. Tombstones and missing keys return ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (Record, error) {
	start := time.Now()
	defer s.recordAttempts(ctx, opGet, 1)

	objKey, err := s.objectKey(key)
	if err != nil {
		s.observeLatency(ctx, opGet, start, outcomeError)
		return Record{}, err
	}
	var (
		e   *envelope
		obj *s3backend.Object
	)
	err = s.runWithRetry(ctx, opGet, s.opts.Retry, func(ctx context.Context) error {
		var err error
		obj, err = s.backend.Get(ctx, objKey)
		if err != nil {
			return classifyError(opGet, err)
		}
		e, err = parseEnvelope(obj.Body, key)
		if err != nil {
			s.recordCorrupt(ctx)
			return err
		}
		return nil
	})
	if err != nil {
		s.observeLatency(ctx, opGet, start, outcomeFor(err))
		return Record{}, err
	}
	rec := recordFromEnvelope(e, obj.ETag)
	if rec.State == Tombstone {
		err := &NotFoundError{
			Key:        key,
			Tombstoned: true,
			Revision:   rec.Revision,
			DeletedAt:  rec.DeletedAt,
		}
		s.observeLatency(ctx, opGet, start, outcomeError)
		return Record{}, err
	}
	s.observeLatency(ctx, opGet, start, outcomeSuccess)
	return rec, nil
}

// GetMeta returns metadata even for tombstones.
func (s *Store) GetMeta(ctx context.Context, key string) (Record, error) {
	start := time.Now()
	defer s.recordAttempts(ctx, opGet, 1)

	objKey, err := s.objectKey(key)
	if err != nil {
		s.observeLatency(ctx, opGet, start, outcomeError)
		return Record{}, err
	}
	var (
		e   *envelope
		obj *s3backend.Object
	)
	err = s.runWithRetry(ctx, opGet, s.opts.Retry, func(ctx context.Context) error {
		var err error
		obj, err = s.backend.Get(ctx, objKey)
		if err != nil {
			return classifyError(opGet, err)
		}
		e, err = parseEnvelope(obj.Body, key)
		if err != nil {
			s.recordCorrupt(ctx)
			return err
		}
		return nil
	})
	if err != nil {
		s.observeLatency(ctx, opGet, start, outcomeFor(err))
		return Record{}, err
	}
	rec := recordFromEnvelope(e, obj.ETag)
	s.observeLatency(ctx, opGet, start, outcomeSuccess)
	return rec, nil
}

// CompareAndSwap replaces the value iff the current live revision equals expect.
func (s *Store) CompareAndSwap(ctx context.Context, key string, expect uint64, newValue []byte, opts ...WriteOption) (Record, error) {
	start := time.Now()
	wopts := applyWriteDefaults(opts, &s.opts)

	if len(newValue) > s.opts.MaxValueBytes {
		s.observeLatency(ctx, opCAS, start, outcomeError)
		return Record{}, ErrTooLarge
	}

	var rec Record
	attempts := 0
	err := s.runWithRetry(ctx, opCAS, *wopts.Retry, func(ctx context.Context) error {
		attempts++
		e, obj, cur, err := s.readRecord(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return err
			}
			return err
		}
		if cur.State == Tombstone {
			return ErrDeleted
		}
		if cur.Revision != expect {
			return ErrConflict
		}
		now := time.Now().UTC()
		body, err := marshalEnvelope(key, e.Rev+1, Live, newValue, e.CreatedAt, now, time.Time{}, s.opts.WriterID)
		if err != nil {
			return err
		}
		etag, err := s.backend.Put(ctx, obj.Key, body, &s3backend.Preconditions{IfMatchETag: obj.ETag})
		if err != nil {
			return classifyError(opCAS, err)
		}
		rec = Record{
			Key:       key,
			Value:     newValue,
			Revision:  e.Rev + 1,
			State:     Live,
			CreatedAt: e.CreatedAt,
			UpdatedAt: now,
			WriterID:  s.opts.WriterID,
			ETag:      etag,
		}
		return nil
	})
	s.recordAttempts(ctx, opCAS, attempts)
	if err != nil {
		s.observeLatency(ctx, opCAS, start, outcomeFor(err))
		return Record{}, err
	}
	s.observeLatency(ctx, opCAS, start, outcomeSuccess)
	return rec, nil
}

// Update performs a read-modify-write loop.
func (s *Store) Update(ctx context.Context, key string, fn UpdateFn, opts ...WriteOption) (Record, error) {
	start := time.Now()
	wopts := applyWriteDefaults(opts, &s.opts)

	var rec Record
	attempts := 0
	err := s.runWithRetry(ctx, opUpdate, *wopts.Retry, func(ctx context.Context) error {
		attempts++
		e, obj, cur, err := s.readRecord(ctx, key)
		if err != nil {
			return err
		}

		var next []byte
		if cur.State == Tombstone {
			if !wopts.IncludeTombstone {
				return ErrDeleted
			}
			next, err = fn(ctx, cur)
			if err != nil {
				return err
			}
			if next == nil {
				rec = cur
				return nil
			}
			if !wopts.Resurrect {
				return ErrDeleted
			}
			now := time.Now().UTC()
			body, merr := marshalEnvelope(key, e.Rev+1, Live, next, e.CreatedAt, now, time.Time{}, s.opts.WriterID)
			if merr != nil {
				return merr
			}
			etag, perr := s.backend.Put(ctx, obj.Key, body, &s3backend.Preconditions{IfMatchETag: obj.ETag})
			if perr != nil {
				return classifyError(opUpdate, perr)
			}
			rec = Record{
				Key:       key,
				Value:     next,
				Revision:  e.Rev + 1,
				State:     Live,
				CreatedAt: e.CreatedAt,
				UpdatedAt: now,
				WriterID:  s.opts.WriterID,
				ETag:      etag,
			}
			return nil
		}

		// Live state.
		next, err = fn(ctx, cur)
		if err != nil {
			return err
		}
		if next == nil || bytes.Equal(next, cur.Value) {
			rec = cur
			return nil
		}
		if len(next) > s.opts.MaxValueBytes {
			return ErrTooLarge
		}
		now := time.Now().UTC()
		body, merr := marshalEnvelope(key, e.Rev+1, Live, next, e.CreatedAt, now, time.Time{}, s.opts.WriterID)
		if merr != nil {
			return merr
		}
		etag, perr := s.backend.Put(ctx, obj.Key, body, &s3backend.Preconditions{IfMatchETag: obj.ETag})
		if perr != nil {
			return classifyError(opUpdate, perr)
		}
		rec = Record{
			Key:       key,
			Value:     next,
			Revision:  e.Rev + 1,
			State:     Live,
			CreatedAt: e.CreatedAt,
			UpdatedAt: now,
			WriterID:  s.opts.WriterID,
			ETag:      etag,
		}
		return nil
	})
	s.recordAttempts(ctx, opUpdate, attempts)
	if err != nil {
		s.observeLatency(ctx, opUpdate, start, outcomeFor(err))
		return Record{}, err
	}
	s.observeLatency(ctx, opUpdate, start, outcomeSuccess)
	return rec, nil
}

// Delete writes a tombstone envelope.
func (s *Store) Delete(ctx context.Context, key string, expect uint64, opts ...WriteOption) (Record, error) {
	start := time.Now()
	wopts := applyWriteDefaults(opts, &s.opts)

	var rec Record
	attempts := 0
	err := s.runWithRetry(ctx, opDelete, *wopts.Retry, func(ctx context.Context) error {
		attempts++
		e, obj, cur, err := s.readRecord(ctx, key)
		if err != nil {
			return err
		}
		if cur.State == Tombstone {
			if expect == cur.Revision || expect == cur.Revision-1 {
				rec = cur
				return nil
			}
			return ErrConflict
		}
		if cur.Revision != expect {
			return ErrConflict
		}
		now := time.Now().UTC()
		body, merr := marshalEnvelope(key, e.Rev+1, Tombstone, nil, e.CreatedAt, now, now, s.opts.WriterID)
		if merr != nil {
			return merr
		}
		etag, perr := s.backend.Put(ctx, obj.Key, body, &s3backend.Preconditions{IfMatchETag: obj.ETag})
		if perr != nil {
			return classifyError(opDelete, perr)
		}
		rec = Record{
			Key:       key,
			Revision:  e.Rev + 1,
			State:     Tombstone,
			CreatedAt: e.CreatedAt,
			UpdatedAt: now,
			DeletedAt: now,
			WriterID:  s.opts.WriterID,
			ETag:      etag,
		}
		return nil
	})
	s.recordAttempts(ctx, opDelete, attempts)
	if err != nil {
		s.observeLatency(ctx, opDelete, start, outcomeFor(err))
		return Record{}, err
	}
	s.observeLatency(ctx, opDelete, start, outcomeSuccess)
	return rec, nil
}

// runWithRetry executes op with the given retry policy, classifying transient and conflict errors.
func (s *Store) runWithRetry(ctx context.Context, op opName, policy s3collections.RetryPolicy, opFn func(context.Context) error) error {
	policy = policy.WithDefaults()
	nextDelay := s3collections.BackoffDelays(policy, nil)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		err := opFn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Classified conflict errors are not retried automatically by this loop
		// unless they are backend precondition failures. We retry on ErrConflict
		// only for update-style operations; for create/delete/cas the caller loop is
		// not used, but the policy still applies to backend retries.
		switch {
		case errors.Is(err, ErrConflict):
			s.recordConflict(ctx, op)
			if op != opUpdate {
				return err
			}
			// Update retries on conflict after re-reading and re-invoking fn.
		case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrNotFound), errors.Is(err, ErrDeleted), errors.Is(err, ErrTooLarge):
			return err
		case s3backend.IsRetryable(err):
			s.recordRetry(ctx, op, "backend")
		default:
			return err
		}
		if attempt == policy.MaxAttempts {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextDelay()):
		}
	}
	return nil
}

// outcomeFor maps an error to a metric outcome label.
func outcomeFor(err error) string {
	if err == nil {
		return outcomeSuccess
	}
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrAlreadyExists) {
		return outcomeConflict
	}
	return outcomeError
}
