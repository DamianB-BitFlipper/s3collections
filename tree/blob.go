package tree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/damianb/s3collections/s3backend"
)

type blobManifest struct {
	V          int        `json:"v"`
	Hash       BlobID     `json:"hash"`
	Size       int64      `json:"size"`
	Encoding   Descriptor `json:"encoding,omitempty"`
	Encryption Descriptor `json:"encryption,omitempty"`
	Metadata   []byte     `json:"metadata,omitempty"`
}

func (m blobManifest) ref() BlobRef {
	return normalizeRef(BlobRef{Hash: m.Hash, Size: m.Size, Encoding: m.Encoding, Encryption: m.Encryption, Metadata: m.Metadata})
}

// PutBlob hashes the exact bytes read from r before publishing them. Raw data
// is uploaded first; the small immutable manifest is the visibility point.
// A failed or abandoned data upload is harmless and eligible for GC.
func (s *Store) PutBlob(ctx context.Context, expectedHash BlobID, r io.Reader, options ...BlobPutOption) (BlobRef, error) {
	if err := validateBlobID(expectedHash); err != nil {
		return BlobRef{}, err
	}
	if r == nil {
		return BlobRef{}, errors.New("tree: nil blob reader")
	}
	var o BlobPutOptions
	for _, opt := range options {
		if opt != nil {
			opt(&o)
		}
	}
	o.Encoding = Descriptor(cloneBytes(o.Encoding))
	o.Encryption = Descriptor(cloneBytes(o.Encryption))
	o.Metadata = cloneBytes(o.Metadata)
	if o.ExpectedSize != nil && (*o.ExpectedSize < s.opts.MinBlobBytes || *o.ExpectedSize > s.opts.MaxBlobBytes) {
		if *o.ExpectedSize < s.opts.MinBlobBytes {
			return BlobRef{}, ErrBlobTooSmall
		}
		return BlobRef{}, ErrBlobTooLarge
	}
	f, err := os.CreateTemp("", "s3collections-tree-blob-*")
	if err != nil {
		return BlobRef{}, err
	}
	name := f.Name()
	defer func() { _ = f.Close(); _ = os.Remove(name) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(&contextReader{ctx: ctx, r: r}, s.opts.MaxBlobBytes+1))
	if err != nil {
		return BlobRef{}, err
	}
	if n > s.opts.MaxBlobBytes {
		return BlobRef{}, ErrBlobTooLarge
	}
	if n < s.opts.MinBlobBytes {
		return BlobRef{}, ErrBlobTooSmall
	}
	if o.ExpectedSize != nil && n != *o.ExpectedSize {
		return BlobRef{}, fmt.Errorf("tree: blob size: got %d want %d", n, *o.ExpectedSize)
	}
	actual := hex.EncodeToString(h.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(actual), []byte(expectedHash)) != 1 {
		return BlobRef{}, ErrHashMismatch
	}
	m := blobManifest{V: 1, Hash: expectedHash, Size: n, Encoding: o.Encoding, Encryption: o.Encryption, Metadata: o.Metadata}
	manifest, err := json.Marshal(m)
	if err != nil {
		return BlobRef{}, err
	}
	// Upload raw immutable data OUTSIDE the mutation gate so that large
	// (multi-hundred-MiB) uploads do not serialize against ref/lease/GC
	// mutations or exceed the gate TTL. A failed or abandoned data upload
	// is harmless: it is an unreferenced object eligible for GC.
	//
	// Check whether the manifest already exists first to avoid a needless
	// re-upload for an already-published blob.
	manifestExists := false
	if gerr := s.runBackend(ctx, "get_blob_manifest", func() error {
		_, e := s.backend.Get(ctx, s.blobMetaKey(expectedHash))
		return e
	}); gerr == nil {
		manifestExists = true
	} else if !errors.Is(gerr, s3backend.ErrNotFound) {
		return BlobRef{}, gerr
	}

	if manifestExists {
		stored, e := s.statBlobID(ctx, expectedHash)
		if e != nil {
			return BlobRef{}, e
		}
		if e = s.verifyBlobData(ctx, expectedHash, stored.Size); e != nil {
			return BlobRef{}, e
		}
		// Re-publish the recent blob pin under the gate so a concurrent GC
		// sweep cannot collect the blob between verification and pin.
		if e = s.withMutationGate(ctx, "put_blob_pin", func(g *mutationGate) error {
			meta, se := s.backend.Get(ctx, s.blobMetaKey(expectedHash))
			if se != nil {
				return se
			}
			var current blobManifest
			if decodeCanonical(meta.Body, &current) != nil || current.V != 1 || current.Hash != expectedHash || current.Size != stored.Size {
				return ErrCorrupt
			}
			stored = current.ref()
			st, se := s.statObject(ctx, s.blobDataKey(expectedHash))
			if se != nil {
				return se
			}
			if st.Size != stored.Size {
				return ErrCorrupt
			}
			if e := s.renewMutationGate(ctx, g); e != nil {
				return e
			}
			return s.publishRecentBlobPin(ctx, expectedHash)
		}); e != nil {
			return BlobRef{}, e
		}
		return stored, nil
	}

	// Upload raw data outside the gate.
	if e := s.uploadRawBlob(ctx, expectedHash, f, n); e != nil {
		return BlobRef{}, e
	}

	// Acquire the gate only for the short publication phase: verify the
	// raw object survived, publish the immutable manifest, and pin.
	var out BlobRef
	err = s.withMutationGate(ctx, "put_blob", func(g *mutationGate) error {
		// A concurrent GC sweep may have removed the raw data object while
		// we were acquiring the gate. If so, re-upload under the gate.
		if e := s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		st, statErr := s.statObject(ctx, s.blobDataKey(expectedHash))
		if statErr != nil {
			return statErr
		}
		if st.Size != n {
			return ErrCorrupt
		}
		if e := s.putImmutable(ctx, "put_blob_manifest", s.blobMetaKey(expectedHash), manifest); e != nil {
			// Manifest may already exist from a concurrent publisher.
			stored, se := s.statBlobID(ctx, expectedHash)
			if se != nil || s.verifyBlobData(ctx, expectedHash, stored.Size) != nil {
				return e
			}
			out = stored
		} else {
			out = m.ref()
		}
		if e := s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		return s.publishRecentBlobPin(ctx, expectedHash)
	})
	if err != nil {
		return BlobRef{}, err
	}
	return out, nil
}

func (s *Store) uploadRawBlob(ctx context.Context, id BlobID, f *os.File, size int64) error {
	key := s.blobDataKey(id)
	upload := func() error {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if size >= s.opts.MultipartThreshold {
			if b, ok := s.backend.(s3backend.MultipartBackend); ok {
				// Raw bytes are already hash-verified and multipart completion is
				// atomic. The immutable conditional manifest remains the visibility
				// point; standard S3 multipart has no portable completion precondition.
				return b.PutMultipart(ctx, key, f, size, nil)
			}
		}
		if b, ok := s.backend.(s3backend.StreamBackend); ok {
			return b.PutStream(ctx, key, f, size, &s3backend.Preconditions{IfNoneMatch: true})
		}
		if size > s.opts.MaxBufferedPut {
			return fmt.Errorf("%w: streaming PUT", ErrBackendCapability)
		}
		body, err := io.ReadAll(io.LimitReader(f, size+1))
		if err != nil {
			return err
		}
		if int64(len(body)) != size {
			return io.ErrUnexpectedEOF
		}
		_, err = s.backend.Put(ctx, key, body, &s3backend.Preconditions{IfNoneMatch: true})
		return err
	}
	err := s.runBackend(ctx, "put_blob_data", upload)
	if err == nil {
		return nil
	}
	if verr := s.verifyBlobData(ctx, id, size); verr == nil {
		return nil
	} else if errors.Is(err, s3backend.ErrPreconditionFailed) {
		return verr
	}
	return err
}
func (s *Store) putImmutable(ctx context.Context, op, key string, body []byte) error {
	err := s.runBackend(ctx, op, func() error {
		_, e := s.backend.Put(ctx, key, body, &s3backend.Preconditions{IfNoneMatch: true})
		return e
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, s3backend.ErrPreconditionFailed) || s3backend.IsRetryable(err) {
		obj, e := s.backend.Get(ctx, key)
		if e == nil && bytes.Equal(obj.Body, body) {
			return nil
		}
	}
	return err
}

// StatBlob validates the immutable manifest and confirms the raw data object
// exists with the declared size without downloading it when StatBackend is
// available.
func (s *Store) StatBlob(ctx context.Context, ref BlobRef) (BlobRef, error) {
	stored, err := s.statBlobID(ctx, ref.Hash)
	if err != nil {
		return BlobRef{}, err
	}
	ref = normalizeRef(ref)
	if ref.Size != 0 || len(ref.Encoding) > 0 || len(ref.Encryption) > 0 || len(ref.Metadata) > 0 {
		if !equalJSON(stored, ref) {
			return BlobRef{}, fmt.Errorf("%w: blob reference metadata differs", ErrCorrupt)
		}
	}
	return stored, nil
}
func (s *Store) StatBlobID(ctx context.Context, id BlobID) (BlobRef, error) {
	return s.statBlobID(ctx, id)
}
func (s *Store) statBlobID(ctx context.Context, id BlobID) (BlobRef, error) {
	if err := validateBlobID(id); err != nil {
		return BlobRef{}, err
	}
	var obj *s3backend.Object
	err := s.runBackend(ctx, "stat_blob", func() error { var e error; obj, e = s.backend.Get(ctx, s.blobMetaKey(id)); return e })
	if errors.Is(err, s3backend.ErrNotFound) {
		return BlobRef{}, ErrNotFound
	}
	if err != nil {
		return BlobRef{}, err
	}
	var m blobManifest
	if err = decodeCanonical(obj.Body, &m); err != nil {
		return BlobRef{}, &CorruptError{Key: s.blobMetaKey(id), Reason: err.Error()}
	}
	if m.V != 1 || m.Hash != id || m.Size < s.opts.MinBlobBytes || m.Size > s.opts.MaxBlobBytes {
		return BlobRef{}, &CorruptError{Key: s.blobMetaKey(id), Reason: "invalid manifest fields"}
	}
	st, err := s.statObject(ctx, s.blobDataKey(id))
	if errors.Is(err, s3backend.ErrNotFound) {
		return BlobRef{}, &CorruptError{Key: s.blobDataKey(id), Reason: "data missing"}
	}
	if err != nil {
		return BlobRef{}, err
	}
	if st.Size != m.Size {
		return BlobRef{}, &CorruptError{Key: st.Key, Reason: "size differs from manifest"}
	}
	return m.ref(), nil
}
func (s *Store) statObject(ctx context.Context, key string) (*s3backend.StatObject, error) {
	var st *s3backend.StatObject
	err := s.runBackend(ctx, "stat", func() error {
		if b, ok := s.backend.(s3backend.StatBackend); ok {
			var e error
			st, e = b.Stat(ctx, key)
			return e
		}
		o, e := s.backend.Get(ctx, key)
		if e == nil {
			st = &s3backend.StatObject{Key: key, ETag: o.ETag, ModTime: o.ModTime, Size: int64(len(o.Body))}
		}
		return e
	})
	return st, err
}

func (s *Store) GetBlob(ctx context.Context, ref BlobRef, options ...BlobGetOption) (*BlobReader, error) {
	stored := normalizeRef(ref)
	if err := validateBlobID(stored.Hash); err != nil {
		return nil, err
	}
	if stored.Size < s.opts.MinBlobBytes || stored.Size > s.opts.MaxBlobBytes {
		return nil, ErrCorrupt
	}
	var o BlobGetOptions
	for _, opt := range options {
		if opt != nil {
			opt.applyBlobGet(&o)
		}
	}
	if o.Range != nil {
		r := *o.Range
		if r.Offset < 0 || r.Length <= 0 || r.Offset >= stored.Size || r.Length > stored.Size-r.Offset {
			return nil, ErrInvalidRange
		}
		b, ok := s.backend.(s3backend.RangeBackend)
		if !ok {
			return nil, fmt.Errorf("%w: range GET", ErrBackendCapability)
		}
		so, e := b.GetRange(ctx, s.blobDataKey(stored.Hash), r.Offset, r.Length)
		if errors.Is(e, s3backend.ErrNotFound) {
			return nil, &CorruptError{Key: s.blobDataKey(stored.Hash), Reason: "data missing"}
		}
		if e != nil {
			return nil, e
		}
		if so.Size != r.Length {
			_ = so.Body.Close()
			return nil, &CorruptError{Key: s.blobDataKey(stored.Hash), Reason: "short range"}
		}
		return &BlobReader{ReadCloser: &rangeLengthReader{r: so.Body, remaining: r.Length}, Ref: stored, Range: &r}, nil
	}
	raw, e := s.openRawBlob(ctx, stored.Hash)
	if errors.Is(e, s3backend.ErrNotFound) {
		return nil, &CorruptError{Key: s.blobDataKey(stored.Hash), Reason: "data missing"}
	}
	if e != nil {
		return nil, e
	}
	return &BlobReader{ReadCloser: &verifyingReader{r: raw, want: stored.Hash, wantSize: stored.Size, h: sha256.New()}, Ref: stored}, nil
}
func (s *Store) GetBlobByID(ctx context.Context, id BlobID, options ...BlobGetOption) (*BlobReader, error) {
	ref, err := s.StatBlobID(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.GetBlob(ctx, ref, options...)
}

func (s *Store) openRawBlob(ctx context.Context, id BlobID) (io.ReadCloser, error) {
	if b, ok := s.backend.(s3backend.StreamBackend); ok {
		so, e := b.GetStream(ctx, s.blobDataKey(id))
		if e != nil {
			return nil, e
		}
		return so.Body, nil
	}
	o, e := s.backend.Get(ctx, s.blobDataKey(id))
	if e != nil {
		return nil, e
	}
	return io.NopCloser(bytes.NewReader(o.Body)), nil
}
func (s *Store) verifyBlobData(ctx context.Context, id BlobID, size int64) error {
	r, err := s.openRawBlob(ctx, id)
	if err != nil {
		return err
	}
	defer r.Close()
	h := sha256.New()
	n, err := io.Copy(h, &contextReader{ctx: ctx, r: r})
	if err != nil {
		return err
	}
	if n != size || subtle.ConstantTimeCompare([]byte(hex.EncodeToString(h.Sum(nil))), []byte(id)) != 1 {
		return ErrHashMismatch
	}
	return nil
}

type rangeLengthReader struct {
	r         io.ReadCloser
	remaining int64
	done      bool
}

func (r *rangeLengthReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n > 0 {
			return 0, &CorruptError{Reason: "range body exceeds declared length"}
		}
		if err == io.EOF {
			r.done = true
			return 0, io.EOF
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	if err == io.EOF && r.remaining != 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}
func (r *rangeLengthReader) Close() error { return r.r.Close() }

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}

type verifyingReader struct {
	r           io.ReadCloser
	want        BlobID
	wantSize, n int64
	h           hash.Hash
	done        bool
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		_, _ = v.h.Write(p[:n])
		v.n += int64(n)
	}
	if err == io.EOF && !v.done {
		v.done = true
		if v.n != v.wantSize || subtle.ConstantTimeCompare([]byte(hex.EncodeToString(v.h.Sum(nil))), []byte(v.want)) != 1 {
			return n, ErrHashMismatch
		}
	}
	return n, err
}
func (v *verifyingReader) Close() error { return v.r.Close() }

func decodeCanonical(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing JSON")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	canonical, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, body) {
		return errors.New("non-canonical JSON")
	}
	return nil
}
