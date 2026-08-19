package tree

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/damianb/s3collections"
	"github.com/damianb/s3collections/s3backend"
)

type BlobID = string
type NodeID = string
type Descriptor []byte

// BlobRef identifies exact stored bytes. Descriptor fields are opaque and
// never interpreted or transformed by this package.
type BlobRef struct {
	Hash       BlobID     `json:"hash"`
	Size       int64      `json:"size"`
	Encoding   Descriptor `json:"encoding,omitempty"`
	Encryption Descriptor `json:"encryption,omitempty"`
	Metadata   []byte     `json:"metadata,omitempty"`
}

type BlobRange struct{ Offset, Length int64 }

type BlobPutOptions struct {
	ExpectedSize *int64
	Encoding     Descriptor
	Encryption   Descriptor
	Metadata     []byte
}
type BlobPutOption func(*BlobPutOptions)

func WithExpectedBlobSize(n int64) BlobPutOption {
	return func(o *BlobPutOptions) { o.ExpectedSize = &n }
}
func WithEncodingDescriptor(v []byte) BlobPutOption {
	return func(o *BlobPutOptions) { o.Encoding = cloneBytes(v) }
}
func WithEncryptionDescriptor(v []byte) BlobPutOption {
	return func(o *BlobPutOptions) { o.Encryption = cloneBytes(v) }
}
func WithBlobMetadata(v []byte) BlobPutOption {
	return func(o *BlobPutOptions) { o.Metadata = cloneBytes(v) }
}

type BlobGetOptions struct{ Range *BlobRange }
type BlobGetOption interface{ applyBlobGet(*BlobGetOptions) }
type blobGetOptionFunc func(*BlobGetOptions)

func (f blobGetOptionFunc) applyBlobGet(o *BlobGetOptions) { f(o) }
func (r *BlobRange) applyBlobGet(o *BlobGetOptions) {
	if r != nil {
		v := *r
		o.Range = &v
	}
}

func WithRange(offset, length int64) BlobGetOption {
	return blobGetOptionFunc(func(o *BlobGetOptions) { o.Range = &BlobRange{Offset: offset, Length: length} })
}

type BlobStat = BlobRef

type BlobReader struct {
	io.ReadCloser
	Ref   BlobRef
	Range *BlobRange
}

// Node is the parsed immutable manifest. A root has nil ParentID and RootID;
// Root(root.ID) is defined to be root.ID.
type Node struct {
	ID             NodeID    `json:"-"`
	ParentID       *NodeID   `json:"parent"`
	RootID         *NodeID   `json:"root"`
	Depth          uint64    `json:"depth"`
	Payloads       []BlobRef `json:"payloads"`
	OpaqueMetadata []byte    `json:"opaque_metadata"`
}

type LineageOptions struct {
	StopAfter NodeID
	Stop      func(Node) bool
	MaxDepth  uint64
}

type Ref struct {
	Name     string
	NodeID   NodeID
	Revision uint64
	ETag     string
}

type Lease struct {
	ID        string
	NodeID    NodeID
	Owner     string
	Token     string
	Fence     uint64
	TTL       time.Duration
	ExpiresAt time.Time
	ETag      string
}

type GCPlanOptions struct {
	Refs         []Ref
	ActiveLeases []Lease
	Roots        []NodeID
	Cutoff       time.Time
	Grace        time.Duration
}
type GCPlanOption func(*GCPlanOptions)

func WithGCRefs(refs ...Ref) GCPlanOption {
	return func(o *GCPlanOptions) { o.Refs = append([]Ref(nil), refs...) }
}
func WithGCLeases(leases ...Lease) GCPlanOption {
	return func(o *GCPlanOptions) { o.ActiveLeases = append([]Lease(nil), leases...) }
}
func WithGCRoots(roots ...NodeID) GCPlanOption {
	return func(o *GCPlanOptions) { o.Roots = append([]NodeID(nil), roots...) }
}
func WithPlanGrace(d time.Duration) GCPlanOption { return func(o *GCPlanOptions) { o.Grace = d } }

type GCObjectVersion struct {
	Key, ETag string
	Size      int64
	ModTime   time.Time
}
type GCNodeCandidate struct {
	ID     NodeID
	Depth  uint64
	Object GCObjectVersion
}
type GCBlobCandidate struct {
	ID             BlobID
	Data, Manifest *GCObjectVersion
}
type GCEdgeCandidate struct {
	Parent, Child NodeID
	Object        GCObjectVersion
}
type GCPlan struct {
	ID          string
	CreatedAt   time.Time
	NotBefore   time.Time
	Cutoff      time.Time
	PinnedRoots []NodeID
	Nodes       []GCNodeCandidate
	Blobs       []GCBlobCandidate
	Edges       []GCEdgeCandidate
}
type SweepResult struct{ NodesDeleted, BlobsDeleted, EdgesDeleted int }

type Store struct {
	name, prefix string
	backend      s3backend.Backend
	opts         Options
}

// Tree is a compatibility name for Store.
type Tree = Store

// New creates a named store. The name and reference names are base64url
// encoded in object keys, so user strings cannot escape the store prefix.
func New(backend s3backend.Backend, name string, opts ...Option) (*Store, error) {
	if backend == nil {
		return nil, errors.New("tree: nil backend")
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	o := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.MinBlobBytes < 0 || o.MaxBlobBytes <= 0 || o.MinBlobBytes > o.MaxBlobBytes {
		return nil, errors.New("tree: invalid blob size range")
	}
	if o.MultipartThreshold <= 0 || o.MaxBufferedPut < 0 {
		return nil, errors.New("tree: invalid upload thresholds")
	}
	if o.MaxLineageDepth == 0 {
		return nil, errors.New("tree: MaxLineageDepth must be positive")
	}
	if o.GCGrace <= 0 || o.ClockSkewHint < 0 || o.MutationGateTTL <= 0 {
		return nil, errors.New("tree: invalid coordination duration")
	}
	if o.Now == nil {
		return nil, errors.New("tree: nil clock")
	}
	base, err := normalizePrefix(o.Prefix)
	if err != nil {
		return nil, err
	}
	o.Prefix = base
	o.Retry = o.Retry.WithDefaults()
	o.Meter = s3collections.MeterOrNoop(o.Meter)
	o.Logger = s3collections.LoggerOrNoop(o.Logger)
	o.Tracer = s3collections.TracerOrNoop(o.Tracer)
	prefix := base + base64.RawURLEncoding.EncodeToString([]byte(name)) + "/"
	return &Store{name: name, prefix: prefix, backend: backend, opts: o}, nil
}
func NewStore(backend s3backend.Backend, name string, opts ...Option) (*Store, error) {
	return New(backend, name, opts...)
}
func (s *Store) Name() string { return s.name }

func validateName(v string) error {
	if v == "" || len(v) > 1024 || !utf8.ValidString(v) {
		return ErrInvalidName
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return ErrInvalidName
		}
	}
	return nil
}
func validateRefName(v string) error {
	if err := validateName(v); err != nil {
		return ErrInvalidRefName
	}
	return nil
}
func normalizePrefix(v string) (string, error) {
	v = strings.Trim(v, "/")
	if v == "" {
		return "", nil
	}
	if strings.Contains(v, "//") {
		return "", errors.New("tree: prefix contains empty segment")
	}
	for _, seg := range strings.Split(v, "/") {
		if seg == "." || seg == ".." {
			return "", errors.New("tree: invalid prefix")
		}
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return "", errors.New("tree: invalid prefix")
		}
	}
	return v + "/", nil
}
func validDigest(v string) bool {
	if len(v) != 64 || strings.ToLower(v) != v {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}
func validateBlobID(v BlobID) error {
	if !validDigest(string(v)) {
		return fmt.Errorf("%w: blob %q", ErrInvalidID, v)
	}
	return nil
}
func validateNodeID(v NodeID) error {
	if !validDigest(string(v)) {
		return fmt.Errorf("%w: node %q", ErrInvalidID, v)
	}
	return nil
}
func (s *Store) blobDataKey(id BlobID) string {
	h := string(id)
	return s.prefix + "b/" + h[:2] + "/" + h + "/data"
}
func (s *Store) blobMetaKey(id BlobID) string {
	h := string(id)
	return s.prefix + "b/" + h[:2] + "/" + h + "/manifest.json"
}
func (s *Store) nodeKey(id NodeID) string {
	h := string(id)
	return s.prefix + "n/" + h[:2] + "/" + h + ".json"
}
func (s *Store) edgeKey(p, c NodeID) string { return s.prefix + "edges/" + string(p) + "/" + string(c) }
func (s *Store) refKey(name string) string {
	return s.prefix + "r/" + base64.RawURLEncoding.EncodeToString([]byte(name)) + ".json"
}
func (s *Store) leaseKey(id string) string     { return s.prefix + "lease/" + id + ".json" }
func (s *Store) gateKey() string               { return s.prefix + "gc/gate.json" }
func (s *Store) recentPinKey(id NodeID) string { return s.prefix + "gc/recent/" + string(id) + ".json" }
func (s *Store) recentBlobPinKey(id BlobID) string {
	return s.prefix + "gc/recent-blobs/" + string(id) + ".json"
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func (s *Store) gcPlanKey(id string) string { return s.prefix + "gc/plans/" + id + ".json" }
func cloneBytes(v []byte) []byte {
	if len(v) == 0 {
		return nil
	}
	return append([]byte(nil), v...)
}
func normalizeRef(r BlobRef) BlobRef {
	r.Encoding = Descriptor(cloneBytes(r.Encoding))
	r.Encryption = Descriptor(cloneBytes(r.Encryption))
	r.Metadata = cloneBytes(r.Metadata)
	return r
}
func cloneRefs(in []BlobRef) []BlobRef {
	if len(in) == 0 {
		return []BlobRef{}
	}
	out := make([]BlobRef, len(in))
	for i := range in {
		out[i] = normalizeRef(in[i])
	}
	return out
}
func hashBytes(b []byte) string { v := sha256.Sum256(b); return hex.EncodeToString(v[:]) }
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func (s *Store) runBackend(ctx context.Context, op string, fn func() error) (retErr error) {
	start := time.Now()
	ctx, span := s.opts.Tracer.StartSpan(ctx, "s3collections.tree."+op, s3collections.L("component", "tree"), s3collections.L("op", op))
	defer func() {
		span.End(retErr)
		outcome := "success"
		if retErr != nil {
			outcome = "error"
		}
		s.opts.Meter.ObserveHistogram(ctx, "s3collections_latency_seconds", time.Since(start).Seconds(), s3collections.L("component", "tree"), s3collections.L("op", op), s3collections.L("outcome", outcome))
	}()
	p := s.opts.Retry
	next := s3collections.BackoffDelays(p, nil)
	attempts := 0
	defer func() {
		s.opts.Meter.ObserveHistogram(ctx, "s3collections_tree_attempts", float64(attempts), s3collections.L("component", "tree"), s3collections.L("op", op))
	}()
	for {
		attempts++
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s3backend.IsRetryable(err) || attempts >= p.MaxAttempts {
			return err
		}
		s.opts.Meter.IncCounter(ctx, "s3collections_retries_total", 1, s3collections.L("component", "tree"), s3collections.L("op", op), s3collections.L("reason", "backend"))
		timer := time.NewTimer(next())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
