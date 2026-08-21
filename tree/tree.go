// Package tree implements a content-addressed Merkle tree on top of
// storage.Engine, with manifest-tracked blobs, compare-and-swap refs,
// lease fencing, and reachability-based garbage collection.
package tree

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"

	"github.com/damianb/s3collections/storage"
)

// Sentinel errors returned by the tree package.
var (
	// ErrHashMismatch is returned when blob content does not match the
	// expected hash.
	ErrHashMismatch = errors.New("tree: hash mismatch")
	// ErrRefConflict is returned when a ref compare-and-swap fails.
	ErrRefConflict = errors.New("tree: ref conflict")
	// ErrFenced is returned when a lease fencing token is stale.
	ErrFenced = errors.New("tree: fenced")
	// ErrNotFound is returned when a tree object does not exist.
	ErrNotFound = errors.New("tree: not found")
)

// Key layout in the metadata KV and blob store.
const (
	blobPrefix     = "blobs/"    // blob store: raw blob bytes by hash
	manifestPrefix = "manifest/" // KV: blob manifest JSON by hash
	nodePrefix     = "nodes/"    // KV: node JSON by node ID
	refPrefix      = "refs/"     // KV: ref JSON by name
	leasePrefix    = "leases/"   // KV: lease JSON by name
)

// Manifest describes a stored blob: its logical key, content hash, and size.
type Manifest struct {
	Key       string `json:"key"`
	Hash      string `json:"hash"`
	Size      int64  `json:"size"`
	ObjectKey string `json:"object_key"`
}

// Node is a deterministic Merkle tree node. A leaf node references blob
// hashes; an internal node references child node IDs. Children are always
// sorted by name, so a node's ID is a pure function of its content.
type Node struct {
	Name     string   `json:"name"`
	Leaf     bool     `json:"leaf"`
	Blobs    []string `json:"blobs,omitempty"`    // leaf: blob hashes
	Children []string `json:"children,omitempty"` // internal: child node IDs
}

// ID returns the deterministic content address of n.
func (n Node) ID() string {
	c := n.canonical()
	sum := sha256.Sum256(c)
	return hex.EncodeToString(sum[:])
}

// canonical returns the deterministic encoding of n.
func (n Node) canonical() []byte {
	var buf bytes.Buffer
	buf.WriteString(n.Name)
	buf.WriteByte(0)
	if n.Leaf {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	blobs := append([]string(nil), n.Blobs...)
	sort.Strings(blobs)
	for _, b := range blobs {
		buf.WriteByte(2)
		buf.WriteString(b)
	}
	children := append([]string(nil), n.Children...)
	sort.Strings(children)
	for _, c := range children {
		buf.WriteByte(3)
		buf.WriteString(c)
	}
	return buf.Bytes()
}

// Ref is a named pointer to a root node ID with a CAS version.
type Ref struct {
	Name    string `json:"name"`
	NodeID  string `json:"node_id"`
	Version uint64 `json:"version"`
}

// Lease is a named lease with a monotonically increasing fencing token.
type Lease struct {
	Name  string `json:"name"`
	Token uint64 `json:"token"`
}

// PutOptions controls PutBlob behavior.
type PutOptions struct {
	// Key is the logical key recorded in the manifest. Defaults to the hash.
	Key string
	// Size is the expected blob size; < 0 means unknown.
	Size int64
}

// Tree is a content-addressed tree store over a storage.Engine.
type Tree struct {
	eng *storage.Engine
}

// New returns a Tree backed by eng.
func New(eng *storage.Engine) *Tree {
	return &Tree{eng: eng}
}

func (t *Tree) transaction(ctx context.Context, fn func(storage.Tx) error) error {
	const maxAttempts = 16
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := t.eng.Metadata.Transaction(ctx, fn)
		if !errors.Is(err, storage.ErrConflict) {
			return err
		}
	}
	return storage.ErrConflict
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// PutBlob streams r into the blob store, verifies the content hash against
// expectedHash, and records a manifest. expectedHash is a lowercase hex
// SHA-256; pass "" to skip verification (the computed hash is returned).
func (t *Tree) PutBlob(ctx context.Context, expectedHash string, r io.Reader, opts PutOptions) (Manifest, error) {
	h := sha256.New()
	tee := io.TeeReader(r, h)
	size := opts.Size
	stagingKey := blobPrefix + "staging/" + randomHex(16)
	if err := t.eng.Blobs.Put(ctx, stagingKey, tee, size); err != nil {
		return Manifest{}, fmt.Errorf("tree: put blob: %w", err)
	}
	hash := hex.EncodeToString(h.Sum(nil))
	if expectedHash != "" && hash != expectedHash {
		_ = t.eng.Blobs.Delete(ctx, stagingKey)
		return Manifest{}, ErrHashMismatch
	}
	if size < 0 {
		info, statErr := t.eng.Blobs.Stat(ctx, stagingKey)
		if statErr != nil {
			_ = t.eng.Blobs.Delete(ctx, stagingKey)
			return Manifest{}, fmt.Errorf("tree: stat staged blob: %w", statErr)
		}
		size = info.Size
	}
	// The unique staging upload is already an immutable final body. Publish
	// its object key in the compact manifest; no second transfer is needed.
	m := Manifest{Key: opts.Key, Hash: hash, Size: size, ObjectKey: stagingKey}
	if m.Key == "" {
		m.Key = hash
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return Manifest{}, err
	}
	duplicate := false
	err = t.transaction(ctx, func(tx storage.Tx) error {
		duplicate = false
		existing, getErr := tx.Get(manifestPrefix + hash)
		if getErr == nil {
			var prior Manifest
			if jsonErr := json.Unmarshal(existing, &prior); jsonErr != nil {
				return fmt.Errorf("tree: bad existing manifest: %w", jsonErr)
			}
			m = prior
			duplicate = true
			return nil
		}
		if !errors.Is(getErr, storage.ErrNotFound) {
			return getErr
		}
		return tx.Put(manifestPrefix+hash, raw)
	})
	if err != nil {
		_ = t.eng.Blobs.Delete(ctx, stagingKey)
		return Manifest{}, fmt.Errorf("tree: publish manifest: %w", err)
	}
	if duplicate && m.ObjectKey != stagingKey {
		_ = t.eng.Blobs.Delete(ctx, stagingKey)
	}
	return m, nil
}

// StatBlob returns the manifest for hash without reading the blob.
func (t *Tree) StatBlob(ctx context.Context, hash string) (Manifest, error) {
	raw, err := t.eng.Metadata.Get(ctx, manifestPrefix+hash)
	if errors.Is(err, storage.ErrNotFound) {
		return Manifest{}, ErrNotFound
	}
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return Manifest{}, fmt.Errorf("tree: bad manifest: %w", err)
	}
	return m, nil
}

// GetBlob opens the blob for hash and returns a reader that verifies the
// content hash on EOF. start/end select a byte range [start, end); pass
// (0, -1) for the whole blob. A ranged read verifies the range length
// against the manifest; a full read verifies the content hash.
func (t *Tree) GetBlob(ctx context.Context, hash string, start, end int64) (io.ReadCloser, error) {
	m, err := t.StatBlob(ctx, hash)
	if err != nil {
		return nil, err
	}
	if end < 0 || end > m.Size {
		end = m.Size
	}
	if start < 0 || start > end {
		return nil, fmt.Errorf("tree: invalid range [%d, %d)", start, end)
	}
	objectKey := m.ObjectKey
	if objectKey == "" { // compatibility with early manifests
		objectKey = blobPrefix + hash
	}
	rc, err := t.eng.Blobs.OpenRange(ctx, objectKey, start, end)
	if err != nil {
		return nil, fmt.Errorf("tree: open blob: %w", err)
	}
	full := start == 0 && end == m.Size
	return &verifyReader{rc: rc, want: end - start, full: full, hash: hash, h: sha256.New()}, nil
}

// verifyReader checks the read length, and for full reads the content hash.
type verifyReader struct {
	rc   io.ReadCloser
	want int64
	n    int64
	full bool
	hash string
	h    hash.Hash
}

func (v *verifyReader) Read(p []byte) (int, error) {
	n, err := v.rc.Read(p)
	if n > 0 {
		v.n += int64(n)
		if v.full {
			_, _ = v.h.Write(p[:n])
		}
	}
	if errors.Is(err, io.EOF) {
		if v.n != v.want {
			return n, fmt.Errorf("tree: short read: got %d, want %d", v.n, v.want)
		}
		if v.full {
			if sum := hex.EncodeToString(v.h.Sum(nil)); sum != v.hash {
				return n, ErrHashMismatch
			}
		}
	}
	return n, err
}

func (v *verifyReader) Close() error { return v.rc.Close() }

// PutNode stores n under its deterministic ID and returns the ID.
func (t *Tree) PutNode(ctx context.Context, n Node) (string, error) {
	sort.Strings(n.Blobs)
	sort.Strings(n.Children)
	raw, err := json.Marshal(n)
	if err != nil {
		return "", err
	}
	if len(raw) > 1<<20 {
		return "", fmt.Errorf("tree: node metadata exceeds 1 MiB")
	}
	id := n.ID()
	if err := t.eng.Metadata.Put(ctx, nodePrefix+id, raw); err != nil {
		return "", fmt.Errorf("tree: put node: %w", err)
	}
	return id, nil
}

// GetNode loads the node with the given ID and verifies its address.
func (t *Tree) GetNode(ctx context.Context, id string) (Node, error) {
	raw, err := t.eng.Metadata.Get(ctx, nodePrefix+id)
	if errors.Is(err, storage.ErrNotFound) {
		return Node{}, ErrNotFound
	}
	if err != nil {
		return Node{}, err
	}
	var n Node
	if err := json.Unmarshal(raw, &n); err != nil {
		return Node{}, fmt.Errorf("tree: bad node: %w", err)
	}
	if got := n.ID(); got != id {
		return Node{}, fmt.Errorf("tree: node id mismatch: got %s, want %s", got, id)
	}
	return n, nil
}

// GetRef returns the current value of ref name, or ErrNotFound.
func (t *Tree) GetRef(ctx context.Context, name string) (Ref, error) {
	raw, err := t.eng.Metadata.Get(ctx, refPrefix+name)
	if errors.Is(err, storage.ErrNotFound) {
		return Ref{}, ErrNotFound
	}
	if err != nil {
		return Ref{}, err
	}
	var r Ref
	if err := json.Unmarshal(raw, &r); err != nil {
		return Ref{}, fmt.Errorf("tree: bad ref: %w", err)
	}
	return r, nil
}

// checkFenceTx validates a lease token inside the same transaction as a
// protected metadata mutation.
func checkFenceTx(tx storage.Tx, name string, token uint64) error {
	raw, err := tx.Get(leasePrefix + name)
	if errors.Is(err, storage.ErrNotFound) {
		return ErrFenced
	}
	if err != nil {
		return err
	}
	var lease Lease
	if err := json.Unmarshal(raw, &lease); err != nil {
		return fmt.Errorf("tree: bad lease: %w", err)
	}
	if lease.Token != token {
		return ErrFenced
	}
	return nil
}

// CompareAndSwapRef atomically sets ref name to nodeID when the current
// version equals expectVersion. A new ref is created when expectVersion is
// 0 and the ref does not exist.
func (t *Tree) CompareAndSwapRef(ctx context.Context, name, nodeID string, expectVersion uint64) (Ref, error) {
	var out Ref
	err := t.transaction(ctx, func(tx storage.Tx) error {
		if _, err := tx.Get(nodePrefix + nodeID); errors.Is(err, storage.ErrNotFound) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		var r Ref
		raw, err := tx.Get(refPrefix + name)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			r = Ref{Name: name}
		case err != nil:
			return err
		default:
			if err := json.Unmarshal(raw, &r); err != nil {
				return fmt.Errorf("tree: bad ref: %w", err)
			}
		}
		if r.Version != expectVersion {
			return ErrRefConflict
		}
		r.NodeID = nodeID
		r.Version++
		buf, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if err := tx.Put(refPrefix+name, buf); err != nil {
			return err
		}
		out = r
		return nil
	})
	if err != nil {
		return Ref{}, err
	}
	return out, nil
}

// CompareAndSwapRefFenced updates a ref only when both its version and the
// named lease token are current. The fence check and ref mutation occur in
// one serializable metadata transaction, so a stale holder cannot pass a
// separate check and race a newer lease.
func (t *Tree) CompareAndSwapRefFenced(ctx context.Context, leaseName string, token uint64, name, nodeID string, expectVersion uint64) (Ref, error) {
	var out Ref
	err := t.transaction(ctx, func(tx storage.Tx) error {
		if err := checkFenceTx(tx, leaseName, token); err != nil {
			return err
		}
		if _, err := tx.Get(nodePrefix + nodeID); errors.Is(err, storage.ErrNotFound) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		var ref Ref
		raw, err := tx.Get(refPrefix + name)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			ref = Ref{Name: name}
		case err != nil:
			return err
		default:
			if err := json.Unmarshal(raw, &ref); err != nil {
				return fmt.Errorf("tree: bad ref: %w", err)
			}
		}
		if ref.Version != expectVersion {
			return ErrRefConflict
		}
		ref.NodeID = nodeID
		ref.Version++
		buf, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		if err := tx.Put(refPrefix+name, buf); err != nil {
			return err
		}
		out = ref
		return nil
	})
	return out, err
}

// DeleteRef atomically removes a ref when its version matches expectVersion.
func (t *Tree) DeleteRef(ctx context.Context, name string, expectVersion uint64) error {
	return t.transaction(ctx, func(tx storage.Tx) error {
		raw, err := tx.Get(refPrefix + name)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var ref Ref
		if err := json.Unmarshal(raw, &ref); err != nil {
			return fmt.Errorf("tree: bad ref: %w", err)
		}
		if ref.Version != expectVersion {
			return ErrRefConflict
		}
		return tx.Delete(refPrefix + name)
	})
}

// DeleteRefFenced combines lease validation and ref deletion atomically.
func (t *Tree) DeleteRefFenced(ctx context.Context, leaseName string, token uint64, name string, expectVersion uint64) error {
	return t.transaction(ctx, func(tx storage.Tx) error {
		if err := checkFenceTx(tx, leaseName, token); err != nil {
			return err
		}
		raw, err := tx.Get(refPrefix + name)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var ref Ref
		if err := json.Unmarshal(raw, &ref); err != nil {
			return fmt.Errorf("tree: bad ref: %w", err)
		}
		if ref.Version != expectVersion {
			return ErrRefConflict
		}
		return tx.Delete(refPrefix + name)
	})
}

// AcquireLease creates or renews the named lease and returns its new
// fencing token. Tokens increase monotonically per lease name.
func (t *Tree) AcquireLease(ctx context.Context, name string) (Lease, error) {
	var out Lease
	err := t.transaction(ctx, func(tx storage.Tx) error {
		var l Lease
		raw, err := tx.Get(leasePrefix + name)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			l = Lease{Name: name}
		case err != nil:
			return err
		default:
			if err := json.Unmarshal(raw, &l); err != nil {
				return fmt.Errorf("tree: bad lease: %w", err)
			}
		}
		l.Token++
		buf, err := json.Marshal(l)
		if err != nil {
			return err
		}
		if err := tx.Put(leasePrefix+name, buf); err != nil {
			return err
		}
		out = l
		return nil
	})
	if err != nil {
		return Lease{}, err
	}
	return out, nil
}

// CheckFence returns ErrFenced unless token equals the lease's current token.
func (t *Tree) CheckFence(ctx context.Context, name string, token uint64) error {
	raw, err := t.eng.Metadata.Get(ctx, leasePrefix+name)
	if errors.Is(err, storage.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	var l Lease
	if err := json.Unmarshal(raw, &l); err != nil {
		return fmt.Errorf("tree: bad lease: %w", err)
	}
	if l.Token != token {
		return ErrFenced
	}
	return nil
}

// ReleaseLease removes the lease only if its fencing token is still current.
func (t *Tree) ReleaseLease(ctx context.Context, lease Lease) error {
	return t.transaction(ctx, func(tx storage.Tx) error {
		if err := checkFenceTx(tx, lease.Name, lease.Token); err != nil {
			return err
		}
		return tx.Delete(leasePrefix + lease.Name)
	})
}

// GCPlan is the set of unreachable objects to sweep.
type GCPlan struct {
	Nodes      []string          // unreachable node IDs
	Blobs      []string          // unreachable blob hashes
	ObjectKeys map[string]string // immutable body key captured during planning
}

// PlanGC walks the graph from all refs and returns every node and blob
// that is not reachable.
func (t *Tree) PlanGC(ctx context.Context) (GCPlan, error) {
	reachableNodes := map[string]bool{}
	reachableBlobs := map[string]bool{}
	refs, err := t.eng.Metadata.Scan(ctx, storage.ScanOptions{Prefix: refPrefix})
	if err != nil {
		return GCPlan{}, err
	}
	var walk func(id string) error
	walk = func(id string) error {
		if reachableNodes[id] {
			return nil
		}
		n, err := t.GetNode(ctx, id)
		if err != nil {
			return err
		}
		reachableNodes[id] = true
		for _, b := range n.Blobs {
			reachableBlobs[b] = true
		}
		for _, c := range n.Children {
			if err := walk(c); err != nil {
				return err
			}
		}
		return nil
	}
	for _, e := range refs {
		var r Ref
		if err := json.Unmarshal(e.Value, &r); err != nil {
			return GCPlan{}, fmt.Errorf("tree: bad ref: %w", err)
		}
		if r.NodeID == "" {
			continue
		}
		if err := walk(r.NodeID); err != nil {
			return GCPlan{}, err
		}
	}
	plan := GCPlan{ObjectKeys: make(map[string]string)}
	nodes, err := t.eng.Metadata.Scan(ctx, storage.ScanOptions{Prefix: nodePrefix})
	if err != nil {
		return GCPlan{}, err
	}
	for _, e := range nodes {
		id := e.Key[len(nodePrefix):]
		if !reachableNodes[id] {
			plan.Nodes = append(plan.Nodes, id)
		}
	}
	manifests, err := t.eng.Metadata.Scan(ctx, storage.ScanOptions{Prefix: manifestPrefix})
	if err != nil {
		return GCPlan{}, err
	}
	for _, e := range manifests {
		h := e.Key[len(manifestPrefix):]
		if !reachableBlobs[h] {
			plan.Blobs = append(plan.Blobs, h)
			var manifest Manifest
			if err := json.Unmarshal(e.Value, &manifest); err != nil {
				return GCPlan{}, fmt.Errorf("tree: bad manifest: %w", err)
			}
			if manifest.ObjectKey != "" {
				plan.ObjectKeys[h] = manifest.ObjectKey
			}
		}
	}
	sort.Strings(plan.Nodes)
	sort.Strings(plan.Blobs)
	return plan, nil
}

// SweepGC deletes every object in plan.
func (t *Tree) bodyObjectKey(ctx context.Context, hash string) string {
	manifest, err := t.StatBlob(ctx, hash)
	if err == nil && manifest.ObjectKey != "" {
		return manifest.ObjectKey
	}
	return blobPrefix + hash
}

// SweepGCFenced removes planned metadata only while the lease token remains
// current. Metadata deletion and the fence check are atomic. Blob deletion
// follows the transaction and is therefore safe only while callers serialize
// reference publication through the same lease; failed body deletes may be
// retried with the unchanged plan.
func (t *Tree) SweepGCFenced(ctx context.Context, leaseName string, token uint64, plan GCPlan) error {
	bodyKeys := make(map[string]string, len(plan.Blobs))
	for _, hash := range plan.Blobs {
		bodyKeys[hash] = plan.ObjectKeys[hash]
		if bodyKeys[hash] == "" {
			bodyKeys[hash] = t.bodyObjectKey(ctx, hash)
		}
	}
	err := t.transaction(ctx, func(tx storage.Tx) error {
		if err := checkFenceTx(tx, leaseName, token); err != nil {
			return err
		}
		for _, id := range plan.Nodes {
			if err := tx.Delete(nodePrefix + id); err != nil {
				return err
			}
		}
		for _, hash := range plan.Blobs {
			if err := tx.Delete(manifestPrefix + hash); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, hash := range plan.Blobs {
		if err := t.eng.Blobs.Delete(ctx, bodyKeys[hash]); err != nil {
			return fmt.Errorf("tree: sweep blob %s: %w", hash, err)
		}
	}
	return nil
}

// SweepGC performs an unfenced sweep for single-writer deployments. Use
// SweepGCFenced when mutation ownership can change between processes.
func (t *Tree) SweepGC(ctx context.Context, plan GCPlan) error {
	for _, id := range plan.Nodes {
		if err := t.eng.Metadata.Delete(ctx, nodePrefix+id); err != nil {
			return fmt.Errorf("tree: sweep node %s: %w", id, err)
		}
	}
	for _, h := range plan.Blobs {
		objectKey := plan.ObjectKeys[h]
		if objectKey == "" {
			objectKey = t.bodyObjectKey(ctx, h)
		}
		if err := t.eng.Metadata.Delete(ctx, manifestPrefix+h); err != nil {
			return fmt.Errorf("tree: sweep manifest %s: %w", h, err)
		}
		if err := t.eng.Blobs.Delete(ctx, objectKey); err != nil {
			return fmt.Errorf("tree: sweep blob %s: %w", h, err)
		}
	}
	return nil
}
