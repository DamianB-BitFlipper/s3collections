package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/damianb/s3collections/s3backend"
)

type nodeManifest struct {
	V              int       `json:"v"`
	Parent         *NodeID   `json:"parent"`
	Root           *NodeID   `json:"root"`
	Depth          uint64    `json:"depth"`
	Payloads       []BlobRef `json:"payloads"`
	OpaqueMetadata []byte    `json:"opaque_metadata"`
}

func canonicalNode(parent, root *NodeID, depth uint64, payloads []BlobRef, metadata []byte) (nodeManifest, []byte, error) {
	ps := cloneRefs(payloads)
	for _, r := range ps {
		if err := validateBlobID(r.Hash); err != nil {
			return nodeManifest{}, nil, err
		}
		if r.Size < 0 {
			return nodeManifest{}, nil, ErrCorrupt
		}
	}
	w := nodeManifest{V: 1, Parent: cloneNodeID(parent), Root: cloneNodeID(root), Depth: depth, Payloads: ps, OpaqueMetadata: cloneBytes(metadata)}
	b, err := json.Marshal(w)
	return w, b, err
}
func cloneNodeID(v *NodeID) *NodeID {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}
func nodeFromManifest(id NodeID, w nodeManifest) Node {
	return Node{ID: id, ParentID: cloneNodeID(w.Parent), RootID: cloneNodeID(w.Root), Depth: w.Depth, Payloads: cloneRefs(w.Payloads), OpaqueMetadata: cloneBytes(w.OpaqueMetadata)}
}

func (s *Store) CommitRoot(ctx context.Context, payloads []BlobRef, metadata []byte) (id NodeID, err error) {
	err = s.withMutationGate(ctx, "commit_root", func(g *mutationGate) error {
		if e := s.verifyPayloads(ctx, payloads); e != nil {
			return e
		}
		_, body, e := canonicalNode(nil, nil, 0, payloads, metadata)
		if e != nil {
			return e
		}
		if e = s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		id, e = s.publishNode(ctx, body)
		if e != nil {
			return e
		}
		if e = s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		return s.publishRecentPin(ctx, id)
	})
	return id, err
}
func (s *Store) CommitChild(ctx context.Context, parentID NodeID, payloads []BlobRef, metadata []byte) (id NodeID, err error) {
	err = s.withMutationGate(ctx, "commit_child", func(g *mutationGate) error {
		parent, e := s.verifyNodeForLiveness(ctx, parentID)
		if e != nil {
			return e
		}
		if parent.Depth == math.MaxUint64 || parent.Depth+1 >= s.opts.MaxLineageDepth {
			return ErrMaxDepth
		}
		if e = s.verifyPayloads(ctx, payloads); e != nil {
			return e
		}
		root := parent.ID
		if parent.RootID != nil {
			root = *parent.RootID
		}
		_, body, e := canonicalNode(&parentID, &root, parent.Depth+1, payloads, metadata)
		if e != nil {
			return e
		}
		if e = s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		id, e = s.publishNode(ctx, body)
		if e != nil {
			return e
		}
		if e = s.renewMutationGate(ctx, g); e != nil {
			return e
		}
		return s.publishRecentPin(ctx, id)
	})
	if err != nil {
		return id, err
	}
	edgeBody, _ := json.Marshal(struct {
		V      int    `json:"v"`
		Parent NodeID `json:"parent"`
		Child  NodeID `json:"child"`
	}{1, parentID, id})
	if err = s.putImmutable(ctx, "put_edge", s.edgeKey(parentID, id), edgeBody); err != nil {
		return id, fmt.Errorf("tree: node %s committed; reverse edge pending: %w", id, err)
	}
	return id, nil
}
func (s *Store) verifyNodeForLiveness(ctx context.Context, id NodeID) (Node, error) {
	line, err := s.ResolveLineage(ctx, id, nil)
	if err != nil {
		return Node{}, err
	}
	for _, n := range line {
		if err = s.verifyPayloads(ctx, n.Payloads); err != nil {
			return Node{}, err
		}
	}
	return line[len(line)-1], nil
}

func (s *Store) verifyPayloads(ctx context.Context, refs []BlobRef) error {
	for _, ref := range refs {
		stored, err := s.StatBlob(ctx, ref)
		if err != nil {
			return fmt.Errorf("tree: payload %s: %w", ref.Hash, err)
		}
		if !equalJSON(stored, normalizeRef(ref)) {
			return fmt.Errorf("%w: blob reference differs from manifest", ErrCorrupt)
		}
	}
	return nil
}
func (s *Store) publishNode(ctx context.Context, body []byte) (NodeID, error) {
	id := NodeID(hashBytes(body))
	if err := s.putImmutable(ctx, "commit_node", s.nodeKey(id), body); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) GetNode(ctx context.Context, id NodeID) (Node, error) {
	if err := validateNodeID(id); err != nil {
		return Node{}, err
	}
	var obj *s3backend.Object
	err := s.runBackend(ctx, "get_node", func() error { var e error; obj, e = s.backend.Get(ctx, s.nodeKey(id)); return e })
	if errors.Is(err, s3backend.ErrNotFound) {
		return Node{}, ErrNotFound
	}
	if err != nil {
		return Node{}, err
	}
	if NodeID(hashBytes(obj.Body)) != id {
		return Node{}, &CorruptError{Key: s.nodeKey(id), Reason: "content hash mismatch"}
	}
	var w nodeManifest
	if err = decodeCanonical(obj.Body, &w); err != nil {
		return Node{}, &CorruptError{Key: s.nodeKey(id), Reason: err.Error()}
	}
	if w.V != 1 {
		return Node{}, &CorruptError{Key: s.nodeKey(id), Reason: "unsupported version"}
	}
	if w.Parent == nil {
		if w.Root != nil || w.Depth != 0 {
			return Node{}, &CorruptError{Key: s.nodeKey(id), Reason: "invalid root fields"}
		}
	} else {
		if w.Root == nil || w.Depth == 0 || validateNodeID(*w.Parent) != nil || validateNodeID(*w.Root) != nil {
			return Node{}, &CorruptError{Key: s.nodeKey(id), Reason: "invalid child fields"}
		}
	}
	for _, ref := range w.Payloads {
		if validateBlobID(ref.Hash) != nil || ref.Size < s.opts.MinBlobBytes || ref.Size > s.opts.MaxBlobBytes {
			return Node{}, &CorruptError{Key: s.nodeKey(id), Reason: "invalid payload reference"}
		}
	}
	_, normalized, nerr := canonicalNode(w.Parent, w.Root, w.Depth, w.Payloads, w.OpaqueMetadata)
	if nerr != nil || !bytes.Equal(normalized, obj.Body) {
		return Node{}, &CorruptError{Key: s.nodeKey(id), Reason: "non-normalized node manifest"}
	}
	return nodeFromManifest(id, w), nil
}

func (s *Store) Root(ctx context.Context, id NodeID) (NodeID, error) {
	line, err := s.ResolveLineage(ctx, id, nil)
	if err != nil {
		return "", err
	}
	return line[0].ID, nil
}

// ResolveLineage returns the included boundary/root through target in that
// order. Stop is caller-defined and evaluated target-to-root; a matching node
// is included. This package assigns no meaning to opaque metadata.
func (s *Store) ResolveLineage(ctx context.Context, id NodeID, opts *LineageOptions) ([]Node, error) {
	if err := validateNodeID(id); err != nil {
		return nil, err
	}
	limit := s.opts.MaxLineageDepth
	if opts != nil && opts.MaxDepth > 0 && opts.MaxDepth < limit {
		limit = opts.MaxDepth
	}
	if opts != nil && opts.StopAfter != "" {
		if err := validateNodeID(opts.StopAfter); err != nil {
			return nil, err
		}
	}
	seen := make(map[NodeID]struct{})
	reverse := make([]Node, 0)
	cur := id
	var expectedRoot NodeID
	var child *Node
	for steps := uint64(0); ; steps++ {
		if steps >= limit {
			return nil, ErrMaxDepth
		}
		if _, ok := seen[cur]; ok {
			return nil, ErrCycle
		}
		seen[cur] = struct{}{}
		n, err := s.GetNode(ctx, cur)
		if err != nil {
			return nil, err
		}
		effective := n.ID
		if n.RootID != nil {
			effective = *n.RootID
		}
		if steps == 0 {
			expectedRoot = effective
		} else {
			if effective != expectedRoot {
				return nil, &CorruptError{Key: s.nodeKey(n.ID), Reason: "root id changed in lineage"}
			}
			if child == nil || child.ParentID == nil || *child.ParentID != n.ID || child.Depth != n.Depth+1 {
				return nil, &CorruptError{Key: s.nodeKey(child.ID), Reason: "depth/parent mismatch"}
			}
		}
		reverse = append(reverse, n)
		stop := false
		if opts != nil {
			stop = opts.StopAfter == n.ID && opts.StopAfter != ""
			if opts.Stop != nil && opts.Stop(n) {
				stop = true
			}
		}
		if stop {
			break
		}
		if n.ParentID == nil {
			if n.ID != expectedRoot || n.Depth != 0 {
				return nil, &CorruptError{Key: s.nodeKey(n.ID), Reason: "lineage terminates at wrong root"}
			}
			break
		}
		child = &reverse[len(reverse)-1]
		cur = *n.ParentID
	}
	for i, j := 0, len(reverse)-1; i < j; i, j = i+1, j-1 {
		reverse[i], reverse[j] = reverse[j], reverse[i]
	}
	return reverse, nil
}
func (s *Store) IsAncestor(ctx context.Context, ancestor, node NodeID) (bool, error) {
	if err := validateNodeID(ancestor); err != nil {
		return false, err
	}
	line, err := s.ResolveLineage(ctx, node, nil)
	if err != nil {
		return false, err
	}
	for _, n := range line {
		if n.ID == ancestor {
			return true, nil
		}
	}
	return false, nil
}
func (s *Store) LowestCommonAncestor(ctx context.Context, a, b NodeID) (NodeID, error) {
	la, err := s.ResolveLineage(ctx, a, nil)
	if err != nil {
		return "", err
	}
	lb, err := s.ResolveLineage(ctx, b, nil)
	if err != nil {
		return "", err
	}
	var out NodeID
	for i := 0; i < len(la) && i < len(lb) && la[i].ID == lb[i].ID; i++ {
		out = la[i].ID
	}
	if out == "" {
		return "", ErrNoCommonAncestor
	}
	return out, nil
}
func (s *Store) LCA(ctx context.Context, a, b NodeID) (NodeID, error) {
	return s.LowestCommonAncestor(ctx, a, b)
}

func (s *Store) ListChildren(ctx context.Context, parent NodeID) ([]NodeID, error) {
	if _, err := s.GetNode(ctx, parent); err != nil {
		return nil, err
	}
	prefix := s.prefix + "edges/" + string(parent) + "/"
	seen := map[NodeID]struct{}{}
	var out []NodeID
	token := ""
	for {
		var page *s3backend.ListPage
		err := s.runBackend(ctx, "list_children", func() error {
			var e error
			page, e = s.backend.List(ctx, prefix, &s3backend.ListOptions{ContinuationToken: token})
			return e
		})
		if err != nil {
			return nil, err
		}
		for _, info := range page.Objects {
			suffix := strings.TrimPrefix(info.Key, prefix)
			id := NodeID(suffix)
			if strings.Contains(suffix, "/") || validateNodeID(id) != nil {
				continue
			}
			n, e := s.GetNode(ctx, id)
			if errors.Is(e, ErrNotFound) {
				continue
			}
			if e != nil {
				return nil, e
			}
			if n.ParentID != nil && *n.ParentID == parent {
				if _, ok := seen[id]; !ok {
					seen[id] = struct{}{}
					out = append(out, id)
				}
			}
		}
		if !page.IsTruncated {
			break
		}
		if page.NextContinuationToken == "" || page.NextContinuationToken == token {
			return nil, &CorruptError{Key: prefix, Reason: "invalid continuation token"}
		}
		token = page.NextContinuationToken
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// RepairEdges reconstructs missing advisory reverse-edge markers by scanning
// immutable node manifests. Parent pointers remain authoritative throughout.
func (s *Store) RepairEdges(ctx context.Context) (int, error) {
	prefix := s.prefix + "n/"
	repaired := 0
	err := s.eachObject(ctx, prefix, func(info s3backend.ObjectInfo) error {
		id, ok := s.nodeIDFromKey(info.Key)
		if !ok {
			return nil
		}
		n, err := s.GetNode(ctx, id)
		if err != nil {
			return err
		}
		if n.ParentID == nil {
			return nil
		}
		key := s.edgeKey(*n.ParentID, n.ID)
		if _, err = s.backend.Get(ctx, key); err == nil {
			return nil
		} else if !errors.Is(err, s3backend.ErrNotFound) {
			return err
		}
		body, _ := json.Marshal(struct {
			V      int    `json:"v"`
			Parent NodeID `json:"parent"`
			Child  NodeID `json:"child"`
		}{1, *n.ParentID, n.ID})
		if err = s.putImmutable(ctx, "repair_edge", key, body); err != nil {
			return err
		}
		repaired++
		return nil
	})
	return repaired, err
}
