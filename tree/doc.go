// Package tree implements a deterministic, content-addressed Merkle tree
// on top of storage.Engine.
//
// Blobs are stored separately from metadata. Each blob lives in the
// engine's BlobStore under an immutable unique object key, and a small
// Manifest in the metadata KV records that object key, logical key, SHA-256,
// and size. PutBlob streams content, verifies the caller-supplied expected
// hash, and writes the manifest atomically after the blob lands.
//
// Tree Nodes are deterministic: children and blob references are sorted
// before hashing, so a node's ID is a pure function of its content and
// identical trees always produce identical IDs. Nodes form a content
// addressed DAG (the "graph"): leaf nodes reference blob hashes and
// internal nodes reference child node IDs.
//
// Refs are named pointers to root node IDs, updated with compare-and-swap
// on a monotonically increasing version inside a serializable transaction.
// Leases carry fencing tokens. CompareAndSwapRefFenced and SweepGCFenced
// validate the token inside the same serializable metadata transaction as
// the protected mutation. CheckFence alone is advisory and must not guard a
// later write across a process-ownership boundary.
//
// Garbage collection is reachability based: PlanGC walks the node graph
// from every ref and returns the unreachable nodes and blobs, and SweepGC
// deletes them.
package tree
