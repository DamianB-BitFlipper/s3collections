// Package lru implements a deterministic least-recently-used metadata
// store on top of storage.KV.
//
// Entries carry a small JSON-encoded metadata record (size, creation
// time, last-access time) and a monotonically increasing revision that is
// bumped transactionally on every mutation. Keys are hashed into a fixed
// number of shards under a configurable prefix so scans stay bounded and
// eviction order is deterministic: the entry with the oldest
// LastAccessAt (ties broken by key) is evicted first.
//
// Capacity (bytes and/or item count) is soft: writes are never rejected,
// and a background evictor trims the store back under the configured
// limits. Close stops only the store's own goroutines; it never closes
// the underlying KV.
package lru
