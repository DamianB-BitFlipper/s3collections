// Package lru implements a distributed LRU metadata store backed by S3.
//
// The Store shards entries across a configurable number of keys, tracks live
// bytes/items, and runs background CLOCK eviction workers. It is built on
// package cas and therefore inherits strong read-after-write and conditional-
// write guarantees without relying on any centralized hot key.
//
// Set resurrects tombstoned entries using cas.Update(..., cas.WithResurrect()).
// Physical tombstone deletion is controlled by Options.TombstoneMinAge; the
// default is 24 hours and should be chosen much larger than the expected S3
// round-trip time to avoid losing freshly resurrected entries.
package lru
