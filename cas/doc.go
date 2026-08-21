// Package cas implements a versioned, compare-and-swap record store on
// top of the storage.KV abstraction.
//
// Records carry a monotonically increasing Revision. Mutations run inside
// storage.KV callback transactions and retry on storage.ErrConflict, so
// CompareAndSwap, Update, and Delete are linearizable per key. Deletes are
// soft: a record becomes a tombstone with State set to StateTombstone, and
// GC reclaims tombstones once the configured retention elapses.
//
// User keys may contain arbitrary bytes; they are encoded with URL-safe
// base64 before being stored under the store prefix.
//
// No-op writes are free: CompareAndSwap and Update do not bump the
// revision when the new value is byte-identical to the stored value.
package cas
