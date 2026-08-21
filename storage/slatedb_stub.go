//go:build !slatedb

package storage

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

// NewSlateDBKV is unavailable in binaries built without the slatedb build
// tag; it always returns ErrSlateDBUnavailable. Build with
// "-tags slatedb" to enable the real implementation.
func NewSlateDBKV(cfg SlateDBConfig) (KV, error) {
	return nil, ErrSlateDBUnavailable
}

// OpenSlateDB is the canonical SlateDB KV constructor.
func OpenSlateDB(cfg SlateDBConfig) (KV, error) { return NewSlateDBKV(cfg) }
