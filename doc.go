// Package s3collections documents the shared model used by the collection
// packages. Transactional metadata is accessed through storage.KV, normally
// supplied by a storage.Engine. Large queue and tree bodies are streamed
// through a separate storage.BlobStore owned by that engine.
//
// The native engine uses the official SlateDB Go v0.15 binding and stores its
// database on S3. A SlateDB database path has one writer process at a time;
// fencing is a safety mechanism, not support for concurrent writers. Body
// upload and metadata publication are separate operations, so deployments
// must garbage-collect uploads that were never published after a grace period.
package s3collections
