// Package tree implements five portable S3-backed primitive families:
// content-addressed immutable blobs, content-addressed immutable node
// manifests, revisioned named references, parent-authoritative topology, and
// fenced leases with reachability garbage collection.
//
// Blob bytes are stored raw and separately from their small metadata
// manifests. Encoding and encryption descriptors are opaque bytes: this
// package neither interprets nor transforms them. Application, VM, and
// snapshot-full semantics deliberately remain outside this package.
//
// All durable coordination uses ordinary S3-compatible operations. The
// package does not use append or write-offset extensions and works with AWS
// general-purpose S3, Cloudflare R2, MinIO, and compatible implementations
// that honor Backend's conditional-write contract. Reverse child edges are
// advisory; only node parent pointers are authoritative.
package tree
