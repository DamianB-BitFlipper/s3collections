// Package s3collections provides distributed data structures backed solely
// by S3: a versioned compare-and-swap store (package cas), a distributed LRU
// metadata store (package lru), and a durable work queue (package queue).
//
// This root package defines the shared plumbing used by every component:
// observability interfaces (Meter, Logger, Tracer), retry policy, and test
// helpers. It has no third-party dependencies; the whole module uses only
// the Go standard library.
//
// Consistency model (see docs/design/00-architecture.md): every structure
// relies only on strong read-after-write GET/LIST, atomic per-key PUT, and
// conditional PUT (If-None-Match / If-Match). Single-key operations are
// linearizable; cross-key invariants are best-effort with background repair.
package s3collections
