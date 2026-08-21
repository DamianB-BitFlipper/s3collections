# Native SlateDB build

The SlateDB engine uses the official `slatedb.io/slatedb-go` v0.15 binding.
That binding calls a Rust library through cgo. It is intentionally kept behind
the `slatedb` Go build tag so ordinary builds and tests stay portable.

A native build needs:

1. a C toolchain and `CGO_ENABLED=1`;
2. Rust and Cargo;
3. the SlateDB native library from the **v0.15** source release, built as a
   release library for the target platform;
4. its library directory in the platform linker search path; and
5. `go test -tags slatedb ./...` (or `go build -tags slatedb`).

Do not mix the Go v0.15 module with a native library from another release.
The native CI job in `.github/workflows/ci.yml` is the executable build recipe.
Pinning both sides avoids an ABI mismatch.

## Runtime ownership and fencing

Treat one database path as a single-writer resource. Only one process may open
it for writes at a time. A second writer can fence the first. The first must
then stop serving writes; retry loops must not hide a fencing error. Use
process-level leader election or exclusive workload placement before opening
the database. Planned failover should stop/close the old engine before opening
the new writer.

An object-store URL names only a bucket, such as `s3://company-state`. Supply
`service-a/metadata` as the distinct database path. This separation matters:
SlateDB owns its layout below that path, while the BlobStore owns queue and
tree body keys.
