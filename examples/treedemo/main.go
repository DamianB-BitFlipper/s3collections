// treedemo demonstrates content-addressed blobs, immutable nodes, named refs,
// lineage traversal, and leases using the in-memory backend.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/damianb/s3collections/s3backend"
	"github.com/damianb/s3collections/tree"
)

func main() {
	ctx := context.Background()
	store, err := tree.New(s3backend.NewMemory(), "demo")
	if err != nil {
		log.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 40<<10)
	sum := sha256.Sum256(payload)
	ref, err := store.PutBlob(ctx, tree.BlobID(hex.EncodeToString(sum[:])), bytes.NewReader(payload), tree.WithExpectedBlobSize(int64(len(payload))))
	if err != nil {
		log.Fatal(err)
	}
	root, err := store.CommitRoot(ctx, []tree.BlobRef{ref}, []byte("opaque root metadata"))
	if err != nil {
		log.Fatal(err)
	}
	head, err := store.CreateRef(ctx, "branches/main", root)
	if err != nil {
		log.Fatal(err)
	}
	child, err := store.CommitChild(ctx, root, nil, []byte("opaque child metadata"))
	if err != nil {
		log.Fatal(err)
	}
	head, err = store.CompareAndSwapRef(ctx, head.Name, head.Revision, child)
	if err != nil {
		log.Fatal(err)
	}
	line, err := store.ResolveLineage(ctx, head.NodeID, nil)
	if err != nil {
		log.Fatal(err)
	}
	lease, err := store.AcquireLease(ctx, head.NodeID, "demo-reader", time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	defer store.ReleaseLease(ctx, lease)
	fmt.Printf("head=%s revision=%d lineage=%d\n", head.NodeID, head.Revision, len(line))
}
