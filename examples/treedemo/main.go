// treedemo shows content-addressed bodies and transactional tree metadata.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/damianb/s3collections/storage"
	"github.com/damianb/s3collections/tree"
)

func main() {
	ctx := context.Background()
	eng, err := storage.NewEngine(storage.NewMemoryKV(), storage.NewMemoryBlobStore())
	if err != nil {
		log.Fatal(err)
	}
	defer eng.Close()

	store := tree.New(eng)
	payload := []byte("release artifact")
	sum := sha256.Sum256(payload)
	manifest, err := store.PutBlob(ctx, hex.EncodeToString(sum[:]), bytes.NewReader(payload), tree.PutOptions{Key: "artifact", Size: int64(len(payload))})
	if err != nil {
		log.Fatal(err)
	}

	node := tree.Node{Name: "release", Leaf: true, Blobs: []string{manifest.Hash}}
	nodeID, err := store.PutNode(ctx, node)
	if err != nil {
		log.Fatal(err)
	}
	head, err := store.CompareAndSwapRef(ctx, "main", nodeID, 0)
	if err != nil {
		log.Fatal(err)
	}
	lease, err := store.AcquireLease(ctx, "publisher")
	if err != nil {
		log.Fatal(err)
	}
	if err := store.CheckFence(ctx, lease.Name, lease.Token); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("head=%s version=%d blob=%s\n", head.NodeID, head.Version, manifest.Hash)
}
