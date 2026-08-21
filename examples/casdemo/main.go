// casdemo demonstrates the versioned CAS store: create a record,
// compare-and-swap it, handle a conflict retry, delete it, and run GC.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/storage"
)

func main() {
	ctx := context.Background()
	kv := storage.NewMemoryKV()
	defer kv.Close()
	store := cas.New(kv, "demo/", cas.WithTombstoneRetention(5*time.Minute))

	// Create a live record.
	rec, err := store.Create(ctx, "config", []byte("v1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("created config at revision %d\n", rec.Revision)

	// Compare-and-swap to v2.
	rec, err = store.CompareAndSwap(ctx, "config", rec.Revision, []byte("v2"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("swapped config to revision %d\n", rec.Revision)

	// Simulate a concurrent writer using a stale revision.
	_, err = store.CompareAndSwap(ctx, "config", rec.Revision-1, []byte("v2-stale"))
	if !errors.Is(err, cas.ErrRevisionMismatch) {
		log.Fatalf("expected conflict, got %v", err)
	}
	fmt.Println("detected stale revision conflict as expected")

	// Read current value.
	rec, err = store.Get(ctx, "config")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("current value: %s at revision %d\n", rec.Value, rec.Revision)

	// Delete the record (tombstone).
	rec, err = store.Delete(ctx, "config", rec.Revision)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("deleted config at revision %d\n", rec.Revision)

	// Get now returns ErrNotFound.
	_, err = store.Get(ctx, "config")
	if !errors.Is(err, cas.ErrNotFound) {
		log.Fatalf("expected not found, got %v", err)
	}
	fmt.Println("get after delete returns ErrNotFound as expected")

	// GC any eligible tombstones (none yet because retention has not passed).
	n, err := store.GC(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("gc deleted %d tombstone(s)\n", n)
}
