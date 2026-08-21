// lrudemo demonstrates the distributed LRU metadata store: set and
// get entries, use Touch, and run the evictor under a small capacity cap.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/damianb/s3collections/lru"
	"github.com/damianb/s3collections/storage"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kv := storage.NewMemoryKV()
	defer kv.Close()
	store, err := lru.New(kv, lru.Options{
		ShardCount:      4,
		CapacityItems:   1, // tiny cap so the evictor has work to do
		EvictorInterval: 400 * time.Millisecond,
		TouchOnGet:      false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := store.StartEvictor(ctx); err != nil {
		log.Fatal(err)
	}

	now := time.Now()
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if err := store.Set(ctx, key, lru.EntryMeta{
			SizeBytes:    int64(100 + i*10),
			CreatedAt:    now,
			LastAccessAt: now,
		}); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("set 6 keys")

	// Get works before eviction runs.
	ent, err := store.Get(ctx, "key-00")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("got key-00: size=%d rev=%d\n", ent.Meta.SizeBytes, ent.Revision)

	// Touch marks key-00 as recently used. The evictor still removes it
	// eventually because the capacity cap is extremely small in this demo.
	if err := store.Touch(ctx, "key-00"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("touched key-00")

	statsBefore, err := store.Stats(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("before eviction: approxItems=%d\n", statsBefore.Items)

	// Wait for the CLOCK evictor to clear access bits and then evict.
	time.Sleep(1 * time.Second)

	found := 0
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if _, err := store.Get(ctx, key); err == nil {
			found++
		}
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("after eviction: found=%d items=%d\n",
		found, stats.Items)
}
