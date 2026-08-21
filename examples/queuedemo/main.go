// queuedemo shows a queue whose metadata and payload bodies use separate stores.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/damianb/s3collections/queue"
	"github.com/damianb/s3collections/storage"
)

func main() {
	ctx := context.Background()
	kv := storage.NewMemoryKV()
	blobs := storage.NewMemoryBlobStore()
	defer kv.Close()
	defer blobs.Close()

	q := queue.New(kv, blobs, "demo", queue.WithLeaseDuration(30*time.Second))
	id, created, err := q.Enqueue(ctx, []byte(`{"task":"demo"}`), queue.EnqueueOptions{JobID: "demo-1"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("job=%s created=%v\n", id, created)

	job, err := q.Claim(ctx, queue.ClaimOptions{})
	if err != nil {
		log.Fatal(err)
	}
	body, err := job.OpenPayload(ctx)
	if err != nil {
		log.Fatal(err)
	}
	payload, err := io.ReadAll(body)
	body.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("claimed=%s attempts=%d payload=%s\n", job.ID(), job.Attempts(), payload)
	if err := job.Complete(ctx); err != nil {
		log.Fatal(err)
	}
}
