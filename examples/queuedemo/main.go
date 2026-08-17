// queuedemo demonstrates the durable work queue: idempotent
// enqueue, two competing workers, lease renewal, retry-to-dead, dead-letter
// requeue, and a maintenance loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/damianb/s3collections/queue"
	"github.com/damianb/s3collections/s3backend"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	be := s3backend.NewMemory()
	q, err := queue.New(be, "demo", func(o *queue.Options) {
		o.WorkerID = "worker-a"
		o.Shards = 4
		o.MaxAttempts = 2
		o.DefaultVisibilityTimeout = 30 * time.Second
		o.ClockSkewTolerance = 5 * time.Second
		o.ReaperInterval = 10 * time.Second
		o.GCInterval = 10 * time.Second
	})
	if err != nil {
		log.Fatal(err)
	}
	q.StartMaintenance(ctx)

	// Idempotent enqueue.
	jobID, existed, err := q.Enqueue(ctx, []byte(`{"task":"demo"}`), queue.EnqueueOptions{
		IdempotencyKey: "demo-task-1",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued job %s existed=%v\n", jobID, existed)

	// A second enqueue with the same idempotency key is a duplicate.
	_, existed, err = q.Enqueue(ctx, []byte(`{"task":"demo"}`), queue.EnqueueOptions{
		IdempotencyKey: "demo-task-1",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("second enqueue existed=%v (dedup-through-retention)\n", existed)

	// Two workers compete to claim the same job.
	var wg sync.WaitGroup
	var winner *queue.Job
	var mu sync.Mutex
	for _, name := range []string{"worker-a", "worker-b"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			j, err := q.Claim(ctx, queue.ClaimOptions{})
			if errors.Is(err, queue.ErrEmpty) {
				fmt.Printf("%s: no job available (already claimed)\n", name)
				return
			}
			if err != nil {
				fmt.Printf("%s: claim error: %v\n", name, err)
				return
			}
			mu.Lock()
			winner = j
			mu.Unlock()
			fmt.Printf("%s: claimed job %s attempts=%d fence=%d\n", name, j.ID, j.Attempts, j.Fence)
		}(name)
	}
	wg.Wait()

	if winner == nil {
		log.Fatal("no worker claimed the job")
	}

	// Renew the lease so the worker keeps the job.
	if err := winner.Renew(ctx, 5*time.Second); err != nil {
		log.Fatal(err)
	}
	fmt.Println("renewed lease")

	// Retry once. Because MaxAttempts is 2, the job moves to dead-lettered.
	if err := winner.Retry(ctx, queue.RetryOptions{Backoff: time.Second, Reason: "demo retry"}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("retried job -> dead (MaxAttempts=2)")

	// List dead jobs.
	dead, _, err := q.ListDead(ctx, queue.ListDeadOptions{Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("dead jobs: %d\n", len(dead))
	for _, d := range dead {
		fmt.Printf("  dead job %s shard=%d attempts=%d reason=%s\n", d.ID, d.Shard, d.Attempts, d.Reason)
	}

	// Requeue the dead job.
	if err := q.RequeueDead(ctx, winner.ID, winner.Shard); err != nil {
		log.Fatal(err)
	}
	fmt.Println("requeued dead job")

	// Claim and complete the requeued job.
	job2, err := q.Claim(ctx, queue.ClaimOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("claimed requeued job %s attempts=%d\n", job2.ID, job2.Attempts)

	if err := job2.Complete(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("completed job")

	// Guard helper: the completed fence should be accepted.
	if err := queue.Guard(job2.Fence, job2.Fence); err != nil {
		log.Fatal(err)
	}
	fmt.Println("fence guard ok")
}
