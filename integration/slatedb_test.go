//go:build slatedb

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/damianb/s3collections/cas"
	"github.com/damianb/s3collections/lru"
	"github.com/damianb/s3collections/queue"
	"github.com/damianb/s3collections/storage"
	"github.com/damianb/s3collections/tree"
	"strings"
	"testing"
)

func TestCollectionsOnOfficialSlateDBBinding(t *testing.T) {
	ctx := context.Background()
	kv, err := storage.OpenSlateDB(storage.SlateDBConfig{ObjectStoreURL: "memory:///", Path: "collections-smoke"})
	if err != nil {
		t.Fatal(err)
	}
	blobs := storage.NewMemoryBlobStore()
	eng, err := storage.NewEngine(kv, blobs)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cs := cas.New(kv, "smoke/")
	if _, err := cs.Create(ctx, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	ls, err := lru.New(kv, lru.Options{Prefix: "smoke-lru/"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ls.Set(ctx, "key", lru.EntryMeta{SizeBytes: 5}); err != nil {
		t.Fatal(err)
	}
	q := queue.New(kv, blobs, "smoke")
	if _, _, err := q.Enqueue(ctx, []byte("payload"), queue.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(ctx, queue.ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	tr := tree.New(eng)
	body := "blob"
	sum := sha256.Sum256([]byte(body))
	if _, err := tr.PutBlob(ctx, hex.EncodeToString(sum[:]), strings.NewReader(body), tree.PutOptions{Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
}
