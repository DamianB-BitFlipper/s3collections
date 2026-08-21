//go:build !slatedb

package storage

import (
	"errors"
	"testing"
)

func TestSlateDBStubUnavailable(t *testing.T) {
	_, err := OpenSlateDB(SlateDBConfig{Path: "x", ObjectStoreURL: "memory:///"})
	if !errors.Is(err, ErrSlateDBUnavailable) {
		t.Fatalf("want ErrSlateDBUnavailable, got %v", err)
	}
}
