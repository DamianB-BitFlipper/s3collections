package lru

import "errors"

// Sentinel errors returned by Store methods.
var (
	// ErrNotFound is returned when a key does not exist or is tombstoned.
	ErrNotFound = errors.New("lru: not found")
	// ErrClosed is returned when a method is called on a closed Store.
	ErrClosed = errors.New("lru: store closed")
)
