package cas

import (
	"errors"
	"fmt"
	"time"
)

// Sentinel errors returned by Store methods.
var (
	// ErrAlreadyExists is returned by Create when an object (live or tombstone) already exists.
	ErrAlreadyExists = errors.New("cas: already exists")
	// ErrNotFound is returned by Get/GetMeta when the key does not exist.
	ErrNotFound = errors.New("cas: not found")
	// ErrDeleted is returned when an operation targets a tombstoned key and resurrection is not allowed.
	ErrDeleted = errors.New("cas: key tombstoned")
	// ErrConflict is returned when a conditional write fails due to a stale revision.
	ErrConflict = errors.New("cas: conflict (stale revision)")
	// ErrTooLarge is returned when a value exceeds MaxValueBytes.
	ErrTooLarge = errors.New("cas: value exceeds max size")
	// ErrCorrupt is returned when an envelope fails schema validation or its checksum/checksum mismatches.
	ErrCorrupt = errors.New("cas: envelope/value checksum mismatch")
)

// NotFoundError carries additional information for tombstoned keys.
// Use errors.As(err, new(NotFoundError)) to inspect.
type NotFoundError struct {
	Key        string
	Tombstoned bool
	Revision   uint64
	DeletedAt  time.Time
}

func (e *NotFoundError) Error() string {
	if e.Tombstoned {
		return fmt.Sprintf("cas: key %q not found (tombstoned at rev %d)", e.Key, e.Revision)
	}
	return fmt.Sprintf("cas: key %q not found", e.Key)
}

// Is reports that NotFoundError matches ErrNotFound for errors.Is.
func (e *NotFoundError) Is(target error) bool {
	return target == ErrNotFound
}

// IsConflict reports whether err is a CAS conflict (errors.Is(err, ErrConflict)).
func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict)
}
