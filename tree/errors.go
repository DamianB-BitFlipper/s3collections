package tree

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound          = errors.New("tree: not found")
	ErrAlreadyExists     = errors.New("tree: already exists")
	ErrConflict          = errors.New("tree: conflict")
	ErrCorrupt           = errors.New("tree: corrupt object")
	ErrHashMismatch      = errors.New("tree: hash mismatch")
	ErrInvalidID         = errors.New("tree: invalid content id")
	ErrInvalidRange      = errors.New("tree: invalid blob range")
	ErrBlobTooSmall      = errors.New("tree: blob below minimum size")
	ErrBlobTooLarge      = errors.New("tree: blob exceeds maximum size")
	ErrBackendCapability = errors.New("tree: backend capability unavailable")
	ErrInvalidName       = errors.New("tree: invalid name")
	ErrInvalidRefName    = errors.New("tree: invalid ref name")
	ErrLeaseLost         = errors.New("tree: lease fence lost")
	ErrInvalidLease      = errors.New("tree: invalid lease")
	ErrCycle             = errors.New("tree: parent cycle")
	ErrMaxDepth          = errors.New("tree: maximum lineage depth exceeded")
	ErrNoCommonAncestor  = errors.New("tree: no common ancestor")
	ErrPlanNotReady      = errors.New("tree: GC plan grace period has not elapsed")
	ErrInvalidGCPlan     = errors.New("tree: invalid GC plan")
)

// Compatibility aliases make family-specific errors match ErrNotFound and
// size errors with errors.Is while retaining one classification policy.
var (
	ErrBlobNotFound = ErrNotFound
	ErrNodeNotFound = ErrNotFound
	ErrRefNotFound  = ErrNotFound
	ErrTooSmall     = ErrBlobTooSmall
	ErrTooLarge     = ErrBlobTooLarge
)

type CorruptError struct{ Key, Reason string }

func (e *CorruptError) Error() string {
	return fmt.Sprintf("tree: corrupt object %q: %s", e.Key, e.Reason)
}
func (e *CorruptError) Is(target error) bool { return target == ErrCorrupt }
