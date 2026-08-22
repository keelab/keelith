package continuation

import (
	"errors"
	"math"
)

var (
	// ErrInvalidRetention reports a non-monotonic or out-of-range frame floor,
	// expiration, list, or garbage-collection request.
	ErrInvalidRetention = errors.New("continuation: invalid retention request")
)

// PruneSnapshot returns an immutable Snapshot whose frames below floor are
// physically removed while its global sequence, revision, and fence remain
// unchanged. Repeating an older or equal floor is idempotent.
func PruneSnapshot(snapshot Snapshot, floor uint64) (Snapshot, error) {
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, ErrInvalidRetention
	}
	if floor == 0 ||
		snapshot.sequence == math.MaxUint64 && floor > snapshot.sequence ||
		snapshot.sequence < math.MaxUint64 && floor > snapshot.sequence+1 {
		return Snapshot{}, ErrInvalidRetention
	}
	if floor <= snapshot.frameFloor {
		return cloneSnapshot(snapshot), nil
	}
	offset := floor - snapshot.frameFloor
	if offset > uint64(len(snapshot.frames)) {
		return Snapshot{}, ErrInvalidRetention
	}
	pruned := cloneSnapshot(snapshot)
	pruned.frameFloor = floor
	pruned.frames = cloneFrames(pruned.frames[int(offset):])
	if err := validateSnapshot(pruned); err != nil {
		return Snapshot{}, ErrInvalidRetention
	}
	return pruned, nil
}
