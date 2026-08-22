package memory

import (
	"fmt"

	"github.com/keelab/keelith/programmable/continuation"
)

// NewFromSnapshots rebuilds a Store from complete validated durable snapshots.
func NewFromSnapshots(
	snapshots ...continuation.Snapshot,
) (*Store, error) {
	store := New()
	for _, snapshot := range snapshots {
		if err := continuation.ValidateSnapshot(snapshot); err != nil {
			return nil, fmt.Errorf(
				"%w: restored snapshot",
				continuation.ErrInvalidStore,
			)
		}
		key := snapshot.CallID().String()
		if _, exists := store.records[key]; exists {
			return nil, continuation.ErrAlreadyExists
		}
		store.records[key] = snapshot
	}
	return store, nil
}
