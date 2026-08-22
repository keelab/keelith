package projection

import (
	"context"
	"time"
)

// SnapshotTxn stages a complete projection generation.
//
// Staged mutations are invisible until Commit atomically replaces the visible
// generation. Abort discards all staged state.
type SnapshotTxn interface {
	Stage(Mutation) error
	Commit(context.Context, Cursor, time.Time) error
	Abort() error
}

// Store atomically persists complete snapshots, ordered deltas, and their
// checkpoints.
type Store interface {
	BeginSnapshot(context.Context, Schema) (SnapshotTxn, error)
	ApplyDelta(context.Context, DeltaBatch) error
	Get(context.Context, ProjectionID, []byte) ([]byte, bool, error)
	Checkpoint(context.Context, ProjectionID) (Checkpoint, bool, error)
}
