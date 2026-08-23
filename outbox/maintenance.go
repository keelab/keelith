package outbox

import (
	"context"
	"fmt"
	"time"
)

const maxReplayIDs = 100

// ReplayRequest atomically moves an exact terminal set back to pending.
type ReplayRequest struct {
	IDs            []string
	ExpectedReason string
	AvailableAt    time.Time
}

// Validate rejects ambiguous, duplicate, stale, or unbounded replay input.
func (r ReplayRequest) Validate(now time.Time) error {
	if now.IsZero() ||
		r.AvailableAt.IsZero() ||
		r.AvailableAt.Before(now) ||
		!validIdentity(r.ExpectedReason, 64) ||
		len(r.IDs) == 0 ||
		len(r.IDs) > maxReplayIDs {
		return fmt.Errorf("%w: replay request is malformed", ErrInvalidOption)
	}
	seen := make(map[string]struct{}, len(r.IDs))
	for _, id := range r.IDs {
		if !validIdentity(id, maxIDBytes) {
			return fmt.Errorf("%w: replay ID is malformed", ErrInvalidOption)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%w: replay ID is duplicated", ErrInvalidOption)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// ReplayResult is a value-free replay summary.
type ReplayResult struct {
	Requested int
	Replayed  int
}

// RetentionRequest bounds published and terminal cleanup.
type RetentionRequest struct {
	PublishedBefore time.Time
	TerminalBefore  time.Time
	Limit           int
}

// Validate rejects unbounded retention cleanup.
func (r RetentionRequest) Validate() error {
	if r.PublishedBefore.IsZero() ||
		r.TerminalBefore.IsZero() ||
		r.Limit <= 0 ||
		r.Limit > maxClaimLimit {
		return fmt.Errorf("%w: retention request is malformed", ErrInvalidOption)
	}
	return nil
}

// Maintenance exposes operator-driven replay and retention outside the
// Dispatcher hot path.
type Maintenance interface {
	Replay(context.Context, ReplayRequest) (ReplayResult, error)
	Purge(context.Context, RetentionRequest) (int64, error)
}

// RetentionDescription is a datastore-, table-, and schedule-free aggregate
// snapshot shared by bounded maintenance implementations.
type RetentionDescription struct {
	Active     int64
	Runs       uint64
	Batches    uint64
	Purged     uint64
	Incomplete uint64
	Failures   uint64
}
