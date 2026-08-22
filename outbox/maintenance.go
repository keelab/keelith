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
func (request ReplayRequest) Validate(now time.Time) error {
	if now.IsZero() ||
		request.AvailableAt.IsZero() ||
		request.AvailableAt.Before(now) ||
		!validIdentity(request.ExpectedReason, 64) ||
		len(request.IDs) == 0 ||
		len(request.IDs) > maxReplayIDs {
		return fmt.Errorf("%w: replay request is malformed", ErrInvalidOption)
	}
	seen := make(map[string]struct{}, len(request.IDs))
	for _, id := range request.IDs {
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
func (request RetentionRequest) Validate() error {
	if request.PublishedBefore.IsZero() ||
		request.TerminalBefore.IsZero() ||
		request.Limit <= 0 ||
		request.Limit > maxClaimLimit {
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
