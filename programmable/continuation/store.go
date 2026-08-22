package continuation

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidStore reports an incomplete store request or implementation.
	ErrInvalidStore = errors.New("continuation: invalid store request")
	// ErrNotFound reports a missing durable call.
	ErrNotFound = errors.New("continuation: call not found")
	// ErrAlreadyExists reports reuse of one CallID.
	ErrAlreadyExists = errors.New("continuation: call already exists")
	// ErrConflict reports a stale expected revision.
	ErrConflict = errors.New("continuation: revision conflict")
	// ErrStaleFence reports a commit from an executor that no longer owns a call.
	ErrStaleFence = errors.New("continuation: stale executor fence")
	// ErrNotReady reports acquisition of a waiting or terminal call.
	ErrNotReady = errors.New("continuation: call is not ready")
)

// CommitRequest atomically replaces one expected, fence-owned revision.
//
// Snapshot must be the direct immutable result of Apply on the currently
// loaded revision.
type CommitRequest struct {
	ExpectedRevision uint64
	Fence            uint64
	LeaseOwner       string
	ExpiresAt        time.Time
	Snapshot         Snapshot
}

// CommandRequest is one idempotent external Signal or Cancel command.
type CommandRequest struct {
	CallID           CallID
	ExpectedRevision uint64
	CommandID        string
	Payload          []byte
}

// Store persists continuable call snapshots.
//
// Acquire atomically allocates a strictly newer fence. Transition rejects any
// owner other than the current fence. SubmitSignal and RequestCancel dedupe
// command IDs before revision comparison so a lost response can be retried
// with its original expected revision.
type Store interface {
	Create(context.Context, Snapshot) (Snapshot, error)
	Load(context.Context, CallID) (Snapshot, error)
	Acquire(context.Context, CallID, uint64) (Snapshot, error)
	Transition(context.Context, CommitRequest) (Snapshot, error)
	ListReady(context.Context, int) ([]Snapshot, error)
	SubmitSignal(context.Context, CommandRequest) (Snapshot, error)
	RequestCancel(context.Context, CommandRequest) (Snapshot, error)
}
