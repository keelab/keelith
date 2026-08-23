package continuation

import (
	"context"
	"errors"
	"fmt"
)

const maxAttachFrames = 1000

var (
	// ErrInvalidAttach reports an invalid runtime, identity, cursor, or limit.
	ErrInvalidAttach = errors.New("continuation: invalid attach request")
	// ErrCursorAhead reports an Attach cursor beyond the durable sequence.
	ErrCursorAhead = errors.New("continuation: attach cursor is ahead")
	// ErrGap reports an Attach cursor below the retained frame floor.
	ErrGap = errors.New("continuation: attach cursor is below retention floor")
)

// CursorError reports an Attach cursor beyond the current durable sequence.
type CursorError struct {
	After   uint64
	Current uint64
}

// Error implements error without exposing call identities or frame payloads.
func (err *CursorError) Error() string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf(
		"%v: after %d, current %d",
		ErrCursorAhead,
		err.After,
		err.Current,
	)
}

// Unwrap exposes ErrCursorAhead.
func (err *CursorError) Unwrap() error {
	if err == nil {
		return nil
	}
	return ErrCursorAhead
}

// GapError reports an Attach cursor whose next frame has been pruned.
type GapError struct {
	After uint64
	Floor uint64
}

// Error implements error without exposing call identities or frame payloads.
func (err *GapError) Error() string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf(
		"%v: after %d, floor %d",
		ErrGap,
		err.After,
		err.Floor,
	)
}

// Unwrap exposes ErrGap.
func (err *GapError) Unwrap() error {
	if err == nil {
		return nil
	}
	return ErrGap
}

// Attachment is one bounded view of a durable call.
//
// Frames contains observations whose sequence is greater than the requested
// after cursor. NextSequence is the first sequence not returned; request the
// next page with after set to NextSequence-1.
type Attachment struct {
	Snapshot     Snapshot
	Frames       []Frame
	Terminal     bool
	NextSequence uint64
	FrameFloor   uint64
}

// StartCall persists one new call together with its immutable input frame.
func (r *Runtime) StartCall(
	ctx context.Context,
	callID CallID,
	operation Operation,
	input []byte,
) (Snapshot, error) {
	if r == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	if _, exists := r.registry.Resolve(operation); !exists {
		return Snapshot{}, ErrMachineNotFound
	}
	snapshot, err := NewSnapshotWithInput(callID, operation, input)
	if err != nil {
		return Snapshot{}, err
	}
	return r.store.Create(ctx, snapshot)
}

// Attach returns one bounded, deep-copied page after an exclusive frame cursor.
func (r *Runtime) Attach(
	ctx context.Context,
	callID CallID,
	after uint64,
	limit int,
) (Attachment, error) {
	if r == nil ||
		ctx == nil ||
		!validIdentity(callID.value) ||
		limit < 1 ||
		limit > maxAttachFrames {
		return Attachment{}, ErrInvalidAttach
	}
	current, err := r.store.Load(ctx, callID)
	if err != nil {
		return Attachment{}, err
	}
	if err := validateSnapshot(current); err != nil {
		return Attachment{}, ErrInvalidAttach
	}
	if current.sequence > after {
		r.observe(ctx, Event{
			Kind:   EventAttachLag,
			Status: current.status,
			Count:  current.sequence - after,
		})
	}
	if after > current.sequence {
		return Attachment{}, &CursorError{
			After:   after,
			Current: current.sequence,
		}
	}
	if after < current.frameFloor-1 {
		r.observe(ctx, Event{
			Kind:  EventGap,
			Count: current.frameFloor - after - 1,
		})
		return Attachment{}, &GapError{
			After: after,
			Floor: current.frameFloor,
		}
	}

	allFrames := current.Frames()
	start := int(after - (current.frameFloor - 1))
	end := len(allFrames)
	if remaining := len(allFrames) - start; remaining > limit {
		end = start + limit
	}
	frames := cloneFrames(allFrames[start:end])
	nextSequence := after + 1
	if len(frames) > 0 {
		nextSequence = frames[len(frames)-1].Sequence() + 1
	}
	return Attachment{
		Snapshot:     cloneSnapshot(current),
		Frames:       frames,
		Terminal:     current.status.Terminal(),
		NextSequence: nextSequence,
		FrameFloor:   current.frameFloor,
	}, nil
}
