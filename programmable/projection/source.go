package projection

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidSubscription reports an incomplete resume or snapshot request.
	ErrInvalidSubscription = errors.New("projection: invalid subscription")
	// ErrInvalidFrame reports an out-of-order or malformed synchronization frame.
	ErrInvalidFrame = errors.New("projection: invalid frame")
)

// SubscribeRequest opens one projection stream from a durable checkpoint or
// explicitly requests a new full snapshot.
type SubscribeRequest struct {
	Schema        Schema
	After         Cursor
	ForceSnapshot bool
}

// Validate checks schema identity and mutually exclusive snapshot/resume state.
func (r SubscribeRequest) Validate() error {
	if err := r.Schema.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSubscription, err)
	}
	if r.ForceSnapshot {
		if r.After != "" {
			return fmt.Errorf("%w: forced snapshot has a resume cursor", ErrInvalidSubscription)
		}
		return nil
	}
	if err := r.After.Validate(); err != nil {
		return fmt.Errorf("%w: resume cursor: %w", ErrInvalidSubscription, err)
	}
	return nil
}

// Source opens transport-neutral projection synchronization sessions.
type Source interface {
	Open(context.Context, SubscribeRequest) (Session, error)
}

// Session yields ordered synchronization frames until it disconnects.
//
// Close must unblock a concurrent Next.
type Session interface {
	Next(context.Context) (Frame, error)
	Close() error
}

// Frame is one immutable synchronization state-machine input.
type Frame interface {
	projectionFrame()
	cloneFrame() Frame
}

// SnapshotBeginFrame starts an isolated replacement generation.
type SnapshotBeginFrame struct {
	Schema Schema
}

func (SnapshotBeginFrame) projectionFrame() {}

func (frame SnapshotBeginFrame) cloneFrame() Frame {
	return frame
}

// SnapshotChunkFrame adds bounded mutations to the staged generation.
type SnapshotChunkFrame struct {
	Mutations []Mutation
}

func (SnapshotChunkFrame) projectionFrame() {}

func (frame SnapshotChunkFrame) cloneFrame() Frame {
	result := SnapshotChunkFrame{
		Mutations: make([]Mutation, len(frame.Mutations)),
	}
	for index, mutation := range frame.Mutations {
		result.Mutations[index] = mutation.Clone()
	}
	return result
}

// SnapshotEndFrame atomically publishes the staged generation.
type SnapshotEndFrame struct {
	Cursor     Cursor
	SourceTime time.Time
}

func (SnapshotEndFrame) projectionFrame() {}

func (frame SnapshotEndFrame) cloneFrame() Frame {
	return frame
}

// DeltaFrame atomically advances one visible generation.
type DeltaFrame struct {
	Batch DeltaBatch
}

func (DeltaFrame) projectionFrame() {}

func (frame DeltaFrame) cloneFrame() Frame {
	return DeltaFrame{Batch: frame.Batch.Clone()}
}

// HeartbeatFrame reports the source head without mutating replica rows.
type HeartbeatFrame struct {
	Cursor     Cursor
	SourceTime time.Time
}

func (HeartbeatFrame) projectionFrame() {}

func (frame HeartbeatFrame) cloneFrame() Frame {
	return frame
}

// GapFrame reports that Requested is older than the retained replay floor.
type GapFrame struct {
	Requested Cursor
	Floor     Cursor
}

func (GapFrame) projectionFrame() {}

func (frame GapFrame) cloneFrame() Frame {
	return frame
}
