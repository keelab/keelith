// Package continuation defines the durable state model for continuable calls.
package continuation

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxIdentityBytes = 512
	maxPayloadBytes  = 1024 * 1024
)

var (
	// ErrInvalidIdentity reports a malformed call, operation, or command ID.
	ErrInvalidIdentity = errors.New("continuation: invalid identity")
	// ErrInvalidFrame reports a malformed or oversized durable frame.
	ErrInvalidFrame = errors.New("continuation: invalid frame")
	// ErrTransition reports a transition that is not in the state machine.
	ErrTransition = errors.New("continuation: invalid state transition")
	// ErrTerminal reports an attempted mutation of a terminal call.
	ErrTerminal = errors.New("continuation: terminal snapshot")
	// ErrRevision reports an exhausted or inconsistent snapshot revision.
	ErrRevision = errors.New("continuation: invalid revision")
	// ErrFence reports a stale or missing execution fence.
	ErrFence = errors.New("continuation: stale fence")
	// ErrSequence reports an exhausted or non-monotonic frame sequence.
	ErrSequence = errors.New("continuation: invalid frame sequence")
	// ErrCommandConflict reports one command ID reused for another command kind.
	ErrCommandConflict = errors.New("continuation: command identity conflict")
)

// StateError adds bounded state-machine context to a stable error category.
type StateError struct {
	Kind error
	From Status
	To   Status
}

// Error implements error without exposing payloads or command identities.
func (err *StateError) Error() string {
	if err == nil {
		return ""
	}
	if err.From == "" && err.To == "" {
		return err.Kind.Error()
	}
	return fmt.Sprintf("%v: %s -> %s", err.Kind, err.From, err.To)
}

// Unwrap exposes the stable error category.
func (err *StateError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Kind
}

// CallID is an immutable durable call identity.
type CallID struct {
	value string
}

// NewCallID validates and constructs a CallID.
func NewCallID(value string) (CallID, error) {
	if !validIdentity(value) {
		return CallID{}, fmt.Errorf("%w: call id", ErrInvalidIdentity)
	}
	return CallID{value: value}, nil
}

// String returns the stable call identity.
func (id CallID) String() string { return id.value }

// Operation is an immutable transport-neutral /service/method identity.
type Operation struct {
	value string
}

// NewOperation validates and constructs an Operation.
func NewOperation(value string) (Operation, error) {
	if !validOperation(value) {
		return Operation{}, fmt.Errorf("%w: operation", ErrInvalidIdentity)
	}
	return Operation{value: value}, nil
}

// String returns the stable operation identity.
func (operation Operation) String() string { return operation.value }

// Status is the durable lifecycle state of one continuable call.
type Status string

const (
	// StatusAccepted is durable work waiting for an executor.
	StatusAccepted Status = "accepted"
	// StatusRunning is work currently owned by a fenced executor.
	StatusRunning Status = "running"
	// StatusWaiting is work blocked on an external Signal.
	StatusWaiting Status = "waiting"
	// StatusSuspended is running work released for later acquisition.
	StatusSuspended Status = "suspended"
	// StatusCancelRequested asks the executor to cancel cooperatively.
	StatusCancelRequested Status = "cancel_requested"
	// StatusCompleted is a successful immutable terminal state.
	StatusCompleted Status = "completed"
	// StatusFailed is an unsuccessful immutable terminal state.
	StatusFailed Status = "failed"
	// StatusCanceled is a cooperatively canceled immutable terminal state.
	StatusCanceled Status = "canceled"
	// StatusExpired is a retention-expired immutable terminal state.
	StatusExpired Status = "expired"
)

// Terminal reports whether a Status permits no further state change.
func (status Status) Terminal() bool {
	switch status {
	case StatusCompleted, StatusFailed, StatusCanceled, StatusExpired:
		return true
	default:
		return false
	}
}

func (status Status) valid() bool {
	switch status {
	case StatusAccepted,
		StatusRunning,
		StatusWaiting,
		StatusSuspended,
		StatusCancelRequested,
		StatusCompleted,
		StatusFailed,
		StatusCanceled,
		StatusExpired:
		return true
	default:
		return false
	}
}

// FrameKind identifies one durable protocol observation.
type FrameKind string

const (
	// FrameAccepted records durable call initialization.
	FrameAccepted FrameKind = "accepted"
	// FrameEvent records one application-defined progress event.
	FrameEvent FrameKind = "event"
	// FrameWaiting records that execution needs an external Signal.
	FrameWaiting FrameKind = "waiting"
	// FrameSignal records one accepted idempotent Signal.
	FrameSignal FrameKind = "signal"
	// FrameSuspended records release by an executor for later acquisition.
	FrameSuspended FrameKind = "suspended"
	// FrameCancelRequested records one accepted cancellation command.
	FrameCancelRequested FrameKind = "cancel_requested"
	// FrameCompleted records successful terminal output.
	FrameCompleted FrameKind = "completed"
	// FrameFailed records terminal failure details.
	FrameFailed FrameKind = "failed"
	// FrameCanceled records cooperative terminal cancellation.
	FrameCanceled FrameKind = "canceled"
	// FrameExpired records terminal retention expiry.
	FrameExpired FrameKind = "expired"
	// FrameWorkflowChild records one payload-bounded durable child terminal.
	FrameWorkflowChild FrameKind = "workflow_child"
)

// Frame is one immutable, monotonically sequenced durable observation.
type Frame struct {
	sequence uint64
	kind     FrameKind
	payload  []byte
}

// NewFrame validates and snapshots an unsequenced frame.
func NewFrame(kind FrameKind, payload []byte) (Frame, error) {
	if !kind.valid() || len(payload) > maxPayloadBytes {
		return Frame{}, ErrInvalidFrame
	}
	return Frame{
		kind:    kind,
		payload: append([]byte(nil), payload...),
	}, nil
}

// Sequence returns the durable one-based frame cursor.
func (frame Frame) Sequence() uint64 { return frame.sequence }

// Kind returns the stable frame kind.
func (frame Frame) Kind() FrameKind { return frame.kind }

// Payload returns an independent payload copy.
func (frame Frame) Payload() []byte {
	return append([]byte(nil), frame.payload...)
}

func (kind FrameKind) valid() bool {
	switch kind {
	case FrameAccepted,
		FrameEvent,
		FrameWaiting,
		FrameSignal,
		FrameSuspended,
		FrameCancelRequested,
		FrameCompleted,
		FrameFailed,
		FrameCanceled,
		FrameExpired,
		FrameWorkflowChild:
		return true
	default:
		return false
	}
}

// Snapshot is an immutable complete state-machine snapshot.
type Snapshot struct {
	callID     CallID
	operation  Operation
	status     Status
	readyAt    time.Time
	revision   uint64
	fence      uint64
	sequence   uint64
	frameFloor uint64
	frames     []Frame
	commands   map[string]commandRecord
	workflow   *workflowSnapshotState
}

// NewSnapshot creates one accepted call at revision one.
func NewSnapshot(callID CallID, operation Operation) (Snapshot, error) {
	if !validIdentity(callID.value) || !validOperation(operation.value) {
		return Snapshot{}, fmt.Errorf(
			"%w: call ID or operation",
			ErrInvalidIdentity,
		)
	}
	return Snapshot{
		callID:     callID,
		operation:  operation,
		status:     StatusAccepted,
		revision:   1,
		frameFloor: 1,
		commands:   make(map[string]commandRecord),
	}, nil
}

// CallID returns the immutable call identity.
func (snapshot Snapshot) CallID() CallID { return snapshot.callID }

// Operation returns the immutable logical operation identity.
func (snapshot Snapshot) Operation() Operation { return snapshot.operation }

// Status returns the current durable state.
func (snapshot Snapshot) Status() Status { return snapshot.status }

// ReadyAt returns the absolute store-authoritative time at which a suspended
// timer becomes claimable. A zero value denotes an immediately ready legacy
// suspension or a non-timer state.
func (snapshot Snapshot) ReadyAt() time.Time { return snapshot.readyAt }

// TimerDue reports whether a timer is due at the supplied authoritative time.
// Non-timer snapshots are always due.
func (snapshot Snapshot) TimerDue(now time.Time) bool {
	return snapshot.readyAt.IsZero() || !now.UTC().Before(snapshot.readyAt)
}

// Revision returns the positive state revision.
func (snapshot Snapshot) Revision() uint64 { return snapshot.revision }

// Fence returns the greatest accepted execution generation.
func (snapshot Snapshot) Fence() uint64 { return snapshot.fence }

// Sequence returns the last assigned durable frame sequence.
func (snapshot Snapshot) Sequence() uint64 { return snapshot.sequence }

// FrameFloor returns the first retained frame sequence. It equals Sequence+1
// when every frame has been pruned.
func (snapshot Snapshot) FrameFloor() uint64 { return snapshot.frameFloor }

// Frames returns independent copies of all durable frames.
func (snapshot Snapshot) Frames() []Frame {
	result := make([]Frame, len(snapshot.frames))
	for index, frame := range snapshot.frames {
		result[index] = cloneFrame(frame)
	}
	return result
}

func validIdentity(value string) bool {
	if value == "" ||
		len(value) > maxIdentityBytes ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validOperation(value string) bool {
	if !validIdentity(value) || !strings.HasPrefix(value, "/") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(value, "/"), "/")
	return len(parts) == 2 &&
		validIdentity(parts[0]) &&
		validIdentity(parts[1])
}

func cloneFrame(frame Frame) Frame {
	return Frame{
		sequence: frame.sequence,
		kind:     frame.kind,
		payload:  append([]byte(nil), frame.payload...),
	}
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	result := snapshot
	result.frames = snapshot.Frames()
	result.commands = make(map[string]commandRecord, len(snapshot.commands))
	for id, record := range snapshot.commands {
		result.commands[id] = record
	}
	result.workflow = cloneWorkflowState(snapshot.workflow)
	return result
}
