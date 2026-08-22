package continuation

import "time"

type commandKind string

const (
	commandSignal commandKind = "signal"
	commandCancel commandKind = "cancel"
)

type commandRecord struct {
	kind   commandKind
	digest [32]byte
}

// Transition is one requested atomic state change.
//
// Frames are unsequenced until Apply accepts the transition. A zero Fence
// retains the current fence; executor-driven transitions should supply the
// fence they own.
type Transition struct {
	status    Status
	fence     uint64
	frames    []Frame
	readyAt   time.Time
	timer     bool
	workflow  *workflowSnapshotState
	commandID string
	command   commandKind
}

// ReadyAt returns a timer transition's normalized absolute wake time.
func (transition Transition) ReadyAt() time.Time { return transition.readyAt }

// Move constructs an executor-driven transition.
func Move(status Status, fence uint64, frames ...Frame) Transition {
	return Transition{
		status: status,
		fence:  fence,
		frames: cloneFrames(frames),
	}
}

// Continue keeps the call Running and atomically appends frames.
func Continue(frames ...Frame) Transition {
	return Move(StatusRunning, 0, frames...)
}

// Signal constructs an idempotent waiting-to-accepted transition.
func Signal(commandID string, frames ...Frame) Transition {
	return Transition{
		status:    StatusAccepted,
		frames:    cloneFrames(frames),
		commandID: commandID,
		command:   commandSignal,
	}
}

// Cancel constructs an idempotent cooperative cancellation request.
func Cancel(commandID string, frames ...Frame) Transition {
	return Transition{
		status:    StatusCancelRequested,
		frames:    cloneFrames(frames),
		commandID: commandID,
		command:   commandCancel,
	}
}

func cloneFrames(frames []Frame) []Frame {
	result := make([]Frame, len(frames))
	for index, frame := range frames {
		result[index] = cloneFrame(frame)
	}
	return result
}
