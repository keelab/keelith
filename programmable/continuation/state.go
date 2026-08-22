package continuation

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"math"
	"time"
)

// Apply validates transition and returns a new immutable Snapshot.
func Apply(current Snapshot, transition Transition) (Snapshot, error) {
	if err := validateSnapshot(current); err != nil {
		return Snapshot{}, err
	}
	if !transition.status.valid() {
		return Snapshot{}, stateError(
			ErrTransition,
			current.status,
			transition.status,
		)
	}
	if transition.timer && transition.readyAt.IsZero() ||
		!transition.readyAt.IsZero() && transition.status != StatusSuspended {
		return Snapshot{}, stateError(
			ErrInvalidTimer,
			current.status,
			transition.status,
		)
	}
	if transition.workflow != nil {
		if err := validateWorkflowSuccessor(
			current.workflow,
			transition.workflow,
		); err != nil {
			return Snapshot{}, stateError(
				ErrInvalidWorkflow,
				current.status,
				transition.status,
			)
		}
	}

	if transition.command != "" {
		if !validIdentity(transition.commandID) {
			return Snapshot{}, stateError(
				ErrInvalidIdentity,
				current.status,
				transition.status,
			)
		}
		record := commandRecord{
			kind:   transition.command,
			digest: commandDigest(transition),
		}
		if previous, exists := current.commands[transition.commandID]; exists {
			if previous != record {
				return Snapshot{}, stateError(
					ErrCommandConflict,
					current.status,
					transition.status,
				)
			}
			return cloneSnapshot(current), nil
		}
	}

	if current.status.Terminal() {
		return Snapshot{}, stateError(
			ErrTerminal,
			current.status,
			transition.status,
		)
	}
	if !allowedTransition(current.status, transition.status, transition.command) {
		return Snapshot{}, stateError(
			ErrTransition,
			current.status,
			transition.status,
		)
	}
	if current.revision == math.MaxUint64 {
		return Snapshot{}, stateError(
			ErrRevision,
			current.status,
			transition.status,
		)
	}

	fence := transition.fence
	if fence == 0 {
		fence = current.fence
	}
	if fence < current.fence ||
		(transition.status == StatusRunning && fence == 0) {
		return Snapshot{}, stateError(
			ErrFence,
			current.status,
			transition.status,
		)
	}

	next := cloneSnapshot(current)
	next.status = transition.status
	if transition.status == StatusSuspended {
		next.readyAt = normalizeReadyAt(transition.readyAt)
	} else {
		next.readyAt = time.Time{}
	}
	if transition.workflow != nil {
		next.workflow = cloneWorkflowState(transition.workflow)
	}
	next.revision++
	next.fence = fence
	for _, frame := range transition.frames {
		if frame.sequence != 0 {
			return Snapshot{}, stateError(
				ErrSequence,
				current.status,
				transition.status,
			)
		}
		if !frame.kind.valid() ||
			frame.kind == FrameAccepted ||
			len(frame.payload) > maxPayloadBytes {
			return Snapshot{}, stateError(
				ErrInvalidFrame,
				current.status,
				transition.status,
			)
		}
		if next.sequence == math.MaxUint64 {
			return Snapshot{}, stateError(
				ErrSequence,
				current.status,
				transition.status,
			)
		}
		next.sequence++
		frame.sequence = next.sequence
		next.frames = append(next.frames, cloneFrame(frame))
	}
	if transition.command != "" {
		next.commands[transition.commandID] = commandRecord{
			kind:   transition.command,
			digest: commandDigest(transition),
		}
	}
	return next, nil
}

func allowedTransition(from, to Status, command commandKind) bool {
	switch command {
	case commandSignal:
		return from == StatusWaiting && to == StatusAccepted
	case commandCancel:
		switch from {
		case StatusAccepted, StatusRunning, StatusWaiting, StatusSuspended:
			return to == StatusCancelRequested
		default:
			return false
		}
	case "":
	default:
		return false
	}

	switch from {
	case StatusAccepted:
		return to == StatusRunning ||
			to == StatusFailed ||
			to == StatusExpired
	case StatusRunning:
		return to == StatusRunning ||
			to == StatusWaiting ||
			to == StatusSuspended ||
			to == StatusCompleted ||
			to == StatusFailed ||
			to == StatusExpired
	case StatusWaiting:
		return to == StatusFailed || to == StatusExpired
	case StatusSuspended:
		return to == StatusAccepted ||
			to == StatusRunning ||
			to == StatusFailed ||
			to == StatusExpired
	case StatusCancelRequested:
		return to == StatusCancelRequested ||
			to == StatusCanceled ||
			to == StatusFailed ||
			to == StatusExpired
	default:
		return false
	}
}

func validateSnapshot(snapshot Snapshot) error {
	if !validIdentity(snapshot.callID.value) ||
		!validOperation(snapshot.operation.value) ||
		!snapshot.status.valid() {
		return stateError(
			ErrInvalidIdentity,
			snapshot.status,
			snapshot.status,
		)
	}
	if err := validateWorkflowState(snapshot.workflow); err != nil {
		return stateError(
			ErrInvalidWorkflow,
			snapshot.status,
			snapshot.status,
		)
	}
	if !snapshot.readyAt.IsZero() &&
		(snapshot.status != StatusSuspended ||
			snapshot.readyAt != normalizeReadyAt(snapshot.readyAt)) {
		return stateError(
			ErrInvalidTimer,
			snapshot.status,
			snapshot.status,
		)
	}
	if snapshot.revision == 0 ||
		snapshot.frameFloor == 0 ||
		snapshot.frameFloor > snapshot.sequence &&
			snapshot.frameFloor != snapshot.sequence+1 ||
		snapshot.sequence-snapshot.frameFloor+1 !=
			uint64(len(snapshot.frames)) {
		return stateError(
			ErrRevision,
			snapshot.status,
			snapshot.status,
		)
	}
	if snapshot.revision == 1 {
		initialWithoutInput := snapshot.sequence == 0 &&
			len(snapshot.frames) == 0
		initialWithInput := snapshot.sequence == 1 &&
			len(snapshot.frames) == 1 &&
			snapshot.frames[0].sequence == 1 &&
			snapshot.frames[0].kind == FrameAccepted
		if snapshot.status != StatusAccepted ||
			snapshot.fence != 0 ||
			snapshot.frameFloor != 1 ||
			len(snapshot.commands) != 0 ||
			(!initialWithoutInput && !initialWithInput) {
			return stateError(
				ErrRevision,
				snapshot.status,
				snapshot.status,
			)
		}
	}
	for index, frame := range snapshot.frames {
		if frame.sequence != snapshot.frameFloor+uint64(index) ||
			!frame.kind.valid() ||
			(frame.kind == FrameAccepted && frame.sequence != 1) ||
			len(frame.payload) > maxPayloadBytes {
			return stateError(
				ErrSequence,
				snapshot.status,
				snapshot.status,
			)
		}
	}
	for id, record := range snapshot.commands {
		if !validIdentity(id) ||
			(record.kind != commandSignal && record.kind != commandCancel) {
			return stateError(
				ErrInvalidIdentity,
				snapshot.status,
				snapshot.status,
			)
		}
	}
	return nil
}

func stateError(kind error, from, to Status) error {
	return &StateError{Kind: kind, From: from, To: to}
}

func commandDigest(transition Transition) [sha256.Size]byte {
	hasher := sha256.New()
	writeDigestPart(hasher, []byte("continuation-command-v1"))
	for _, frame := range transition.frames {
		writeDigestPart(hasher, []byte(frame.kind))
		writeDigestPart(hasher, frame.payload)
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func writeDigestPart(hasher hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = hasher.Write(size[:])
	_, _ = hasher.Write(value)
}
