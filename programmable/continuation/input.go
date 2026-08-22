package continuation

// NewSnapshotWithInput creates one accepted call at revision one and persists
// its input as the sole FrameAccepted frame at sequence one.
func NewSnapshotWithInput(
	callID CallID,
	operation Operation,
	input []byte,
) (Snapshot, error) {
	snapshot, err := NewSnapshot(callID, operation)
	if err != nil {
		return Snapshot{}, err
	}
	frame, err := NewFrame(FrameAccepted, input)
	if err != nil {
		return Snapshot{}, err
	}
	frame.sequence = 1
	snapshot.sequence = 1
	snapshot.frames = []Frame{frame}
	return snapshot, nil
}

// Input returns an independent copy of the initial accepted payload.
//
// Snapshots created with NewSnapshot have no input and return nil.
func (snapshot Snapshot) Input() []byte {
	if len(snapshot.frames) == 0 ||
		snapshot.frames[0].sequence != 1 ||
		snapshot.frames[0].kind != FrameAccepted {
		return nil
	}
	return snapshot.frames[0].Payload()
}

// ValidateSnapshot verifies durable identity, lifecycle, sequence, frame, and
// command-deduplication invariants.
func ValidateSnapshot(snapshot Snapshot) error {
	return validateSnapshot(snapshot)
}
