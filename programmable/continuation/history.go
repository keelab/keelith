package continuation

import "errors"

const maxHistoryPayloadBytes = 4 * 1024 * 1024

var (
	// ErrInvalidHistory reports an invalid detail payload budget.
	ErrInvalidHistory = errors.New("continuation: invalid history request")
	// ErrHistoryBudget reports a detail page beyond its authorized byte budget.
	ErrHistoryBudget = errors.New("continuation: history payload budget exceeded")
)

func payloadFreeHistory(attachment Attachment) Attachment {
	result := attachment
	result.Snapshot = cloneSnapshot(attachment.Snapshot)
	for index := range result.Snapshot.frames {
		result.Snapshot.frames[index].payload = nil
	}
	result.Frames = cloneFrames(attachment.Frames)
	for index := range result.Frames {
		result.Frames[index].payload = nil
	}
	return result
}

func boundedDetailHistory(
	attachment Attachment,
	maxPayloadBytes int,
) (Attachment, error) {
	if maxPayloadBytes <= 0 || maxPayloadBytes > maxHistoryPayloadBytes {
		return Attachment{}, ErrInvalidHistory
	}
	consumed := 0
	for _, frame := range attachment.Frames {
		if len(frame.payload) > maxPayloadBytes-consumed {
			return Attachment{}, ErrHistoryBudget
		}
		consumed += len(frame.payload)
	}
	result := attachment
	result.Snapshot = cloneSnapshot(attachment.Snapshot)
	for index := range result.Snapshot.frames {
		result.Snapshot.frames[index].payload = nil
	}
	result.Frames = cloneFrames(attachment.Frames)
	return result, nil
}
