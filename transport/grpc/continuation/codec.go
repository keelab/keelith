package continuationgrpc

import (
	"errors"
	"fmt"
	"time"

	continuationv1 "github.com/keelab/keelith/api/continuation/v1"
	"github.com/keelab/keelith/programmable/continuation"
)

const (
	maxWirePayloadBytes        = 1024 * 1024
	maxWirePageFrames          = 1000
	maxWireMessageBytes        = 32 * 1024 * 1024
	maxWireHistoryPayloadBytes = 4 * 1024 * 1024
)

var (
	// ErrInvalidWireMessage reports a malformed continuation v1 message.
	ErrInvalidWireMessage = errors.New(
		"continuation grpc transport: invalid wire message",
	)
	// ErrWireMessageTooLarge reports a message outside the configured budget.
	ErrWireMessageTooLarge = errors.New(
		"continuation grpc transport: wire message too large",
	)
)

func callIDFromWire(value string) (continuation.CallID, error) {
	callID, err := continuation.NewCallID(value)
	if err != nil {
		return continuation.CallID{}, ErrInvalidWireMessage
	}
	return callID, nil
}

func operationFromWire(value string) (continuation.Operation, error) {
	target, err := continuation.NewOperation(value)
	if err != nil {
		return continuation.Operation{}, ErrInvalidWireMessage
	}
	return target, nil
}

func validCommandID(value string) bool {
	_, err := continuation.NewCallID(value)
	return err == nil
}

func snapshotToWire(
	snapshot continuation.Snapshot,
) (*continuationv1.Snapshot, error) {
	if err := continuation.ValidateSnapshot(snapshot); err != nil {
		return nil, ErrInvalidWireMessage
	}
	status, err := statusToWire(snapshot.Status())
	if err != nil {
		return nil, err
	}
	encoded := &continuationv1.Snapshot{
		CallId:     snapshot.CallID().String(),
		Operation:  snapshot.Operation().String(),
		Status:     status,
		Revision:   snapshot.Revision(),
		Fence:      snapshot.Fence(),
		Sequence:   snapshot.Sequence(),
		FrameFloor: snapshot.FrameFloor(),
		Terminal:   snapshot.Status().Terminal(),
	}
	if !snapshot.ReadyAt().IsZero() {
		encoded.ReadyAt = snapshot.ReadyAt().UTC().Format(time.RFC3339Nano)
	}
	if workflow, ok := snapshot.Workflow(); ok {
		encoded.Workflow, err = workflowToWire(workflow)
		if err != nil {
			return nil, err
		}
	}
	return encoded, nil
}

func frameToWire(
	frame continuation.Frame,
) (*continuationv1.Frame, error) {
	if frame.Sequence() == 0 || len(frame.Payload()) > maxWirePayloadBytes {
		return nil, ErrInvalidWireMessage
	}
	kind, err := frameKindToWire(frame.Kind())
	if err != nil {
		return nil, err
	}
	return &continuationv1.Frame{
		Sequence: frame.Sequence(),
		Kind:     kind,
		Payload:  frame.Payload(),
	}, nil
}

func pageToWire(
	attachment continuation.Attachment,
	after uint64,
	limit int,
) (*continuationv1.Page, error) {
	if limit < 1 || limit > maxWirePageFrames ||
		len(attachment.Frames) > limit ||
		attachment.FrameFloor != attachment.Snapshot.FrameFloor() ||
		attachment.Terminal != attachment.Snapshot.Status().Terminal() {
		return nil, ErrInvalidWireMessage
	}
	snapshot, err := snapshotToWire(attachment.Snapshot)
	if err != nil {
		return nil, err
	}
	frames := make([]*continuationv1.Frame, len(attachment.Frames))
	expected := after + 1
	for index, frame := range attachment.Frames {
		if frame.Sequence() != expected {
			return nil, ErrInvalidWireMessage
		}
		encoded, frameErr := frameToWire(frame)
		if frameErr != nil {
			return nil, frameErr
		}
		frames[index] = encoded
		expected++
	}
	if attachment.NextSequence != expected ||
		attachment.NextSequence > attachment.Snapshot.Sequence()+1 {
		return nil, ErrInvalidWireMessage
	}
	return &continuationv1.Page{
		Snapshot:     snapshot,
		Frames:       frames,
		NextSequence: attachment.NextSequence,
		FrameFloor:   attachment.FrameFloor,
		Terminal:     attachment.Terminal,
	}, nil
}

func startResponseToWire(
	snapshot continuation.Snapshot,
) (*continuationv1.StartResponse, error) {
	encoded, err := snapshotToWire(snapshot)
	if err != nil {
		return nil, err
	}
	response := &continuationv1.StartResponse{Snapshot: encoded}
	if !snapshot.Status().Terminal() {
		return response, nil
	}
	frames := snapshot.Frames()
	if len(frames) == 0 {
		return response, nil
	}
	last := frames[len(frames)-1]
	if !terminalFrameKind(last.Kind()) {
		return response, nil
	}
	response.TerminalFrame, err = frameToWire(last)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func splitAttachPage(
	page *continuationv1.Page,
) []*continuationv1.AttachResponse {
	if page == nil {
		return nil
	}
	if len(page.GetFrames()) == 0 {
		if !page.GetTerminal() {
			return nil
		}
		return []*continuationv1.AttachResponse{{Page: page}}
	}
	responses := make([]*continuationv1.AttachResponse, len(page.GetFrames()))
	for index, frame := range page.GetFrames() {
		responses[index] = &continuationv1.AttachResponse{
			Page: &continuationv1.Page{
				Snapshot:     page.GetSnapshot(),
				Frames:       []*continuationv1.Frame{frame},
				NextSequence: frame.GetSequence() + 1,
				FrameFloor:   page.GetFrameFloor(),
				Terminal:     page.GetTerminal(),
			},
		}
	}
	return responses
}

func validateStartResponse(
	response *continuationv1.StartResponse,
	callID continuation.CallID,
	target continuation.Operation,
) error {
	if response == nil || validateSnapshotWire(response.GetSnapshot()) != nil ||
		response.GetSnapshot().GetCallId() != callID.String() ||
		response.GetSnapshot().GetOperation() != target.String() {
		return ErrInvalidWireMessage
	}
	terminal := response.GetSnapshot().GetTerminal()
	if response.GetTerminalFrame() != nil {
		if !terminal || validateFrameWire(response.GetTerminalFrame()) != nil ||
			response.GetTerminalFrame().GetSequence() !=
				response.GetSnapshot().GetSequence() {
			return ErrInvalidWireMessage
		}
	}
	return nil
}

func validatePageWire(
	page *continuationv1.Page,
	callID continuation.CallID,
	after uint64,
	limit int,
) error {
	if page == nil || limit < 1 || limit > maxWirePageFrames ||
		validateSnapshotWire(page.GetSnapshot()) != nil ||
		page.GetSnapshot().GetCallId() != callID.String() ||
		page.GetFrameFloor() != page.GetSnapshot().GetFrameFloor() ||
		page.GetTerminal() != page.GetSnapshot().GetTerminal() ||
		len(page.GetFrames()) > limit {
		return ErrInvalidWireMessage
	}
	expected := after + 1
	for _, frame := range page.GetFrames() {
		if validateFrameWire(frame) != nil || frame.GetSequence() != expected {
			return ErrInvalidWireMessage
		}
		expected++
	}
	if page.GetNextSequence() != expected ||
		page.GetNextSequence() > page.GetSnapshot().GetSequence()+1 {
		return ErrInvalidWireMessage
	}
	return nil
}

func validateSnapshotWire(snapshot *continuationv1.Snapshot) error {
	if snapshot == nil || snapshot.GetRevision() == 0 ||
		snapshot.GetFrameFloor() == 0 ||
		snapshot.GetFrameFloor() > snapshot.GetSequence()+1 {
		return ErrInvalidWireMessage
	}
	if _, err := callIDFromWire(snapshot.GetCallId()); err != nil {
		return err
	}
	if _, err := operationFromWire(snapshot.GetOperation()); err != nil {
		return err
	}
	status, err := statusFromWire(snapshot.GetStatus())
	if err != nil || status.Terminal() != snapshot.GetTerminal() {
		return ErrInvalidWireMessage
	}
	if readyAt := snapshot.GetReadyAt(); readyAt != "" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, readyAt)
		if parseErr != nil || readyAt != parsed.UTC().Format(time.RFC3339Nano) ||
			status != continuation.StatusSuspended {
			return ErrInvalidWireMessage
		}
	}
	if snapshot.GetWorkflow() != nil &&
		validateWorkflowWire(snapshot.GetWorkflow()) != nil {
		return ErrInvalidWireMessage
	}
	return nil
}

func validateFrameWire(frame *continuationv1.Frame) error {
	if frame == nil || frame.GetSequence() == 0 ||
		len(frame.GetPayload()) > maxWirePayloadBytes {
		return ErrInvalidWireMessage
	}
	_, err := frameKindFromWire(frame.GetKind())
	return err
}

func statusToWire(
	status continuation.Status,
) (continuationv1.CallStatus, error) {
	switch status {
	case continuation.StatusAccepted:
		return continuationv1.CallStatus_CALL_STATUS_ACCEPTED, nil
	case continuation.StatusRunning:
		return continuationv1.CallStatus_CALL_STATUS_RUNNING, nil
	case continuation.StatusWaiting:
		return continuationv1.CallStatus_CALL_STATUS_WAITING, nil
	case continuation.StatusSuspended:
		return continuationv1.CallStatus_CALL_STATUS_SUSPENDED, nil
	case continuation.StatusCancelRequested:
		return continuationv1.CallStatus_CALL_STATUS_CANCEL_REQUESTED, nil
	case continuation.StatusCompleted:
		return continuationv1.CallStatus_CALL_STATUS_COMPLETED, nil
	case continuation.StatusFailed:
		return continuationv1.CallStatus_CALL_STATUS_FAILED, nil
	case continuation.StatusCanceled:
		return continuationv1.CallStatus_CALL_STATUS_CANCELED, nil
	case continuation.StatusExpired:
		return continuationv1.CallStatus_CALL_STATUS_EXPIRED, nil
	default:
		return continuationv1.CallStatus_CALL_STATUS_UNSPECIFIED,
			ErrInvalidWireMessage
	}
}

func statusFromWire(
	status continuationv1.CallStatus,
) (continuation.Status, error) {
	switch status {
	case continuationv1.CallStatus_CALL_STATUS_ACCEPTED:
		return continuation.StatusAccepted, nil
	case continuationv1.CallStatus_CALL_STATUS_RUNNING:
		return continuation.StatusRunning, nil
	case continuationv1.CallStatus_CALL_STATUS_WAITING:
		return continuation.StatusWaiting, nil
	case continuationv1.CallStatus_CALL_STATUS_SUSPENDED:
		return continuation.StatusSuspended, nil
	case continuationv1.CallStatus_CALL_STATUS_CANCEL_REQUESTED:
		return continuation.StatusCancelRequested, nil
	case continuationv1.CallStatus_CALL_STATUS_COMPLETED:
		return continuation.StatusCompleted, nil
	case continuationv1.CallStatus_CALL_STATUS_FAILED:
		return continuation.StatusFailed, nil
	case continuationv1.CallStatus_CALL_STATUS_CANCELED:
		return continuation.StatusCanceled, nil
	case continuationv1.CallStatus_CALL_STATUS_EXPIRED:
		return continuation.StatusExpired, nil
	default:
		return "", ErrInvalidWireMessage
	}
}

func frameKindToWire(
	kind continuation.FrameKind,
) (continuationv1.FrameKind, error) {
	switch kind {
	case continuation.FrameAccepted:
		return continuationv1.FrameKind_FRAME_KIND_ACCEPTED, nil
	case continuation.FrameEvent:
		return continuationv1.FrameKind_FRAME_KIND_EVENT, nil
	case continuation.FrameWaiting:
		return continuationv1.FrameKind_FRAME_KIND_WAITING, nil
	case continuation.FrameSignal:
		return continuationv1.FrameKind_FRAME_KIND_SIGNAL, nil
	case continuation.FrameSuspended:
		return continuationv1.FrameKind_FRAME_KIND_SUSPENDED, nil
	case continuation.FrameCancelRequested:
		return continuationv1.FrameKind_FRAME_KIND_CANCEL_REQUESTED, nil
	case continuation.FrameCompleted:
		return continuationv1.FrameKind_FRAME_KIND_COMPLETED, nil
	case continuation.FrameFailed:
		return continuationv1.FrameKind_FRAME_KIND_FAILED, nil
	case continuation.FrameCanceled:
		return continuationv1.FrameKind_FRAME_KIND_CANCELED, nil
	case continuation.FrameExpired:
		return continuationv1.FrameKind_FRAME_KIND_EXPIRED, nil
	case continuation.FrameWorkflowChild:
		return continuationv1.FrameKind_FRAME_KIND_WORKFLOW_CHILD, nil
	default:
		return continuationv1.FrameKind_FRAME_KIND_UNSPECIFIED,
			ErrInvalidWireMessage
	}
}

func frameKindFromWire(
	kind continuationv1.FrameKind,
) (continuation.FrameKind, error) {
	switch kind {
	case continuationv1.FrameKind_FRAME_KIND_ACCEPTED:
		return continuation.FrameAccepted, nil
	case continuationv1.FrameKind_FRAME_KIND_EVENT:
		return continuation.FrameEvent, nil
	case continuationv1.FrameKind_FRAME_KIND_WAITING:
		return continuation.FrameWaiting, nil
	case continuationv1.FrameKind_FRAME_KIND_SIGNAL:
		return continuation.FrameSignal, nil
	case continuationv1.FrameKind_FRAME_KIND_SUSPENDED:
		return continuation.FrameSuspended, nil
	case continuationv1.FrameKind_FRAME_KIND_CANCEL_REQUESTED:
		return continuation.FrameCancelRequested, nil
	case continuationv1.FrameKind_FRAME_KIND_COMPLETED:
		return continuation.FrameCompleted, nil
	case continuationv1.FrameKind_FRAME_KIND_FAILED:
		return continuation.FrameFailed, nil
	case continuationv1.FrameKind_FRAME_KIND_CANCELED:
		return continuation.FrameCanceled, nil
	case continuationv1.FrameKind_FRAME_KIND_EXPIRED:
		return continuation.FrameExpired, nil
	case continuationv1.FrameKind_FRAME_KIND_WORKFLOW_CHILD:
		return continuation.FrameWorkflowChild, nil
	default:
		return "", fmt.Errorf("%w: frame kind", ErrInvalidWireMessage)
	}
}

func terminalFrameKind(kind continuation.FrameKind) bool {
	switch kind {
	case continuation.FrameCompleted,
		continuation.FrameFailed,
		continuation.FrameCanceled,
		continuation.FrameExpired:
		return true
	default:
		return false
	}
}
