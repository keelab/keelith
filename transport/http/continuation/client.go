// Package continuationhttp implements the continuation v1 HTTP profile.
package continuationhttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"strconv"
	"time"

	continuationv1 "github.com/keelab/keelith/api/continuation/v1"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/programmable/continuation"
	transporthttp "github.com/keelab/keelith/transport/http"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	continuationReconnectDelay     = 10 * time.Millisecond
	continuationClientCloseTimeout = 250 * time.Millisecond
)

var (
	// ErrInvalidContinuationProtocol reports an inconsistent or unsupported
	// continuation response.
	ErrInvalidContinuationProtocol = errors.New(
		"http transport: invalid continuation protocol",
	)
	// ErrContinuationTerminal reports a durable terminal state that is not a
	// successful completion.
	ErrContinuationTerminal = errors.New(
		"http transport: continuation did not complete successfully",
	)
)

// ContinuationClient is the complete outbound client for the versioned
// continuation HTTP profile.
type ContinuationClient struct {
	client    *transporthttp.Client
	baseURL   string
	start     operation.Operation
	attach    operation.Operation
	signal    operation.Operation
	cancel    operation.Operation
	eventSize int
}

// ContinuationClientStream validates one Attach SSE connection.
type ContinuationClientStream struct {
	stream    *transporthttp.SSEClientStream[*continuationv1.AttachResponse]
	callID    string
	operation string
	after     uint64
}

// NewContinuationClient constructs a profile client on an existing Keelith
// HTTP Client so outbound middleware, metadata, discovery, and TLS are reused.
func NewContinuationClient(
	client *transporthttp.Client,
	baseURL string,
) (*ContinuationClient, error) {
	if client == nil {
		return nil, fmt.Errorf(
			"%w: continuation client is nil",
			transporthttp.ErrInvalidCall,
		)
	}
	normalized, err := transporthttp.NormalizeClientBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	start, err := newContinuationHTTPOperation("Start", operation.KindUnary)
	if err != nil {
		return nil, err
	}
	attach, err := newContinuationHTTPOperation(
		"Attach",
		operation.KindServerStream,
	)
	if err != nil {
		return nil, err
	}
	signal, err := newContinuationHTTPOperation("Signal", operation.KindUnary)
	if err != nil {
		return nil, err
	}
	cancel, err := newContinuationHTTPOperation("Cancel", operation.KindUnary)
	if err != nil {
		return nil, err
	}
	return &ContinuationClient{
		client:    client,
		baseURL:   normalized,
		start:     start,
		attach:    attach,
		signal:    signal,
		cancel:    cancel,
		eventSize: defaultContinuationEventBytes,
	}, nil
}

// Start creates one durable call and returns either an accepted or inline
// terminal response.
func (client *ContinuationClient) Start(
	ctx context.Context,
	callID continuation.CallID,
	target continuation.Operation,
	input []byte,
) (*continuationv1.StartResponse, error) {
	if err := client.validateCall(ctx, callID, target); err != nil {
		return nil, err
	}
	if len(input) > maxContinuationPayloadBytes {
		return nil, transporthttp.ErrRequestTooLarge
	}
	message := &continuationv1.StartRequest{
		CallId:    callID.String(),
		Operation: target.String(),
		Input:     append([]byte(nil), input...),
	}
	request, err := transporthttp.NewProtoRequest(
		ctx,
		client.baseURL,
		nethttp.MethodPost,
		ContinuationRoutePrefix,
		message,
		"*",
	)
	if err != nil {
		return nil, err
	}
	response, err := transporthttp.InvokeProto(
		ctx,
		client.client,
		client.start,
		request,
		func() *continuationv1.StartResponse {
			return &continuationv1.StartResponse{}
		},
	)
	if err != nil {
		return nil, err
	}
	if err := validateContinuationStartResponse(
		response,
		callID.String(),
		target.String(),
	); err != nil {
		return nil, err
	}
	return response, nil
}

// Attach opens one validated SSE connection after an exclusive sequence.
func (client *ContinuationClient) Attach(
	ctx context.Context,
	callID continuation.CallID,
	after uint64,
) (*ContinuationClientStream, error) {
	if client == nil || ctx == nil ||
		!validContinuationHTTPIdentity(
			callID.String(),
			maxContinuationCallIDBytes,
		) {
		return nil, fmt.Errorf(
			"%w: invalid continuation Attach request",
			transporthttp.ErrInvalidCall,
		)
	}
	request, err := nethttp.NewRequestWithContext(
		ctx,
		nethttp.MethodGet,
		client.baseURL+ContinuationRoutePrefix+"/"+
			callID.String()+"/events",
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: build Attach request: %w", transporthttp.ErrInvalidCall, err)
	}
	request.Header.Set("Accept", "text/event-stream")
	options := []transporthttp.SSEClientOption{
		transporthttp.WithSSEClientMaxEventBytes(client.eventSize),
	}
	if after > 0 {
		options = append(
			options,
			transporthttp.WithSSELastEventID(strconv.FormatUint(after, 10)),
		)
	}
	stream, err := transporthttp.OpenProtoSSE(
		ctx,
		client.client,
		client.attach,
		request,
		func() *continuationv1.AttachResponse {
			return &continuationv1.AttachResponse{}
		},
		options...,
	)
	if err != nil {
		return nil, err
	}
	return &ContinuationClientStream{
		stream: stream,
		callID: callID.String(),
		after:  after,
	}, nil
}

// Signal submits one idempotent external signal.
func (client *ContinuationClient) Signal(
	ctx context.Context,
	callID continuation.CallID,
	commandID string,
	payload []byte,
) (*continuationv1.SignalResponse, error) {
	if client == nil || ctx == nil ||
		!validContinuationHTTPIdentity(
			callID.String(),
			maxContinuationCallIDBytes,
		) ||
		!validContinuationHTTPIdentity(
			commandID,
			maxContinuationCommandBytes,
		) {
		return nil, fmt.Errorf(
			"%w: invalid continuation Signal request",
			transporthttp.ErrInvalidCall,
		)
	}
	if len(payload) > maxContinuationPayloadBytes {
		return nil, transporthttp.ErrRequestTooLarge
	}
	message := &continuationv1.SignalRequest{
		CallId:    callID.String(),
		CommandId: commandID,
		Payload:   append([]byte(nil), payload...),
	}
	request, err := transporthttp.NewProtoRequest(
		ctx,
		client.baseURL,
		nethttp.MethodPost,
		continuationSignalPattern,
		message,
		"*",
	)
	if err != nil {
		return nil, err
	}
	response, err := transporthttp.InvokeProto(
		ctx,
		client.client,
		client.signal,
		request,
		func() *continuationv1.SignalResponse {
			return &continuationv1.SignalResponse{}
		},
	)
	if err != nil {
		return nil, err
	}
	if err := validateContinuationCommandSnapshot(
		response.GetSnapshot(),
		callID.String(),
	); err != nil {
		return nil, err
	}
	return response, nil
}

// Cancel submits one idempotent cooperative cancellation request.
func (client *ContinuationClient) Cancel(
	ctx context.Context,
	callID continuation.CallID,
	commandID string,
) (*continuationv1.CancelResponse, error) {
	if client == nil || ctx == nil ||
		!validContinuationHTTPIdentity(
			callID.String(),
			maxContinuationCallIDBytes,
		) ||
		!validContinuationHTTPIdentity(
			commandID,
			maxContinuationCommandBytes,
		) {
		return nil, fmt.Errorf(
			"%w: invalid continuation Cancel request",
			transporthttp.ErrInvalidCall,
		)
	}
	payload, err := protojson.Marshal(&continuationv1.CancelRequest{
		CallId:    callID.String(),
		CommandId: commandID,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: encode Cancel request: %w", transporthttp.ErrInvalidCall, err)
	}
	request, err := nethttp.NewRequestWithContext(
		ctx,
		nethttp.MethodDelete,
		client.baseURL+ContinuationRoutePrefix+"/"+callID.String(),
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: build Cancel request: %w", transporthttp.ErrInvalidCall, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	response, err := transporthttp.InvokeProto(
		ctx,
		client.client,
		client.cancel,
		request,
		func() *continuationv1.CancelResponse {
			return &continuationv1.CancelResponse{}
		},
	)
	if err != nil {
		return nil, err
	}
	if err := validateContinuationCommandSnapshot(
		response.GetSnapshot(),
		callID.String(),
	); err != nil {
		return nil, err
	}
	return response, nil
}

// Call starts a fixed durable operation, reconnects Attach after validated
// frames, and returns the successful terminal payload.
func (client *ContinuationClient) Call(
	ctx context.Context,
	callID continuation.CallID,
	target continuation.Operation,
	input []byte,
) ([]byte, error) {
	response, err := client.Start(ctx, callID, target, input)
	if err != nil {
		return nil, err
	}
	if response.GetSnapshot().GetTerminal() {
		return continuationCompletedPayload(
			response.GetSnapshot(),
			response.GetTerminalFrame(),
		)
	}

	after := response.GetSnapshot().GetSequence()
	for {
		stream, attachErr := client.Attach(ctx, callID, after)
		if attachErr != nil {
			return nil, attachErr
		}
		for {
			response, receiveErr := stream.Recv()
			if receiveErr != nil {
				closeContinuationClientStream(stream)
				if !errors.Is(receiveErr, io.EOF) {
					return nil, receiveErr
				}
				break
			}
			page := response.GetPage()
			frame := page.GetFrames()[0]
			if page.GetSnapshot().GetOperation() != target.String() {
				closeContinuationClientStream(stream)
				return nil, fmt.Errorf(
					"%w: Attach operation changed",
					ErrInvalidContinuationProtocol,
				)
			}
			after = frame.GetSequence()
			if page.GetSnapshot().GetTerminal() &&
				after == page.GetSnapshot().GetSequence() {
				closeContinuationClientStream(stream)
				return continuationCompletedPayload(
					page.GetSnapshot(),
					frame,
				)
			}
		}
		timer := time.NewTimer(continuationReconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
	}
}

// Recv returns the next strictly contiguous, profile-valid Attach page.
func (stream *ContinuationClientStream) Recv() (*continuationv1.AttachResponse, error) {
	if stream == nil || stream.stream == nil {
		return nil, ErrInvalidContinuationProtocol
	}
	message, err := stream.stream.Recv()
	if err != nil {
		return nil, err
	}
	response := message.Value()
	page := response.GetPage()
	if page == nil || len(page.GetFrames()) != 1 {
		return nil, ErrInvalidContinuationProtocol
	}
	frame := page.GetFrames()[0]
	if message.Name() != continuationEvent ||
		frame == nil ||
		frame.GetSequence() != stream.after+1 ||
		message.ID() != strconv.FormatUint(frame.GetSequence(), 10) ||
		frame.GetKind() == continuationv1.FrameKind_FRAME_KIND_UNSPECIFIED ||
		len(frame.GetPayload()) > maxContinuationPayloadBytes ||
		page.GetNextSequence() != frame.GetSequence()+1 {
		return nil, ErrInvalidContinuationProtocol
	}
	snapshot := page.GetSnapshot()
	operationName, err := validateContinuationSnapshot(
		snapshot,
		stream.callID,
	)
	if err != nil ||
		snapshot.GetSequence() < frame.GetSequence() ||
		page.GetFrameFloor() != snapshot.GetFrameFloor() ||
		page.GetTerminal() != snapshot.GetTerminal() {
		return nil, ErrInvalidContinuationProtocol
	}
	if stream.operation == "" {
		stream.operation = operationName
	} else if stream.operation != operationName {
		return nil, ErrInvalidContinuationProtocol
	}
	if frame.GetSequence() == snapshot.GetSequence() &&
		snapshot.GetTerminal() &&
		!continuationTerminalFrameMatches(snapshot, frame) {
		return nil, ErrInvalidContinuationProtocol
	}
	if !snapshot.GetTerminal() && continuationFrameKindTerminal(frame.GetKind()) {
		return nil, ErrInvalidContinuationProtocol
	}
	stream.after = frame.GetSequence()
	return response, nil
}

// Close closes the current Attach connection.
func (stream *ContinuationClientStream) Close(ctx context.Context) error {
	if stream == nil || stream.stream == nil {
		return nil
	}
	return stream.stream.Close(ctx)
}

func (client *ContinuationClient) validateCall(
	ctx context.Context,
	callID continuation.CallID,
	target continuation.Operation,
) error {
	if client == nil || ctx == nil ||
		!validContinuationHTTPIdentity(
			callID.String(),
			maxContinuationCallIDBytes,
		) ||
		len(target.String()) > maxContinuationOperationBytes {
		return fmt.Errorf(
			"%w: invalid continuation call",
			transporthttp.ErrInvalidCall,
		)
	}
	if _, err := continuation.NewOperation(target.String()); err != nil {
		return fmt.Errorf("%w: invalid continuation operation", transporthttp.ErrInvalidCall)
	}
	return nil
}

func validateContinuationStartResponse(
	response *continuationv1.StartResponse,
	callID string,
	operationName string,
) error {
	if response == nil {
		return ErrInvalidContinuationProtocol
	}
	gotOperation, err := validateContinuationSnapshot(
		response.GetSnapshot(),
		callID,
	)
	if err != nil || gotOperation != operationName {
		return ErrInvalidContinuationProtocol
	}
	frame := response.GetTerminalFrame()
	if response.GetSnapshot().GetTerminal() {
		if !continuationTerminalFrameMatches(response.GetSnapshot(), frame) {
			return ErrInvalidContinuationProtocol
		}
	} else if frame != nil {
		return ErrInvalidContinuationProtocol
	}
	return nil
}

func validateContinuationCommandSnapshot(
	snapshot *continuationv1.Snapshot,
	callID string,
) error {
	_, err := validateContinuationSnapshot(snapshot, callID)
	return err
}

func validateContinuationSnapshot(
	snapshot *continuationv1.Snapshot,
	callID string,
) (string, error) {
	if snapshot == nil ||
		snapshot.GetCallId() != callID ||
		snapshot.GetRevision() == 0 ||
		snapshot.GetSequence() == 0 ||
		snapshot.GetFrameFloor() == 0 ||
		snapshot.GetFrameFloor() > snapshot.GetSequence()+1 {
		return "", ErrInvalidContinuationProtocol
	}
	if _, err := continuation.NewCallID(snapshot.GetCallId()); err != nil {
		return "", ErrInvalidContinuationProtocol
	}
	target, err := continuation.NewOperation(snapshot.GetOperation())
	if err != nil {
		return "", ErrInvalidContinuationProtocol
	}
	terminal, valid := continuationStatusTerminal(snapshot.GetStatus())
	if !valid || terminal != snapshot.GetTerminal() {
		return "", ErrInvalidContinuationProtocol
	}
	return target.String(), nil
}

func continuationStatusTerminal(
	status continuationv1.CallStatus,
) (bool, bool) {
	switch status {
	case continuationv1.CallStatus_CALL_STATUS_ACCEPTED,
		continuationv1.CallStatus_CALL_STATUS_RUNNING,
		continuationv1.CallStatus_CALL_STATUS_WAITING,
		continuationv1.CallStatus_CALL_STATUS_SUSPENDED,
		continuationv1.CallStatus_CALL_STATUS_CANCEL_REQUESTED:
		return false, true
	case continuationv1.CallStatus_CALL_STATUS_COMPLETED,
		continuationv1.CallStatus_CALL_STATUS_FAILED,
		continuationv1.CallStatus_CALL_STATUS_CANCELED,
		continuationv1.CallStatus_CALL_STATUS_EXPIRED:
		return true, true
	default:
		return false, false
	}
}

func continuationTerminalFrameMatches(
	snapshot *continuationv1.Snapshot,
	frame *continuationv1.Frame,
) bool {
	if snapshot == nil ||
		frame == nil ||
		frame.GetSequence() != snapshot.GetSequence() ||
		len(frame.GetPayload()) > maxContinuationPayloadBytes {
		return false
	}
	switch snapshot.GetStatus() {
	case continuationv1.CallStatus_CALL_STATUS_COMPLETED:
		return frame.GetKind() ==
			continuationv1.FrameKind_FRAME_KIND_COMPLETED
	case continuationv1.CallStatus_CALL_STATUS_FAILED:
		return frame.GetKind() ==
			continuationv1.FrameKind_FRAME_KIND_FAILED
	case continuationv1.CallStatus_CALL_STATUS_CANCELED:
		return frame.GetKind() ==
			continuationv1.FrameKind_FRAME_KIND_CANCELED
	case continuationv1.CallStatus_CALL_STATUS_EXPIRED:
		return frame.GetKind() ==
			continuationv1.FrameKind_FRAME_KIND_EXPIRED
	default:
		return false
	}
}

func continuationFrameKindTerminal(kind continuationv1.FrameKind) bool {
	switch kind {
	case continuationv1.FrameKind_FRAME_KIND_COMPLETED,
		continuationv1.FrameKind_FRAME_KIND_FAILED,
		continuationv1.FrameKind_FRAME_KIND_CANCELED,
		continuationv1.FrameKind_FRAME_KIND_EXPIRED:
		return true
	default:
		return false
	}
}

func continuationCompletedPayload(
	snapshot *continuationv1.Snapshot,
	frame *continuationv1.Frame,
) ([]byte, error) {
	if !continuationTerminalFrameMatches(snapshot, frame) {
		return nil, ErrInvalidContinuationProtocol
	}
	if snapshot.GetStatus() !=
		continuationv1.CallStatus_CALL_STATUS_COMPLETED {
		return nil, fmt.Errorf(
			"%w: status %s",
			ErrContinuationTerminal,
			snapshot.GetStatus().String(),
		)
	}
	return append([]byte(nil), frame.GetPayload()...), nil
}

func closeContinuationClientStream(stream *ContinuationClientStream) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		continuationClientCloseTimeout,
	)
	defer cancel()
	_ = stream.Close(ctx)
}
