package continuationhttp

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	continuationv1 "github.com/keelab/keelith/api/continuation/v1"
	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/programmable/continuation"
	"github.com/keelab/keelith/security/authz"
	transporthttp "github.com/keelab/keelith/transport/http"
	ksse "github.com/keelab/keelith/transport/sse"
)

const (
	// ContinuationProtocolVersion is the stable wire version emitted and
	// required by the continuation HTTP profile.
	ContinuationProtocolVersion = "v1"
	// ContinuationRoutePrefix is the versioned root for continuation calls.
	ContinuationRoutePrefix = "/keelith.continuation.v1/continuations"

	continuationAttachPattern = ContinuationRoutePrefix + "/{call_id}/events"
	continuationSignalPattern = ContinuationRoutePrefix + "/{call_id}:signal"
	continuationCancelPattern = ContinuationRoutePrefix + "/{call_id}"

	continuationService = "keelith.continuation.v1.ContinuationService"
	continuationEvent   = "continuation.frame.v1"

	defaultContinuationPollInterval = 250 * time.Millisecond
	defaultContinuationPageSize     = 100
	defaultContinuationEventBytes   = ksse.MaximumEventBytes
	defaultContinuationInlineBudget = 50 * time.Millisecond
	defaultContinuationInlineSteps  = 64

	maxContinuationPageSize       = 1000
	maxContinuationCallIDBytes    = 256
	maxContinuationCommandBytes   = 256
	maxContinuationOperationBytes = 512
	maxContinuationWorkflowBytes  = 256
	maxContinuationPayloadBytes   = 1024 * 1024
	maxContinuationRequestBytes   = 1536 * 1024
)

// ContinuationRuntime is the transport-facing durable continuation API.
//
// *continuation.Runtime implements this interface directly. An application
// adapter may synchronously finish StartCall and return a terminal Snapshot;
// the HTTP start route then responds with 200 instead of 202.
type ContinuationRuntime interface {
	StartCall(
		context.Context,
		continuation.CallID,
		continuation.Operation,
		[]byte,
	) (continuation.Snapshot, error)
	Attach(
		context.Context,
		continuation.CallID,
		uint64,
		int,
	) (continuation.Attachment, error)
	SubmitSignal(
		context.Context,
		continuation.CallID,
		string,
		[]byte,
	) (continuation.Snapshot, error)
	RequestCancel(
		context.Context,
		continuation.CallID,
		string,
	) (continuation.Snapshot, error)
}

// ContinuationInlineRuntime is the optional synchronous fast path used by the
// Start route. Implementations must persist the call before executing it and
// return a non-terminal Snapshot when the budget is exhausted.
type ContinuationInlineRuntime interface {
	StartCallInline(
		context.Context,
		continuation.CallID,
		continuation.Operation,
		[]byte,
		continuation.InlineBudget,
	) (continuation.Snapshot, error)
}

type continuationWorkflowRuntime interface {
	StartWorkflow(
		context.Context,
		continuation.CallID,
		continuation.Operation,
		string,
		[]byte,
	) (continuation.Snapshot, error)
}

// ContinuationRouteOption configures continuation route polling and bounds.
type ContinuationRouteOption interface {
	applyContinuationRoute(*continuationRouteOptions) error
}

type continuationRouteOptionFunc func(*continuationRouteOptions) error

func (f continuationRouteOptionFunc) applyContinuationRoute(
	options *continuationRouteOptions,
) error {
	return f(options)
}

type continuationRouteOptions struct {
	pollInterval time.Duration
	pageSize     int
	eventBytes   int
	inlineBudget continuation.InlineBudget
	publicAccess bool
	authorizer   authz.Authorizer
	ownerships   continuation.OwnershipStore
	accessSet    bool
}

// WithContinuationPublicAccess explicitly keeps the versioned continuation
// routes unauthenticated.
func WithContinuationPublicAccess() ContinuationRouteOption {
	return continuationRouteOptionFunc(func(
		options *continuationRouteOptions,
	) error {
		if options.accessSet {
			return fmt.Errorf("continuation access policy is duplicated")
		}
		options.publicAccess = true
		options.accessSet = true
		return nil
	})
}

// WithContinuationAuthorizer enables principal-bound CallID ownership and
// delegates each resource/action decision to authorizer.
func WithContinuationAuthorizer(
	authorizer authz.Authorizer,
	ownerships continuation.OwnershipStore,
) ContinuationRouteOption {
	return continuationRouteOptionFunc(func(
		options *continuationRouteOptions,
	) error {
		if options.accessSet || isNilContinuationValue(authorizer) ||
			isNilContinuationValue(ownerships) {
			return fmt.Errorf("continuation access policy is invalid")
		}
		options.authorizer = authorizer
		options.ownerships = ownerships
		options.accessSet = true
		return nil
	})
}

// WithContinuationPollInterval sets the idle Attach polling interval.
func WithContinuationPollInterval(interval time.Duration) ContinuationRouteOption {
	return continuationRouteOptionFunc(func(
		options *continuationRouteOptions,
	) error {
		if interval < time.Millisecond || interval > time.Minute {
			return fmt.Errorf(
				"continuation poll interval is outside supported bounds",
			)
		}
		options.pollInterval = interval
		return nil
	})
}

// WithContinuationPageSize sets the bounded number of frames loaded per poll.
func WithContinuationPageSize(size int) ContinuationRouteOption {
	return continuationRouteOptionFunc(func(
		options *continuationRouteOptions,
	) error {
		if size < 1 || size > maxContinuationPageSize {
			return fmt.Errorf(
				"continuation page size is outside supported bounds",
			)
		}
		options.pageSize = size
		return nil
	})
}

// WithContinuationMaxEventBytes sets the fully rendered SSE event budget.
func WithContinuationMaxEventBytes(maximum int) ContinuationRouteOption {
	return continuationRouteOptionFunc(func(
		options *continuationRouteOptions,
	) error {
		if maximum < 256 || maximum > ksse.MaximumEventBytes {
			return fmt.Errorf(
				"continuation event size is outside supported bounds",
			)
		}
		options.eventBytes = maximum
		return nil
	})
}

// WithContinuationInlineBudget sets the synchronous Start execution budget.
func WithContinuationInlineBudget(
	budget continuation.InlineBudget,
) ContinuationRouteOption {
	return continuationRouteOptionFunc(func(
		options *continuationRouteOptions,
	) error {
		if budget.Duration < time.Millisecond ||
			budget.Duration > 5*time.Second ||
			budget.MaxTransitions < 1 ||
			budget.MaxTransitions > 10_000 {
			return fmt.Errorf(
				"continuation inline budget is outside supported bounds",
			)
		}
		options.inlineBudget = budget
		return nil
	})
}

// RegisterContinuationRoutes installs the versioned start, Attach SSE, Signal,
// and Cancel endpoints through Router's public registration API.
func RegisterContinuationRoutes(
	router *transporthttp.Router,
	runtime ContinuationRuntime,
	optionList ...ContinuationRouteOption,
) error {
	if router == nil || isNilContinuationRuntime(runtime) {
		return fmt.Errorf(
			"%w: continuation router or runtime is nil",
			transporthttp.ErrInvalidRoute,
		)
	}
	options := continuationRouteOptions{
		pollInterval: defaultContinuationPollInterval,
		pageSize:     defaultContinuationPageSize,
		eventBytes:   defaultContinuationEventBytes,
		inlineBudget: continuation.InlineBudget{
			Duration:       defaultContinuationInlineBudget,
			MaxTransitions: defaultContinuationInlineSteps,
		},
	}
	for index, option := range optionList {
		if option == nil {
			return fmt.Errorf(
				"%w: continuation option %d is nil",
				transporthttp.ErrInvalidRoute,
				index,
			)
		}
		if err := option.applyContinuationRoute(&options); err != nil {
			return fmt.Errorf(
				"%w: continuation option %d: %w",
				transporthttp.ErrInvalidRoute,
				index,
				err,
			)
		}
	}
	if !options.accessSet {
		return fmt.Errorf(
			"%w: continuation access policy must be explicit",
			transporthttp.ErrInvalidRoute,
		)
	}
	service, serviceErr := continuation.NewService(
		continuation.ServiceConfig{
			Runtime:      runtime,
			PublicAccess: options.publicAccess,
			Authorizer:   options.authorizer,
			Ownerships:   options.ownerships,
		},
	)
	if serviceErr != nil {
		return fmt.Errorf(
			"%w: continuation access: %w",
			transporthttp.ErrInvalidRoute,
			serviceErr,
		)
	}
	runtime = service
	sseEncoder, err := transporthttp.NewSSEEncoder(transporthttp.SSEConfig{
		MaxEventBytes: options.eventBytes,
	})
	if err != nil {
		return fmt.Errorf("%w: continuation SSE: %w", transporthttp.ErrInvalidRoute, err)
	}
	streamEncoder := continuationSSEEncoder(sseEncoder)

	startOperation, err := newContinuationHTTPOperation(
		"Start",
		operation.KindUnary,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", transporthttp.ErrInvalidRoute, err)
	}
	attachOperation, err := newContinuationHTTPOperation(
		"Attach",
		operation.KindServerStream,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", transporthttp.ErrInvalidRoute, err)
	}
	signalOperation, err := newContinuationHTTPOperation(
		"Signal",
		operation.KindUnary,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", transporthttp.ErrInvalidRoute, err)
	}
	cancelOperation, err := newContinuationHTTPOperation(
		"Cancel",
		operation.KindUnary,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", transporthttp.ErrInvalidRoute, err)
	}

	if err := router.Handle(
		nethttp.MethodPost,
		ContinuationRoutePrefix,
		startOperation,
		decodeContinuationStart(),
		handleContinuationStart(runtime, options.inlineBudget),
		encodeContinuationStart,
	); err != nil {
		return err
	}
	if err := router.Handle(
		nethttp.MethodGet,
		continuationAttachPattern,
		attachOperation,
		decodeContinuationAttach,
		handleContinuationAttach(runtime, options),
		streamEncoder,
		transporthttp.WithStreaming(),
	); err != nil {
		return err
	}
	if err := router.HandleTemplate(
		nethttp.MethodPost,
		continuationSignalPattern,
		signalOperation,
		decodeContinuationSignal(),
		handleContinuationSignal(runtime),
		transporthttp.EncodeProto,
	); err != nil {
		return err
	}
	if err := router.Handle(
		nethttp.MethodDelete,
		continuationCancelPattern,
		cancelOperation,
		decodeContinuationCancel(),
		handleContinuationCancel(runtime),
		transporthttp.EncodeProto,
	); err != nil {
		return err
	}
	return nil
}

type continuationStartRequest struct {
	callID          continuation.CallID
	operation       continuation.Operation
	workflowVersion string
	input           []byte
}

type continuationCommandRequest struct {
	callID    continuation.CallID
	commandID string
	payload   []byte
}

type continuationAttachRequest struct {
	callID continuation.CallID
	after  uint64
}

type continuationStartResponse struct {
	status  int
	message *continuationv1.StartResponse
}

type continuationStreamResponse struct {
	stream transporthttp.ServerSentEvents
	cancel context.CancelFunc
}

func decodeContinuationStart() transporthttp.Decoder {
	decode := boundedContinuationDecoder(transporthttp.DecodeProto(
		func() *continuationv1.StartRequest {
			return &continuationv1.StartRequest{}
		},
		transporthttp.WithProtoBody(),
		transporthttp.WithProtoQueryDisabled(),
	))
	return func(request *nethttp.Request) (any, error) {
		decoded, err := decode(request)
		if err != nil {
			return nil, err
		}
		message, ok := decoded.(*continuationv1.StartRequest)
		if !ok {
			return nil, fmt.Errorf("continuation: invalid start request")
		}
		if len(message.GetInput()) > maxContinuationPayloadBytes {
			return nil, transporthttp.ErrRequestTooLarge
		}
		callID, err := newContinuationHTTPCallID(message.GetCallId())
		if err != nil {
			return nil, err
		}
		if len(message.GetOperation()) > maxContinuationOperationBytes {
			return nil, fmt.Errorf("continuation: operation is too long")
		}
		target, err := continuation.NewOperation(message.GetOperation())
		if err != nil {
			return nil, fmt.Errorf("continuation: invalid operation")
		}
		workflowVersion := message.GetWorkflowVersion()
		if workflowVersion != "" && !validContinuationHTTPIdentity(
			workflowVersion,
			maxContinuationWorkflowBytes,
		) {
			return nil, fmt.Errorf("continuation: invalid workflow version")
		}
		return continuationStartRequest{
			callID:          callID,
			operation:       target,
			workflowVersion: workflowVersion,
			input:           append([]byte(nil), message.GetInput()...),
		}, nil
	}
}

func decodeContinuationSignal() transporthttp.Decoder {
	decode := boundedContinuationDecoder(transporthttp.DecodeProto(
		func() *continuationv1.SignalRequest {
			return &continuationv1.SignalRequest{}
		},
		transporthttp.WithProtoBody(),
		transporthttp.WithProtoPathTemplate(continuationSignalPattern),
		transporthttp.WithProtoQueryDisabled(),
	))
	return func(request *nethttp.Request) (any, error) {
		decoded, err := decode(request)
		if err != nil {
			return nil, err
		}
		message, ok := decoded.(*continuationv1.SignalRequest)
		if !ok {
			return nil, fmt.Errorf("continuation: invalid signal request")
		}
		if len(message.GetPayload()) > maxContinuationPayloadBytes {
			return nil, transporthttp.ErrRequestTooLarge
		}
		callID, err := newContinuationHTTPCallID(message.GetCallId())
		if err != nil {
			return nil, err
		}
		if !validContinuationHTTPIdentity(
			message.GetCommandId(),
			maxContinuationCommandBytes,
		) {
			return nil, fmt.Errorf("continuation: invalid command ID")
		}
		return continuationCommandRequest{
			callID:    callID,
			commandID: message.GetCommandId(),
			payload:   append([]byte(nil), message.GetPayload()...),
		}, nil
	}
}

func decodeContinuationCancel() transporthttp.Decoder {
	decode := boundedContinuationDecoder(transporthttp.DecodeProto(
		func() *continuationv1.CancelRequest {
			return &continuationv1.CancelRequest{}
		},
		transporthttp.WithProtoBody(),
		transporthttp.WithProtoPathField("call_id", "call_id"),
		transporthttp.WithProtoQueryDisabled(),
	))
	return func(request *nethttp.Request) (any, error) {
		decoded, err := decode(request)
		if err != nil {
			return nil, err
		}
		message, ok := decoded.(*continuationv1.CancelRequest)
		if !ok {
			return nil, fmt.Errorf("continuation: invalid cancel request")
		}
		callID, err := newContinuationHTTPCallID(message.GetCallId())
		if err != nil {
			return nil, err
		}
		if !validContinuationHTTPIdentity(
			message.GetCommandId(),
			maxContinuationCommandBytes,
		) {
			return nil, fmt.Errorf("continuation: invalid command ID")
		}
		return continuationCommandRequest{
			callID:    callID,
			commandID: message.GetCommandId(),
		}, nil
	}
}

func decodeContinuationAttach(request *nethttp.Request) (any, error) {
	if request == nil ||
		request.URL == nil ||
		request.URL.RawQuery != "" {
		return nil, fmt.Errorf("continuation: invalid Attach request")
	}
	callID, err := newContinuationHTTPCallID(request.PathValue("call_id"))
	if err != nil {
		return nil, err
	}
	decoded, err := transporthttp.DecodeSSERequest(request)
	if err != nil {
		return nil, err
	}
	sseRequest, ok := decoded.(transporthttp.SSERequest)
	if !ok {
		return nil, fmt.Errorf("continuation: invalid SSE request")
	}
	after := uint64(0)
	if sseRequest.LastEventID() != "" {
		after, err = strconv.ParseUint(sseRequest.LastEventID(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf(
				"continuation: Last-Event-ID is not a sequence",
			)
		}
	}
	return continuationAttachRequest{callID: callID, after: after}, nil
}

func boundedContinuationDecoder(decoder transporthttp.Decoder) transporthttp.Decoder {
	return func(request *nethttp.Request) (any, error) {
		if request == nil || request.Body == nil || decoder == nil {
			return nil, fmt.Errorf("continuation: invalid request body")
		}
		if request.ContentLength > maxContinuationRequestBytes {
			return nil, transporthttp.ErrRequestTooLarge
		}
		request.Body = nethttp.MaxBytesReader(
			nil,
			request.Body,
			maxContinuationRequestBytes,
		)
		return decoder(request)
	}
}

func handleContinuationStart(
	runtime ContinuationRuntime,
	inlineBudget continuation.InlineBudget,
) middleware.Handler {
	return func(
		ctx context.Context,
		request any,
	) (any, error) {
		decoded, ok := request.(continuationStartRequest)
		if !ok {
			return nil, continuationRequestError()
		}
		var snapshot continuation.Snapshot
		var err error
		if decoded.workflowVersion != "" {
			workflow, ok := runtime.(continuationWorkflowRuntime)
			if !ok {
				return nil, continuationRequestError()
			}
			snapshot, err = workflow.StartWorkflow(
				ctx,
				decoded.callID,
				decoded.operation,
				decoded.workflowVersion,
				decoded.input,
			)
		} else if inline, ok := runtime.(ContinuationInlineRuntime); ok {
			snapshot, err = inline.StartCallInline(
				ctx,
				decoded.callID,
				decoded.operation,
				decoded.input,
				inlineBudget,
			)
		} else {
			snapshot, err = runtime.StartCall(
				ctx,
				decoded.callID,
				decoded.operation,
				decoded.input,
			)
		}
		if err != nil {
			return nil, continuationHTTPError(err)
		}
		status := nethttp.StatusAccepted
		if snapshot.Status().Terminal() {
			status = nethttp.StatusOK
		}
		return continuationStartResponse{
			status: status,
			message: &continuationv1.StartResponse{
				Snapshot:      continuationProtoSnapshot(snapshot),
				TerminalFrame: continuationProtoTerminalFrame(snapshot),
			},
		}, nil
	}
}

func encodeContinuationStart(
	ctx context.Context,
	writer nethttp.ResponseWriter,
	response any,
) error {
	encoded, ok := response.(continuationStartResponse)
	if !ok || encoded.message == nil {
		return fmt.Errorf("continuation: invalid Start response")
	}
	writer.WriteHeader(encoded.status)
	return transporthttp.EncodeProto(ctx, writer, encoded.message)
}

func handleContinuationSignal(
	runtime ContinuationRuntime,
) middleware.Handler {
	return func(
		ctx context.Context,
		request any,
	) (any, error) {
		decoded, ok := request.(continuationCommandRequest)
		if !ok {
			return nil, continuationRequestError()
		}
		snapshot, err := runtime.SubmitSignal(
			ctx,
			decoded.callID,
			decoded.commandID,
			decoded.payload,
		)
		if err != nil {
			return nil, continuationHTTPError(err)
		}
		return &continuationv1.SignalResponse{
			Snapshot: continuationProtoSnapshot(snapshot),
		}, nil
	}
}

func handleContinuationCancel(
	runtime ContinuationRuntime,
) middleware.Handler {
	return func(
		ctx context.Context,
		request any,
	) (any, error) {
		decoded, ok := request.(continuationCommandRequest)
		if !ok {
			return nil, continuationRequestError()
		}
		snapshot, err := runtime.RequestCancel(
			ctx,
			decoded.callID,
			decoded.commandID,
		)
		if err != nil {
			return nil, continuationHTTPError(err)
		}
		return &continuationv1.CancelResponse{
			Snapshot: continuationProtoSnapshot(snapshot),
		}, nil
	}
}

func handleContinuationAttach(
	runtime ContinuationRuntime,
	options continuationRouteOptions,
) middleware.Handler {
	return func(
		ctx context.Context,
		request any,
	) (any, error) {
		decoded, ok := request.(continuationAttachRequest)
		if !ok {
			return nil, continuationRequestError()
		}
		initial, err := runtime.Attach(
			ctx,
			decoded.callID,
			decoded.after,
			options.pageSize,
		)
		if err != nil {
			return nil, continuationHTTPError(err)
		}
		streamContext, cancel := context.WithCancel(ctx)
		events := make(chan transporthttp.SSEEvent)
		failures := make(chan error, 1)
		stream, err := transporthttp.NewServerSentEvents(events, failures)
		if err != nil {
			cancel()
			return nil, err
		}
		go pumpContinuationEvents(
			streamContext,
			runtime,
			decoded.callID,
			decoded.after,
			options,
			initial,
			events,
			failures,
		)
		return continuationStreamResponse{
			stream: stream,
			cancel: cancel,
		}, nil
	}
}

func continuationSSEEncoder(encoder transporthttp.Encoder) transporthttp.Encoder {
	return func(
		ctx context.Context,
		writer nethttp.ResponseWriter,
		response any,
	) error {
		stream, ok := response.(continuationStreamResponse)
		if !ok || stream.cancel == nil || encoder == nil {
			return fmt.Errorf("continuation: invalid Attach response")
		}
		defer stream.cancel()
		return encoder(ctx, writer, stream.stream)
	}
}

func pumpContinuationEvents(
	ctx context.Context,
	runtime ContinuationRuntime,
	callID continuation.CallID,
	after uint64,
	options continuationRouteOptions,
	initial continuation.Attachment,
	events chan<- transporthttp.SSEEvent,
	failures chan<- error,
) {
	defer close(events)
	defer close(failures)
	attachment := initial
	for {
		if err := validateContinuationAttachment(
			attachment,
			after,
			options.pageSize,
		); err != nil {
			sendContinuationFailure(ctx, failures, err)
			return
		}
		for _, frame := range attachment.Frames {
			event, err := continuationFrameEvent(frame, attachment.Snapshot)
			if err != nil {
				sendContinuationFailure(ctx, failures, err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case events <- event:
			}
			after = frame.Sequence()
		}
		if attachment.Terminal &&
			after == attachment.Snapshot.Sequence() {
			return
		}
		if after < attachment.Snapshot.Sequence() {
			var err error
			attachment, err = runtime.Attach(
				ctx,
				callID,
				after,
				options.pageSize,
			)
			if err != nil {
				if context.Cause(ctx) == nil {
					sendContinuationFailure(
						ctx,
						failures,
						continuationHTTPError(err),
					)
				}
				return
			}
			continue
		}

		timer := time.NewTimer(options.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		var err error
		attachment, err = runtime.Attach(
			ctx,
			callID,
			after,
			options.pageSize,
		)
		if err != nil {
			if context.Cause(ctx) == nil {
				sendContinuationFailure(
					ctx,
					failures,
					continuationHTTPError(err),
				)
			}
			return
		}
	}
}

func validateContinuationAttachment(
	attachment continuation.Attachment,
	after uint64,
	limit int,
) error {
	if len(attachment.Frames) > limit ||
		attachment.Snapshot.Sequence() < after {
		return fmt.Errorf("continuation: invalid Attach page")
	}
	expected := after + 1
	for _, frame := range attachment.Frames {
		if frame.Sequence() != expected {
			return fmt.Errorf("continuation: non-contiguous Attach page")
		}
		expected++
	}
	if len(attachment.Frames) == 0 &&
		attachment.Snapshot.Sequence() > after {
		return fmt.Errorf("continuation: empty Attach page before cursor")
	}
	return nil
}

func sendContinuationFailure(
	ctx context.Context,
	failures chan<- error,
	err error,
) {
	select {
	case <-ctx.Done():
	case failures <- err:
	}
}

func continuationFrameEvent(
	frame continuation.Frame,
	snapshot continuation.Snapshot,
) (transporthttp.SSEEvent, error) {
	return transporthttp.NewSSEProtoEvent(
		strconv.FormatUint(frame.Sequence(), 10),
		continuationEvent,
		&continuationv1.AttachResponse{
			Page: &continuationv1.Page{
				Snapshot: continuationProtoSnapshot(snapshot),
				Frames: []*continuationv1.Frame{{
					Sequence: frame.Sequence(),
					Kind:     continuationProtoFrameKind(frame.Kind()),
					Payload:  frame.Payload(),
				}},
				NextSequence: frame.Sequence() + 1,
				FrameFloor:   snapshot.FrameFloor(),
				Terminal:     snapshot.Status().Terminal(),
			},
		},
		0,
	)
}

func continuationProtoSnapshot(
	snapshot continuation.Snapshot,
) *continuationv1.Snapshot {
	return &continuationv1.Snapshot{
		CallId:     snapshot.CallID().String(),
		Operation:  snapshot.Operation().String(),
		Status:     continuationProtoStatus(snapshot.Status()),
		Revision:   snapshot.Revision(),
		Fence:      snapshot.Fence(),
		Sequence:   snapshot.Sequence(),
		FrameFloor: snapshot.FrameFloor(),
		Terminal:   snapshot.Status().Terminal(),
		ReadyAt:    continuationReadyAt(snapshot.ReadyAt()),
		Workflow:   continuationProtoWorkflow(snapshot),
	}
}

func continuationReadyAt(readyAt time.Time) string {
	if readyAt.IsZero() {
		return ""
	}
	return readyAt.UTC().Format(time.RFC3339Nano)
}

func continuationProtoWorkflow(
	snapshot continuation.Snapshot,
) *continuationv1.Workflow {
	workflow, ok := snapshot.Workflow()
	if !ok {
		return nil
	}
	nodes := workflow.Nodes()
	encoded := &continuationv1.Workflow{
		Version:     workflow.Version(),
		Fingerprint: workflow.Fingerprint(),
		StartedAt:   workflow.StartedAt().UTC().Format(time.RFC3339Nano),
		Nodes:       make([]*continuationv1.WorkflowNode, len(nodes)),
	}
	for index, node := range nodes {
		encoded.Nodes[index] = &continuationv1.WorkflowNode{
			Id:           node.ID(),
			Kind:         continuationProtoWorkflowNodeKind(node.Kind()),
			Status:       continuationProtoWorkflowNodeStatus(node.Status()),
			Attempt:      node.Attempt(),
			ChildCallId:  node.ChildCallID().String(),
			ReadyAt:      continuationReadyAt(node.ReadyAt()),
			FailureClass: node.FailureClass(),
		}
	}
	return encoded
}

func continuationProtoWorkflowNodeKind(
	kind continuation.WorkflowNodeKind,
) continuationv1.WorkflowNodeKind {
	switch kind {
	case continuation.WorkflowNodeMachine:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_MACHINE
	case continuation.WorkflowNodeTimer:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_TIMER
	case continuation.WorkflowNodeChild:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_CHILD
	case continuation.WorkflowNodeJoin:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_JOIN
	default:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_UNSPECIFIED
	}
}

func continuationProtoWorkflowNodeStatus(
	status continuation.WorkflowNodeStatus,
) continuationv1.WorkflowNodeStatus {
	switch status {
	case continuation.WorkflowNodePending:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_PENDING
	case continuation.WorkflowNodeRunning:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_RUNNING
	case continuation.WorkflowNodeWaiting:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_WAITING
	case continuation.WorkflowNodeSucceeded:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_SUCCEEDED
	case continuation.WorkflowNodeFailed:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_FAILED
	case continuation.WorkflowNodeSkipped:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_SKIPPED
	default:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_UNSPECIFIED
	}
}

func continuationProtoTerminalFrame(
	snapshot continuation.Snapshot,
) *continuationv1.Frame {
	if !snapshot.Status().Terminal() {
		return nil
	}
	frames := snapshot.Frames()
	if len(frames) == 0 {
		return nil
	}
	frame := frames[len(frames)-1]
	var expected continuation.FrameKind
	switch snapshot.Status() {
	case continuation.StatusCompleted:
		expected = continuation.FrameCompleted
	case continuation.StatusFailed:
		expected = continuation.FrameFailed
	case continuation.StatusCanceled:
		expected = continuation.FrameCanceled
	case continuation.StatusExpired:
		expected = continuation.FrameExpired
	default:
		return nil
	}
	if frame.Kind() != expected ||
		frame.Sequence() != snapshot.Sequence() {
		return nil
	}
	return &continuationv1.Frame{
		Sequence: frame.Sequence(),
		Kind:     continuationProtoFrameKind(frame.Kind()),
		Payload:  frame.Payload(),
	}
}

func continuationProtoStatus(
	status continuation.Status,
) continuationv1.CallStatus {
	switch status {
	case continuation.StatusAccepted:
		return continuationv1.CallStatus_CALL_STATUS_ACCEPTED
	case continuation.StatusRunning:
		return continuationv1.CallStatus_CALL_STATUS_RUNNING
	case continuation.StatusWaiting:
		return continuationv1.CallStatus_CALL_STATUS_WAITING
	case continuation.StatusSuspended:
		return continuationv1.CallStatus_CALL_STATUS_SUSPENDED
	case continuation.StatusCancelRequested:
		return continuationv1.CallStatus_CALL_STATUS_CANCEL_REQUESTED
	case continuation.StatusCompleted:
		return continuationv1.CallStatus_CALL_STATUS_COMPLETED
	case continuation.StatusFailed:
		return continuationv1.CallStatus_CALL_STATUS_FAILED
	case continuation.StatusCanceled:
		return continuationv1.CallStatus_CALL_STATUS_CANCELED
	case continuation.StatusExpired:
		return continuationv1.CallStatus_CALL_STATUS_EXPIRED
	default:
		return continuationv1.CallStatus_CALL_STATUS_UNSPECIFIED
	}
}

func continuationProtoFrameKind(
	kind continuation.FrameKind,
) continuationv1.FrameKind {
	switch kind {
	case continuation.FrameAccepted:
		return continuationv1.FrameKind_FRAME_KIND_ACCEPTED
	case continuation.FrameEvent:
		return continuationv1.FrameKind_FRAME_KIND_EVENT
	case continuation.FrameWaiting:
		return continuationv1.FrameKind_FRAME_KIND_WAITING
	case continuation.FrameSignal:
		return continuationv1.FrameKind_FRAME_KIND_SIGNAL
	case continuation.FrameSuspended:
		return continuationv1.FrameKind_FRAME_KIND_SUSPENDED
	case continuation.FrameCancelRequested:
		return continuationv1.FrameKind_FRAME_KIND_CANCEL_REQUESTED
	case continuation.FrameCompleted:
		return continuationv1.FrameKind_FRAME_KIND_COMPLETED
	case continuation.FrameFailed:
		return continuationv1.FrameKind_FRAME_KIND_FAILED
	case continuation.FrameCanceled:
		return continuationv1.FrameKind_FRAME_KIND_CANCELED
	case continuation.FrameExpired:
		return continuationv1.FrameKind_FRAME_KIND_EXPIRED
	case continuation.FrameWorkflowChild:
		return continuationv1.FrameKind_FRAME_KIND_WORKFLOW_CHILD
	default:
		return continuationv1.FrameKind_FRAME_KIND_UNSPECIFIED
	}
}

func newContinuationHTTPCallID(
	value string,
) (continuation.CallID, error) {
	if !validContinuationHTTPIdentity(value, maxContinuationCallIDBytes) {
		return continuation.CallID{}, fmt.Errorf("continuation: invalid call ID")
	}
	callID, err := continuation.NewCallID(value)
	if err != nil {
		return continuation.CallID{}, fmt.Errorf("continuation: invalid call ID")
	}
	return callID, nil
}

func validContinuationHTTPIdentity(value string, maximum int) bool {
	if value == "" ||
		len(value) > maximum ||
		strings.TrimSpace(value) != value {
		return false
	}
	for index := range len(value) {
		ch := value[index]
		if ch >= 'a' && ch <= 'z' ||
			ch >= 'A' && ch <= 'Z' ||
			ch >= '0' && ch <= '9' ||
			ch == '-' ||
			ch == '.' ||
			ch == '_' ||
			ch == '~' {
			continue
		}
		return false
	}
	return true
}

func newContinuationHTTPOperation(
	method string,
	kind operation.Kind,
) (operation.Operation, error) {
	return operation.New("http", continuationService, method, kind)
}

func continuationRequestError() error {
	return kerrors.New(
		nethttp.StatusBadRequest,
		"INVALID_CONTINUATION_REQUEST",
		"continuation request is invalid",
	)
}

func continuationHTTPError(err error) error {
	switch {
	case errors.Is(err, continuation.ErrAuthenticationRequired):
		return kerrors.Wrap(
			err,
			nethttp.StatusUnauthorized,
			"CONTINUATION_AUTHENTICATION_REQUIRED",
			"continuation authentication is required",
		)
	case errors.Is(err, continuation.ErrAccessDenied):
		return kerrors.Wrap(
			err,
			nethttp.StatusForbidden,
			"CONTINUATION_ACCESS_DENIED",
			"continuation access is denied",
		)
	case errors.Is(err, continuation.ErrAuthorizationFailed):
		return kerrors.Wrap(
			err,
			nethttp.StatusInternalServerError,
			"CONTINUATION_AUTHORIZATION_FAILED",
			"continuation authorization failed",
		)
	case errors.Is(err, continuation.ErrNotFound):
		return kerrors.Wrap(
			err,
			nethttp.StatusNotFound,
			"CONTINUATION_NOT_FOUND",
			"continuation call was not found",
		)
	case errors.Is(err, continuation.ErrCursorAhead):
		return kerrors.Wrap(
			err,
			nethttp.StatusConflict,
			"CURSOR_AHEAD",
			"continuation cursor is ahead of durable state",
		)
	case errors.Is(err, continuation.ErrGap):
		return kerrors.Wrap(
			err,
			nethttp.StatusGone,
			"RETENTION_GAP",
			"continuation frames are no longer retained",
		)
	case errors.Is(err, continuation.ErrTimerNotReady):
		return kerrors.Wrap(
			err,
			nethttp.StatusConflict,
			"TIMER_NOT_READY",
			"continuation timer is not ready",
		)
	case errors.Is(err, continuation.ErrHistoryBudget):
		return kerrors.Wrap(
			err,
			nethttp.StatusRequestEntityTooLarge,
			"HISTORY_BUDGET_EXCEEDED",
			"continuation history exceeds its payload budget",
		)
	case errors.Is(err, continuation.ErrAlreadyExists),
		errors.Is(err, continuation.ErrConflict),
		errors.Is(err, continuation.ErrCommandConflict),
		errors.Is(err, continuation.ErrTransition),
		errors.Is(err, continuation.ErrTerminal),
		errors.Is(err, continuation.ErrLeaseHeld),
		errors.Is(err, continuation.ErrLeaseLost),
		errors.Is(err, continuation.ErrStaleFence),
		errors.Is(err, continuation.ErrNotReady):
		return kerrors.Wrap(
			err,
			nethttp.StatusConflict,
			"CONTINUATION_CONFLICT",
			"continuation state conflicts with the request",
		)
	case errors.Is(err, continuation.ErrInvalidIdentity),
		errors.Is(err, continuation.ErrInvalidFrame),
		errors.Is(err, continuation.ErrInvalidRuntime),
		errors.Is(err, continuation.ErrInvalidAttach),
		errors.Is(err, continuation.ErrInvalidStore),
		errors.Is(err, continuation.ErrMachineNotFound),
		errors.Is(err, continuation.ErrInvalidWorkflow),
		errors.Is(err, continuation.ErrWorkflowCycle),
		errors.Is(err, continuation.ErrInvalidHistory):
		return kerrors.Wrap(
			err,
			nethttp.StatusBadRequest,
			"INVALID_CONTINUATION_REQUEST",
			"continuation request is invalid",
		)
	default:
		return err
	}
}

func isNilContinuationValue(value any) bool {
	if value == nil {
		return true
	}
	reflection := reflect.ValueOf(value)
	switch reflection.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflection.IsNil()
	default:
		return false
	}
}

func isNilContinuationRuntime(runtime ContinuationRuntime) bool {
	if runtime == nil {
		return true
	}
	value := reflect.ValueOf(runtime)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
