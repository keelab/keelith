// Package continuationgrpc exposes the transport-neutral Continuation Service
// through a bounded, authenticated gRPC v1 protocol.
package continuationgrpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	continuationv1 "github.com/keelab/keelith/api/continuation/v1"
	"github.com/keelab/keelith/programmable/continuation"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	defaultServerMaxRequestBytes  = 1536 * 1024
	defaultServerMaxResponseBytes = 4 * 1024 * 1024
	defaultServerPollInterval     = 100 * time.Millisecond
	defaultServerInlineDuration   = 50 * time.Millisecond
	defaultServerInlineSteps      = 64
)

var (
	// ErrInvalidServer reports an invalid service, option, or registration.
	ErrInvalidServer = errors.New(
		"continuation grpc transport: invalid server",
	)
)

// ServerOption configures bounded server behavior.
type ServerOption interface {
	applyServer(*serverOptions) error
}

type serverOptionFunc func(*serverOptions) error

func (f serverOptionFunc) applyServer(options *serverOptions) error {
	return f(options)
}

type serverOptions struct {
	maxRequestBytes  int
	maxResponseBytes int
	pollInterval     time.Duration
	inlineBudget     continuation.InlineBudget
}

// WithServerMaxRequestBytes sets the fully decoded per-request budget.
func WithServerMaxRequestBytes(maximum int) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if maximum < 256 || maximum > maxWireMessageBytes {
			return ErrInvalidServer
		}
		options.maxRequestBytes = maximum
		return nil
	})
}

// WithServerMaxResponseBytes sets the fully encoded unary/stream message
// budget. Attach splits pages to keep each message within this boundary.
func WithServerMaxResponseBytes(maximum int) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if maximum < 256 || maximum > maxWireMessageBytes {
			return ErrInvalidServer
		}
		options.maxResponseBytes = maximum
		return nil
	})
}

// WithServerPollInterval sets idle Attach polling without changing cursor
// semantics.
func WithServerPollInterval(interval time.Duration) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if interval < time.Millisecond || interval > time.Minute {
			return ErrInvalidServer
		}
		options.pollInterval = interval
		return nil
	})
}

// WithServerInlineBudget bounds synchronous execution after Start persists.
func WithServerInlineBudget(budget continuation.InlineBudget) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if budget.Duration < time.Millisecond ||
			budget.Duration > 5*time.Second ||
			budget.MaxTransitions < 1 ||
			budget.MaxTransitions > 10_000 {
			return ErrInvalidServer
		}
		options.inlineBudget = budget
		return nil
	})
}

// Server adapts exactly one access-controlled Continuation Service.
type Server struct {
	continuationv1.UnimplementedContinuationServiceServer

	service          *continuation.Service
	maxRequestBytes  int
	maxResponseBytes int
	pollInterval     time.Duration
	inlineBudget     continuation.InlineBudget
	registered       bool
}

// NewServer validates and snapshots a fail-closed Continuation Service.
func NewServer(
	service *continuation.Service,
	optionList ...ServerOption,
) (*Server, error) {
	if service == nil {
		return nil, ErrInvalidServer
	}
	options := serverOptions{
		maxRequestBytes:  defaultServerMaxRequestBytes,
		maxResponseBytes: defaultServerMaxResponseBytes,
		pollInterval:     defaultServerPollInterval,
		inlineBudget: continuation.InlineBudget{
			Duration:       defaultServerInlineDuration,
			MaxTransitions: defaultServerInlineSteps,
		},
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidServer,
				index,
			)
		}
		if err := option.applyServer(&options); err != nil {
			return nil, fmt.Errorf(
				"%w: option %d",
				ErrInvalidServer,
				index,
			)
		}
	}
	return &Server{
		service:          service,
		maxRequestBytes:  options.maxRequestBytes,
		maxResponseBytes: options.maxResponseBytes,
		pollInterval:     options.pollInterval,
		inlineBudget:     options.inlineBudget,
	}, nil
}

// Register installs ContinuationService exactly once.
func (s *Server) Register(registrar grpc.ServiceRegistrar) error {
	if s == nil || registrar == nil || s.registered {
		return ErrInvalidServer
	}
	continuationv1.RegisterContinuationServiceServer(registrar, s)
	s.registered = true
	return nil
}

// Start persists and optionally completes one authorized call inline.
func (s *Server) Start(
	ctx context.Context,
	request *continuationv1.StartRequest,
) (*continuationv1.StartResponse, error) {
	if err := s.validateRequest(ctx, request); err != nil {
		return nil, err
	}
	callID, err := callIDFromWire(request.GetCallId())
	if err != nil {
		return nil, invalidRequestStatus("INVALID_CALL_ID")
	}
	target, err := operationFromWire(request.GetOperation())
	if err != nil {
		return nil, invalidRequestStatus("INVALID_OPERATION")
	}
	if len(request.GetInput()) > maxWirePayloadBytes {
		return nil, messageBudgetStatus()
	}
	var snapshot continuation.Snapshot
	if request.GetWorkflowVersion() == "" {
		snapshot, err = s.service.StartCallInline(
			ctx,
			callID,
			target,
			request.GetInput(),
			s.inlineBudget,
		)
	} else {
		version, versionErr := callIDFromWire(request.GetWorkflowVersion())
		if versionErr != nil {
			return nil, invalidRequestStatus("INVALID_WORKFLOW_VERSION")
		}
		snapshot, err = s.service.StartWorkflow(
			ctx,
			callID,
			target,
			version.String(),
			request.GetInput(),
		)
	}
	if err != nil {
		return nil, continuationStatus(err)
	}
	response, err := startResponseToWire(snapshot)
	if err != nil {
		return nil, invalidBackendStatus()
	}
	if err := s.validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

// Poll returns one authorized bounded live page.
func (s *Server) Poll(
	ctx context.Context,
	request *continuationv1.PollRequest,
) (*continuationv1.PollResponse, error) {
	if err := s.validateRequest(ctx, request); err != nil {
		return nil, err
	}
	callID, limit, err := parsePageRequest(
		request.GetCallId(),
		request.GetLimit(),
	)
	if err != nil {
		return nil, invalidRequestStatus("INVALID_POLL")
	}
	attachment, err := s.service.Attach(
		ctx,
		callID,
		request.GetAfter(),
		limit,
	)
	if err != nil {
		return nil, continuationStatus(err)
	}
	page, err := pageToWire(attachment, request.GetAfter(), limit)
	if err != nil {
		return nil, invalidBackendStatus()
	}
	response := &continuationv1.PollResponse{Page: page}
	if err := s.validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

// Attach follows authorized pages until terminal state or disconnect.
func (s *Server) Attach(
	request *continuationv1.AttachRequest,
	stream grpc.ServerStreamingServer[continuationv1.AttachResponse],
) error {
	if s == nil || stream == nil {
		return invalidBackendStatus()
	}
	if err := s.validateRequest(stream.Context(), request); err != nil {
		return err
	}
	callID, limit, err := parsePageRequest(
		request.GetCallId(),
		request.GetLimit(),
	)
	if err != nil {
		return invalidRequestStatus("INVALID_ATTACH")
	}
	after := request.GetAfter()
	for {
		attachment, attachErr := s.service.Attach(
			stream.Context(),
			callID,
			after,
			limit,
		)
		if attachErr != nil {
			return continuationStatus(attachErr)
		}
		page, encodeErr := pageToWire(attachment, after, limit)
		if encodeErr != nil {
			return invalidBackendStatus()
		}
		for _, response := range splitAttachPage(page) {
			if err := s.validateResponse(response); err != nil {
				return err
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		}
		if page.GetTerminal() {
			return nil
		}
		if len(page.GetFrames()) > 0 {
			after = page.GetNextSequence() - 1
			continue
		}
		timer := time.NewTimer(s.pollInterval)
		select {
		case <-stream.Context().Done():
			timer.Stop()
			return statusFromContext(stream.Context())
		case <-timer.C:
		}
	}
}

// Signal submits one authorized idempotent command.
func (s *Server) Signal(
	ctx context.Context,
	request *continuationv1.SignalRequest,
) (*continuationv1.SignalResponse, error) {
	if err := s.validateRequest(ctx, request); err != nil {
		return nil, err
	}
	callID, err := callIDFromWire(request.GetCallId())
	if err != nil || !validCommandID(request.GetCommandId()) {
		return nil, invalidRequestStatus("INVALID_SIGNAL")
	}
	if len(request.GetPayload()) > maxWirePayloadBytes {
		return nil, messageBudgetStatus()
	}
	snapshot, err := s.service.SubmitSignal(
		ctx,
		callID,
		request.GetCommandId(),
		request.GetPayload(),
	)
	if err != nil {
		return nil, continuationStatus(err)
	}
	encoded, err := snapshotToWire(snapshot)
	if err != nil {
		return nil, invalidBackendStatus()
	}
	response := &continuationv1.SignalResponse{Snapshot: encoded}
	if err := s.validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

// Cancel submits one authorized idempotent cancellation command.
func (s *Server) Cancel(
	ctx context.Context,
	request *continuationv1.CancelRequest,
) (*continuationv1.CancelResponse, error) {
	if err := s.validateRequest(ctx, request); err != nil {
		return nil, err
	}
	callID, err := callIDFromWire(request.GetCallId())
	if err != nil || !validCommandID(request.GetCommandId()) {
		return nil, invalidRequestStatus("INVALID_CANCEL")
	}
	snapshot, err := s.service.RequestCancel(
		ctx,
		callID,
		request.GetCommandId(),
	)
	if err != nil {
		return nil, continuationStatus(err)
	}
	encoded, err := snapshotToWire(snapshot)
	if err != nil {
		return nil, invalidBackendStatus()
	}
	response := &continuationv1.CancelResponse{Snapshot: encoded}
	if err := s.validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

// GetHistory returns one separately authorized bounded historical page.
func (s *Server) GetHistory(
	ctx context.Context,
	request *continuationv1.GetHistoryRequest,
) (*continuationv1.GetHistoryResponse, error) {
	if err := s.validateRequest(ctx, request); err != nil {
		return nil, err
	}
	callID, limit, err := parsePageRequest(
		request.GetCallId(),
		request.GetLimit(),
	)
	if err != nil {
		return nil, invalidRequestStatus("INVALID_HISTORY")
	}
	var attachment continuation.Attachment
	if request.GetIncludePayload() {
		attachment, err = s.service.HistoryDetail(
			ctx,
			callID,
			request.GetAfter(),
			limit,
			int(request.GetMaxPayloadBytes()),
		)
	} else {
		if request.GetMaxPayloadBytes() != 0 {
			return nil, invalidRequestStatus("INVALID_HISTORY_BUDGET")
		}
		attachment, err = s.service.History(
			ctx,
			callID,
			request.GetAfter(),
			limit,
		)
	}
	if err != nil {
		return nil, continuationStatus(err)
	}
	page, err := pageToWire(attachment, request.GetAfter(), limit)
	if err != nil {
		return nil, invalidBackendStatus()
	}
	response := &continuationv1.GetHistoryResponse{Page: page}
	if err := s.validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *Server) validateRequest(
	ctx context.Context,
	request proto.Message,
) error {
	if s == nil || ctx == nil || s.service == nil {
		return invalidBackendStatus()
	}
	if request == nil {
		return invalidRequestStatus("MISSING_REQUEST")
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuationStatus(cause)
	}
	if proto.Size(request) > s.maxRequestBytes {
		return messageBudgetStatus()
	}
	return nil
}

func (s *Server) validateResponse(response proto.Message) error {
	if response == nil {
		return invalidBackendStatus()
	}
	if proto.Size(response) > s.maxResponseBytes {
		return messageBudgetStatus()
	}
	return nil
}

func parsePageRequest(
	callIDValue string,
	limitValue uint32,
) (continuation.CallID, int, error) {
	callID, err := callIDFromWire(callIDValue)
	if err != nil || limitValue < 1 || limitValue > maxWirePageFrames {
		return continuation.CallID{}, 0, ErrInvalidWireMessage
	}
	return callID, int(limitValue), nil
}

func statusFromContext(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return continuationStatus(cause)
	}
	return continuationStatus(context.Canceled)
}
