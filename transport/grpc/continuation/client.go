package continuationgrpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	continuationv1 "github.com/keelab/keelith/api/continuation/v1"
	"github.com/keelab/keelith/programmable/continuation"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const defaultClientMaxResponseBytes = 4 * 1024 * 1024

var (
	// ErrInvalidClient reports an invalid protocol client or option.
	ErrInvalidClient = errors.New(
		"continuation grpc transport: invalid client",
	)
)

// ClientOption configures validated continuation v1 calls.
type ClientOption interface {
	applyClient(*clientOptions) error
}

type clientOptionFunc func(*clientOptions) error

func (function clientOptionFunc) applyClient(options *clientOptions) error {
	return function(options)
}

type clientOptions struct {
	maxResponseBytes int
	callOptions      []grpc.CallOption
}

// WithClientMaxResponseBytes sets the fully encoded inbound message budget.
func WithClientMaxResponseBytes(maximum int) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maximum < 256 || maximum > maxWireMessageBytes {
			return ErrInvalidClient
		}
		options.maxResponseBytes = maximum
		return nil
	})
}

// WithClientCallOptions snapshots gRPC options applied to every RPC.
func WithClientCallOptions(callOptions ...grpc.CallOption) ClientOption {
	snapshot := append([]grpc.CallOption(nil), callOptions...)
	return clientOptionFunc(func(options *clientOptions) error {
		for _, option := range snapshot {
			if isNilClientValue(option) {
				return ErrInvalidClient
			}
		}
		options.callOptions = append([]grpc.CallOption(nil), snapshot...)
		return nil
	})
}

// Client validates the complete ContinuationService protocol surface.
type Client struct {
	client           continuationv1.ContinuationServiceClient
	maxResponseBytes int
	callOptions      []grpc.CallOption
}

// NewClient validates and snapshots one generated protocol client.
func NewClient(
	client continuationv1.ContinuationServiceClient,
	optionList ...ClientOption,
) (*Client, error) {
	if isNilClientValue(client) {
		return nil, ErrInvalidClient
	}
	options := clientOptions{
		maxResponseBytes: defaultClientMaxResponseBytes,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidClient,
				index,
			)
		}
		if err := option.applyClient(&options); err != nil {
			return nil, fmt.Errorf(
				"%w: option %d",
				ErrInvalidClient,
				index,
			)
		}
	}
	return &Client{
		client:           client,
		maxResponseBytes: options.maxResponseBytes,
		callOptions:      append([]grpc.CallOption(nil), options.callOptions...),
	}, nil
}

// Start durably creates one call and validates its snapshot identity.
func (client *Client) Start(
	ctx context.Context,
	callID continuation.CallID,
	target continuation.Operation,
	input []byte,
) (*continuationv1.StartResponse, error) {
	if err := client.validateCall(ctx, callID); err != nil ||
		target.String() == "" {
		return nil, ErrInvalidClient
	}
	if len(input) > maxWirePayloadBytes {
		return nil, ErrWireMessageTooLarge
	}
	response, err := client.client.Start(
		ctx,
		&continuationv1.StartRequest{
			CallId:    callID.String(),
			Operation: target.String(),
			Input:     append([]byte(nil), input...),
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if err := client.validateResponse(response); err != nil {
		return nil, err
	}
	if err := validateStartResponse(response, callID, target); err != nil {
		return nil, err
	}
	return response, nil
}

// StartWorkflow durably creates one exact immutable workflow definition.
func (client *Client) StartWorkflow(
	ctx context.Context,
	callID continuation.CallID,
	target continuation.Operation,
	version string,
	input []byte,
) (*continuationv1.StartResponse, error) {
	if err := client.validateCall(ctx, callID); err != nil ||
		target.String() == "" || !validCommandID(version) {
		return nil, ErrInvalidClient
	}
	if len(input) > maxWirePayloadBytes {
		return nil, ErrWireMessageTooLarge
	}
	response, err := client.client.Start(
		ctx,
		&continuationv1.StartRequest{
			CallId:          callID.String(),
			Operation:       target.String(),
			Input:           append([]byte(nil), input...),
			WorkflowVersion: version,
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if err := client.validateResponse(response); err != nil {
		return nil, err
	}
	if err := validateStartResponse(response, callID, target); err != nil ||
		response.GetSnapshot().GetWorkflow() == nil ||
		response.GetSnapshot().GetWorkflow().GetVersion() != version {
		return nil, ErrInvalidWireMessage
	}
	return response, nil
}

// Poll returns one validated live page after an exclusive sequence.
func (client *Client) Poll(
	ctx context.Context,
	callID continuation.CallID,
	after uint64,
	limit int,
) (*continuationv1.PollResponse, error) {
	if err := client.validatePageCall(ctx, callID, limit); err != nil {
		return nil, err
	}
	response, err := client.client.Poll(
		ctx,
		&continuationv1.PollRequest{
			CallId: callID.String(),
			After:  after,
			Limit:  uint32(limit),
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if err := client.validateResponse(response); err != nil {
		return nil, err
	}
	if err := validatePageWire(response.GetPage(), callID, after, limit); err != nil {
		return nil, err
	}
	return response, nil
}

// Attach opens one validated server stream after an exclusive sequence.
func (client *Client) Attach(
	ctx context.Context,
	callID continuation.CallID,
	after uint64,
	limit int,
) (*AttachmentStream, error) {
	if err := client.validatePageCall(ctx, callID, limit); err != nil {
		return nil, err
	}
	stream, err := client.client.Attach(
		ctx,
		&continuationv1.AttachRequest{
			CallId: callID.String(),
			After:  after,
			Limit:  uint32(limit),
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if isNilClientValue(stream) {
		return nil, ErrInvalidClient
	}
	return &AttachmentStream{
		stream:           stream,
		callID:           callID,
		after:            after,
		limit:            limit,
		maxResponseBytes: client.maxResponseBytes,
	}, nil
}

// Signal submits one idempotent signal and validates the returned identity.
func (client *Client) Signal(
	ctx context.Context,
	callID continuation.CallID,
	commandID string,
	payload []byte,
) (*continuationv1.SignalResponse, error) {
	if err := client.validateCall(ctx, callID); err != nil ||
		!validCommandID(commandID) {
		return nil, ErrInvalidClient
	}
	if len(payload) > maxWirePayloadBytes {
		return nil, ErrWireMessageTooLarge
	}
	response, err := client.client.Signal(
		ctx,
		&continuationv1.SignalRequest{
			CallId:    callID.String(),
			CommandId: commandID,
			Payload:   append([]byte(nil), payload...),
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if err := client.validateSnapshotResponse(
		response,
		response.GetSnapshot(),
		callID,
	); err != nil {
		return nil, err
	}
	return response, nil
}

// Cancel submits one idempotent cancellation command.
func (client *Client) Cancel(
	ctx context.Context,
	callID continuation.CallID,
	commandID string,
) (*continuationv1.CancelResponse, error) {
	if err := client.validateCall(ctx, callID); err != nil ||
		!validCommandID(commandID) {
		return nil, ErrInvalidClient
	}
	response, err := client.client.Cancel(
		ctx,
		&continuationv1.CancelRequest{
			CallId:    callID.String(),
			CommandId: commandID,
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if err := client.validateSnapshotResponse(
		response,
		response.GetSnapshot(),
		callID,
	); err != nil {
		return nil, err
	}
	return response, nil
}

// GetHistory returns one separately authorized historical page.
func (client *Client) GetHistory(
	ctx context.Context,
	callID continuation.CallID,
	after uint64,
	limit int,
) (*continuationv1.GetHistoryResponse, error) {
	if err := client.validatePageCall(ctx, callID, limit); err != nil {
		return nil, err
	}
	response, err := client.client.GetHistory(
		ctx,
		&continuationv1.GetHistoryRequest{
			CallId: callID.String(),
			After:  after,
			Limit:  uint32(limit),
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if err := client.validateResponse(response); err != nil {
		return nil, err
	}
	if err := validatePageWire(response.GetPage(), callID, after, limit); err != nil {
		return nil, err
	}
	return response, nil
}

// GetHistoryDetail returns one separately authorized payload-bearing page.
func (client *Client) GetHistoryDetail(
	ctx context.Context,
	callID continuation.CallID,
	after uint64,
	limit int,
	maxPayloadBytes int,
) (*continuationv1.GetHistoryResponse, error) {
	if err := client.validatePageCall(ctx, callID, limit); err != nil ||
		maxPayloadBytes <= 0 || maxPayloadBytes > maxWireHistoryPayloadBytes {
		return nil, ErrInvalidClient
	}
	response, err := client.client.GetHistory(
		ctx,
		&continuationv1.GetHistoryRequest{
			CallId:          callID.String(),
			After:           after,
			Limit:           uint32(limit),
			IncludePayload:  true,
			MaxPayloadBytes: uint32(maxPayloadBytes),
		},
		client.callOptions...,
	)
	if err != nil {
		return nil, err
	}
	if err := client.validateResponse(response); err != nil {
		return nil, err
	}
	if err := validatePageWire(response.GetPage(), callID, after, limit); err != nil {
		return nil, err
	}
	return response, nil
}

// AttachmentStream validates strict cursor continuity for Attach responses.
type AttachmentStream struct {
	stream grpc.ServerStreamingClient[continuationv1.AttachResponse]

	mu               sync.Mutex
	callID           continuation.CallID
	after            uint64
	limit            int
	maxResponseBytes int
}

// Recv receives one bounded page and advances the exclusive cursor.
func (stream *AttachmentStream) Recv() (*continuationv1.AttachResponse, error) {
	if stream == nil || isNilClientValue(stream.stream) {
		return nil, ErrInvalidClient
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	response, err := stream.stream.Recv()
	if err != nil {
		return nil, err
	}
	if response == nil || proto.Size(response) > stream.maxResponseBytes {
		return nil, ErrWireMessageTooLarge
	}
	if err := validatePageWire(
		response.GetPage(),
		stream.callID,
		stream.after,
		stream.limit,
	); err != nil {
		return nil, err
	}
	stream.after = response.GetPage().GetNextSequence() - 1
	return response, nil
}

func (client *Client) validateCall(
	ctx context.Context,
	callID continuation.CallID,
) error {
	if client == nil || ctx == nil || isNilClientValue(client.client) ||
		callID.String() == "" {
		return ErrInvalidClient
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}

func (client *Client) validatePageCall(
	ctx context.Context,
	callID continuation.CallID,
	limit int,
) error {
	if err := client.validateCall(ctx, callID); err != nil {
		return err
	}
	if limit < 1 || limit > maxWirePageFrames {
		return ErrInvalidClient
	}
	return nil
}

func (client *Client) validateResponse(response proto.Message) error {
	if response == nil {
		return ErrInvalidWireMessage
	}
	if proto.Size(response) > client.maxResponseBytes {
		return ErrWireMessageTooLarge
	}
	return nil
}

func (client *Client) validateSnapshotResponse(
	response proto.Message,
	snapshot *continuationv1.Snapshot,
	callID continuation.CallID,
) error {
	if err := client.validateResponse(response); err != nil {
		return err
	}
	if validateSnapshotWire(snapshot) != nil ||
		snapshot.GetCallId() != callID.String() {
		return ErrInvalidWireMessage
	}
	return nil
}

func isNilClientValue(value any) bool {
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
