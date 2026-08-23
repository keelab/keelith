package projectiongrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"

	projectionv1 "github.com/keelab/keelith/api/projection/v1"
	"github.com/keelab/keelith/programmable/projection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const defaultClientMaxFrameBytes = maxWireFrameBytes

var (
	// ErrInvalidClient reports a nil protocol client or unsafe byte budget.
	ErrInvalidClient = errors.New(
		"projection grpc transport: invalid client",
	)
	// ErrSessionClosed reports Next after a local session Close.
	ErrSessionClosed = errors.New(
		"projection grpc transport: session closed",
	)
)

// ClientOption configures the remote projection Source.
type ClientOption interface {
	applyClient(*clientOptions) error
}

type clientOptionFunc func(*clientOptions) error

func (function clientOptionFunc) applyClient(options *clientOptions) error {
	return function(options)
}

type clientOptions struct {
	maxFrameBytes int
	callOptions   []grpc.CallOption
}

// WithClientMaxFrameBytes sets the decoded per-frame byte budget.
func WithClientMaxFrameBytes(maximum int) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maximum < 256 || maximum > maxWireFrameBytes {
			return ErrInvalidClient
		}
		options.maxFrameBytes = maximum
		return nil
	})
}

// WithCallOptions snapshots gRPC options used for each new subscription.
func WithCallOptions(callOptions ...grpc.CallOption) ClientOption {
	snapshot := append([]grpc.CallOption(nil), callOptions...)
	return clientOptionFunc(func(options *clientOptions) error {
		for _, option := range snapshot {
			if isNilValue(option) {
				return ErrInvalidClient
			}
		}
		options.callOptions = snapshot
		return nil
	})
}

// Source implements projection.Source over ProjectionService.
type Source struct {
	client        projectionv1.ProjectionServiceClient
	maxFrameBytes int
	callOptions   []grpc.CallOption
}

var _ projection.Source = (*Source)(nil)

// NewSource validates and constructs one remote projection Source.
func NewSource(
	client projectionv1.ProjectionServiceClient,
	optionList ...ClientOption,
) (*Source, error) {
	if isNilProjectionClient(client) {
		return nil, ErrInvalidClient
	}
	options := clientOptions{maxFrameBytes: defaultClientMaxFrameBytes}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidClient,
				index,
			)
		}
		if err := option.applyClient(&options); err != nil {
			return nil, err
		}
	}
	return &Source{
		client:        client,
		maxFrameBytes: options.maxFrameBytes,
		callOptions:   append([]grpc.CallOption(nil), options.callOptions...),
	}, nil
}

// Open starts one cancelable server stream from a durable checkpoint.
func (source *Source) Open(
	ctx context.Context,
	request projection.SubscribeRequest,
) (projection.Session, error) {
	if source == nil || ctx == nil {
		return nil, ErrInvalidClient
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	sessionContext, cancel := context.WithCancelCause(ctx)
	stream, err := source.client.Subscribe(
		sessionContext,
		requestToWire(request),
		source.callOptions...,
	)
	if err != nil {
		cancel(err)
		return nil, err
	}
	if isNilValue(stream) {
		cancel(ErrInvalidClient)
		return nil, ErrInvalidClient
	}
	return &clientSession{
		stream:        stream,
		cancel:        cancel,
		maxFrameBytes: source.maxFrameBytes,
	}, nil
}

type receiveResult struct {
	frame *projectionv1.SubscribeResponse
	err   error
}

type clientSession struct {
	stream grpc.ServerStreamingClient[projectionv1.SubscribeResponse]
	cancel context.CancelCauseFunc

	maxFrameBytes int
	receiveMu     sync.Mutex
	stateMu       sync.Mutex
	closed        bool
}

// Next receives and validates one bounded wire frame.
func (session *clientSession) Next(
	ctx context.Context,
) (projection.Frame, error) {
	if session == nil || ctx == nil {
		return nil, ErrInvalidClient
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	session.receiveMu.Lock()
	defer session.receiveMu.Unlock()
	if session.isClosed() {
		return nil, ErrSessionClosed
	}

	result := make(chan receiveResult, 1)
	go func() {
		frame, err := session.stream.Recv()
		result <- receiveResult{frame: frame, err: err}
	}()
	select {
	case received := <-result:
		if received.err != nil {
			if session.isClosed() &&
				(status.Code(received.err) == codes.Canceled ||
					errors.Is(received.err, context.Canceled)) {
				return nil, ErrSessionClosed
			}
			if errors.Is(received.err, io.EOF) {
				return nil, io.EOF
			}
			return nil, received.err
		}
		if received.frame == nil {
			return nil, ErrInvalidWireFrame
		}
		if proto.Size(received.frame) > session.maxFrameBytes {
			return nil, ErrFrameTooLarge
		}
		frame, err := frameFromWire(received.frame)
		if err != nil {
			return nil, err
		}
		return frame, nil
	case <-ctx.Done():
		session.cancel(context.Cause(ctx))
		return nil, context.Cause(ctx)
	}
}

// Close cancels the stream and unblocks a concurrent Next.
func (session *clientSession) Close() error {
	if session == nil {
		return nil
	}
	session.stateMu.Lock()
	if session.closed {
		session.stateMu.Unlock()
		return nil
	}
	session.closed = true
	session.stateMu.Unlock()
	session.cancel(ErrSessionClosed)
	return nil
}

func (session *clientSession) isClosed() bool {
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	return session.closed
}

func isNilProjectionClient(
	client projectionv1.ProjectionServiceClient,
) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
