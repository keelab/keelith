package hertz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/protocol"
	hsse "github.com/cloudwego/hertz/pkg/protocol/sse"
	"github.com/keelab/keelith/operation"
	khttp "github.com/keelab/keelith/transport/http"
	ksse "github.com/keelab/keelith/transport/sse"
	"google.golang.org/protobuf/proto"
)

var errSSEClientClosed = fmt.Errorf(
	"hertz profile: SSE client closed: %w",
	context.Canceled,
)

// SSEClientOption configures one native Hertz SSE subscription.
type SSEClientOption interface {
	applySSEClient(*sseClientOptions) error
}

type sseClientOptionFunc func(*sseClientOptions) error

func (function sseClientOptionFunc) applySSEClient(
	options *sseClientOptions,
) error {
	return function(options)
}

type sseClientOptions struct {
	lastEventID   string
	maxEventBytes int
}

// WithSSELastEventID resumes after the last fully processed event.
func WithSSELastEventID(value string) SSEClientOption {
	return sseClientOptionFunc(func(options *sseClientOptions) error {
		if !ksse.ValidID(value) {
			return fmt.Errorf("%w: invalid Last-Event-ID", ksse.ErrInvalid)
		}
		options.lastEventID = value
		return nil
	})
}

// WithSSEClientMaxEventBytes overrides the per-event decode budget.
func WithSSEClientMaxEventBytes(maximum int) SSEClientOption {
	return sseClientOptionFunc(func(options *sseClientOptions) error {
		if maximum < 256 || maximum > ksse.MaximumEventBytes {
			return fmt.Errorf(
				"%w: event size is outside supported bounds",
				ksse.ErrInvalid,
			)
		}
		options.maxEventBytes = maximum
		return nil
	})
}

// SSEMessage is one immutable decoded SSE message.
type SSEMessage[T any] struct {
	id            string
	name          string
	value         T
	retryDuration time.Duration
}

// ID returns the current server reconnection cursor.
func (message SSEMessage[T]) ID() string { return message.id }

// Name returns the optional event type.
func (message SSEMessage[T]) Name() string { return message.name }

// Value returns the freshly decoded event value.
func (message SSEMessage[T]) Value() T { return message.value }

// Retry returns the most recently declared reconnection delay.
func (message SSEMessage[T]) Retry() time.Duration {
	return message.retryDuration
}

// SSEClientStream owns one native Hertz response body and parser goroutine.
type SSEClientStream[T any] struct {
	messages chan SSEMessage[T]
	done     chan struct{}
	cancel   context.CancelCauseFunc

	userClosed atomic.Bool
	mu         sync.Mutex
	terminal   error
}

// Recv waits for the next event. Normal completion or Close returns io.EOF.
func (stream *SSEClientStream[T]) Recv() (SSEMessage[T], error) {
	if stream == nil {
		var zero SSEMessage[T]
		return zero, ksse.ErrInvalid
	}
	message, open := <-stream.messages
	if open {
		return message, nil
	}
	var zero SSEMessage[T]
	err := stream.terminalError()
	if err == nil || stream.userClosed.Load() &&
		(errors.Is(err, errSSEClientClosed) ||
			errors.Is(err, context.Canceled)) {
		return zero, io.EOF
	}
	return zero, err
}

// Close cancels the subscription and waits for the response body to close.
func (stream *SSEClientStream[T]) Close(ctx context.Context) error {
	if stream == nil {
		return nil
	}
	if ctx == nil {
		return ErrInvalidCall
	}
	stream.userClosed.Store(true)
	stream.cancel(errSSEClientClosed)
	select {
	case <-stream.done:
		err := stream.terminalError()
		if err == nil ||
			errors.Is(err, errSSEClientClosed) ||
			errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (stream *SSEClientStream[T]) finish(err error) {
	stream.mu.Lock()
	stream.terminal = err
	stream.mu.Unlock()
	close(stream.messages)
	close(stream.done)
}

func (stream *SSEClientStream[T]) terminalError() error {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.terminal
}

// OpenProtoSSE opens a typed Proto SSE subscription through the native Hertz
// Client while retaining common Middleware and Picker feedback to stream
// terminal.
func OpenProtoSSE[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
	factory func() T,
	optionList ...SSEClientOption,
) (*SSEClientStream[T], error) {
	return openProtoSSE(
		ctx,
		client,
		target,
		request,
		factory,
		"",
		optionList...,
	)
}

// OpenProtoSSEResponseBody opens a typed native Hertz SSE subscription whose
// data contains one google.api.HttpRule.response_body field path value.
func OpenProtoSSEResponseBody[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
	factory func() T,
	responseBody string,
	optionList ...SSEClientOption,
) (*SSEClientStream[T], error) {
	return openProtoSSE(
		ctx,
		client,
		target,
		request,
		factory,
		responseBody,
		optionList...,
	)
}

func openProtoSSE[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
	factory func() T,
	responseBody string,
	optionList ...SSEClientOption,
) (*SSEClientStream[T], error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidCall)
	}
	if client == nil ||
		request == nil ||
		factory == nil ||
		target.Transport() != "http" ||
		target.Kind() != operation.KindServerStream {
		return nil, fmt.Errorf("%w: invalid Proto SSE call", ErrInvalidCall)
	}
	options := sseClientOptions{
		maxEventBytes: ksse.DefaultEventBytes,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: SSE client option %d is nil",
				ErrInvalidCall,
				index,
			)
		}
		if err := option.applySSEClient(&options); err != nil {
			return nil, fmt.Errorf(
				"%w: SSE client option %d: %v",
				ErrInvalidCall,
				index,
				err,
			)
		}
	}
	probe := factory()
	if isNilProtoMessage(probe) {
		return nil, fmt.Errorf(
			"%w: Proto factory returned nil",
			ErrInvalidCall,
		)
	}
	if err := khttp.ValidateProtoResponseBody(probe, responseBody); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCall, err)
	}

	prepared := protocol.AcquireRequest()
	request.CopyTo(prepared)
	if options.lastEventID != "" {
		prepared.Header.Set("Last-Event-ID", options.lastEventID)
	}
	streamContext, cancel := context.WithCancelCause(ctx)
	stream := &SSEClientStream[T]{
		messages: make(chan SSEMessage[T]),
		done:     make(chan struct{}),
		cancel:   cancel,
	}
	ready := make(chan error, 1)
	go func() {
		defer protocol.ReleaseRequest(prepared)
		connected := false
		_, err := client.Invoke(streamContext, target, ClientCall{
			Request:   prepared,
			Streaming: true,
			Decode: func(
				decodeContext context.Context,
				response *protocol.Response,
			) (any, error) {
				reader, readerErr := hsse.NewReader(response)
				if readerErr != nil {
					return nil, fmt.Errorf(
						"%w: response is not SSE",
						ksse.ErrInvalid,
					)
				}
				reader.SetMaxBufferSize(options.maxEventBytes + 512)
				connected = true
				ready <- nil
				return nil, consumeProtoSSE(
					decodeContext,
					reader,
					factory,
					responseBody,
					options.maxEventBytes,
					stream.messages,
				)
			},
		})
		if !connected {
			ready <- err
		}
		stream.finish(err)
	}()

	select {
	case err := <-ready:
		if err != nil {
			cancel(err)
			return nil, err
		}
		return stream, nil
	case <-ctx.Done():
		cause := context.Cause(ctx)
		cancel(cause)
		return nil, cause
	}
}

func consumeProtoSSE[T proto.Message](
	ctx context.Context,
	reader *hsse.Reader,
	factory func() T,
	responseBody string,
	maximum int,
	target chan<- SSEMessage[T],
) error {
	return reader.ForEach(ctx, func(raw *hsse.Event) error {
		event, err := ksse.NewEvent(ksse.EventSpec{
			ID:    raw.ID,
			Name:  raw.Type,
			Data:  string(raw.Data),
			Retry: raw.Retry,
		})
		if err != nil {
			return err
		}
		if _, err := ksse.Render(event, maximum); err != nil {
			if errors.Is(err, ksse.ErrEventTooLarge) {
				return ErrResponseTooLarge
			}
			return err
		}
		message := factory()
		if isNilProtoMessage(message) {
			return fmt.Errorf(
				"%w: Proto factory returned nil",
				ErrInvalidCall,
			)
		}
		if err := khttp.UnmarshalProtoResponseBody(
			raw.Data,
			message,
			responseBody,
		); err != nil {
			return fmt.Errorf("%w: decode Proto event", ksse.ErrInvalid)
		}
		decoded := SSEMessage[T]{
			id:            event.ID(),
			name:          event.Name(),
			value:         message,
			retryDuration: event.Retry(),
		}
		select {
		case target <- decoded:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
}
