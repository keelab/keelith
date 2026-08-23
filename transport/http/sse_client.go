package http

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	nethttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/keelab/keelith/operation"
	"google.golang.org/protobuf/proto"
)

var errSSEClientClosed = errors.New("http transport: SSE client closed")

// SSEClientOption configures one outbound SSE subscription.
type SSEClientOption interface {
	applySSEClient(*sseClientOptions) error
}

type sseClientOptionFunc func(*sseClientOptions) error

func (f sseClientOptionFunc) applySSEClient(
	options *sseClientOptions,
) error {
	return f(options)
}

type sseClientOptions struct {
	lastEventID   string
	maxEventBytes int
}

// WithSSELastEventID resumes after the last fully processed event.
func WithSSELastEventID(value string) SSEClientOption {
	return sseClientOptionFunc(func(options *sseClientOptions) error {
		if !validSSEID(value) {
			return fmt.Errorf("%w: invalid Last-Event-ID", ErrInvalidSSE)
		}
		options.lastEventID = value
		return nil
	})
}

// WithSSEClientMaxEventBytes overrides the per-event decode budget.
func WithSSEClientMaxEventBytes(maxBytes int) SSEClientOption {
	return sseClientOptionFunc(func(options *sseClientOptions) error {
		if maxBytes < 256 || maxBytes > maxSSEEventBytes {
			return fmt.Errorf(
				"%w: event size is outside supported bounds",
				ErrInvalidSSE,
			)
		}
		options.maxEventBytes = maxBytes
		return nil
	})
}

// SSEMessage is one immutable decoded SSE message.
type SSEMessage[T any] struct {
	id    string
	name  string
	value T
	retry time.Duration
}

// ID returns the current server reconnection cursor.
func (message SSEMessage[T]) ID() string { return message.id }

// Name returns the optional event type.
func (message SSEMessage[T]) Name() string { return message.name }

// Value returns the freshly decoded event value.
func (message SSEMessage[T]) Value() T { return message.value }

// Retry returns the most recently declared reconnection delay.
func (message SSEMessage[T]) Retry() time.Duration { return message.retry }

// SSEClientStream owns one HTTP response body and its parser goroutine.
type SSEClientStream[T any] struct {
	messages chan SSEMessage[T]
	done     chan struct{}
	cancel   context.CancelCauseFunc

	userClosed atomic.Bool
	mu         sync.Mutex
	terminal   error
}

// Recv waits for the next event. A normally completed or explicitly closed
// stream returns io.EOF.
func (stream *SSEClientStream[T]) Recv() (SSEMessage[T], error) {
	if stream == nil {
		var zero SSEMessage[T]
		return zero, ErrInvalidSSE
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
		return ErrNilContext
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

// OpenProtoSSE opens a typed Proto SSE subscription through Client.
//
// The returned stream retains the Client middleware invocation until EOF,
// cancellation, or Close. The HTTP response has no total body limit; every
// individual event remains bounded.
func OpenProtoSSE[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *nethttp.Request,
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

// OpenProtoSSEResponseBody opens a typed SSE subscription whose data field
// contains one google.api.HttpRule.response_body field path value.
func OpenProtoSSEResponseBody[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *nethttp.Request,
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
	request *nethttp.Request,
	factory func() T,
	responseBody string,
	optionList ...SSEClientOption,
) (*SSEClientStream[T], error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if client == nil ||
		request == nil ||
		request.URL == nil ||
		factory == nil ||
		target.Transport() != "http" ||
		target.Kind() != operation.KindServerStream {
		return nil, fmt.Errorf("%w: invalid Proto SSE call", ErrInvalidCall)
	}
	options := sseClientOptions{maxEventBytes: defaultSSEEventBytes}
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
				"%w: SSE client option %d: %w",
				ErrInvalidCall,
				index,
				err,
			)
		}
	}
	probe := factory()
	if isNilProto(probe) {
		return nil, fmt.Errorf("%w: Proto factory returned nil", ErrInvalidCall)
	}
	if err := ValidateProtoResponseBody(probe, responseBody); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCall, err)
	}
	prepared := request.Clone(ctx)
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
		connected := false
		_, err := client.Invoke(streamContext, target, ClientCall{
			Request:   prepared,
			Streaming: true,
			Decode: func(
				decodeContext context.Context,
				response *nethttp.Response,
			) (any, error) {
				if err := validateSSEContentType(response); err != nil {
					return nil, err
				}
				connected = true
				ready <- nil
				return nil, consumeProtoSSE(
					decodeContext,
					response.Body,
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

func validateSSEContentType(response *nethttp.Response) error {
	if response == nil {
		return fmt.Errorf("%w: response is nil", ErrInvalidSSE)
	}
	contentType, _, err := mime.ParseMediaType(
		response.Header.Get("Content-Type"),
	)
	if err != nil || contentType != "text/event-stream" {
		return fmt.Errorf("%w: response is not text/event-stream", ErrInvalidSSE)
	}
	return nil
}

func consumeProtoSSE[T proto.Message](
	ctx context.Context,
	reader io.Reader,
	factory func() T,
	responseBody string,
	maxEventBytes int,
	target chan<- SSEMessage[T],
) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4*1024), maxEventBytes+512)
	lastID := ""
	var eventName string
	var retry time.Duration
	var data strings.Builder
	eventBytes := 0

	dispatch := func() error {
		if data.Len() == 0 {
			eventName = ""
			eventBytes = 0
			return nil
		}
		payload := strings.TrimSuffix(data.String(), "\n")
		message := factory()
		if isNilProto(message) {
			return fmt.Errorf("%w: Proto factory returned nil", ErrInvalidSSE)
		}
		if err := UnmarshalProtoResponseBody(
			[]byte(payload),
			message,
			responseBody,
		); err != nil {
			return fmt.Errorf("%w: decode Proto event", ErrInvalidSSE)
		}
		event := SSEMessage[T]{
			id:    lastID,
			name:  eventName,
			value: message,
			retry: retry,
		}
		select {
		case target <- event:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		data.Reset()
		eventName = ""
		eventBytes = 0
		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		if !utf8.ValidString(line) {
			return ErrInvalidSSE
		}
		eventBytes += len(line) + 1
		if eventBytes > maxEventBytes {
			return ErrResponseTooLarge
		}
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		} else {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
		case "event":
			if !validSSEName(value) {
				return ErrInvalidSSE
			}
			eventName = value
		case "id":
			if !validSSEID(value) {
				return ErrInvalidSSE
			}
			lastID = value
		case "retry":
			milliseconds, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				continue
			}
			candidate := time.Duration(milliseconds) * time.Millisecond
			if candidate >= minSSERetry && candidate <= maxSSERetry {
				retry = candidate
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if errors.Is(err, bufio.ErrTooLong) {
			return ErrResponseTooLarge
		}
		return err
	}
	if data.Len() > 0 {
		if err := dispatch(); err != nil {
			return err
		}
	}
	return nil
}
