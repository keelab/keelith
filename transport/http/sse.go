package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"time"

	ksse "github.com/keelab/keelith/transport/sse"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrInvalidSSE reports malformed configuration, event, or response type.
	ErrInvalidSSE = ksse.ErrInvalid
	// ErrSSEUnsupported reports a ResponseWriter without streaming flush.
	ErrSSEUnsupported = errors.New(
		"http transport: server-sent events unsupported",
	)
	// ErrSSESource reports a terminal producer error after streaming started.
	ErrSSESource = ksse.ErrSource
)

const (
	defaultSSEEventBytes = ksse.DefaultEventBytes
	maxSSEEventBytes     = ksse.MaximumEventBytes
	minSSERetry          = ksse.MinimumRetry
	maxSSERetry          = ksse.MaximumRetry
)

func validSSEID(value string) bool   { return ksse.ValidID(value) }
func validSSEName(value string) bool { return ksse.ValidName(value) }

// SSEEventSpec is the construction input for one immutable SSE event.
type SSEEventSpec = ksse.EventSpec

// SSEEvent is one immutable server-sent event.
type SSEEvent = ksse.Event

// SSERequest is the immutable reconnection input for one SSE subscription.
type SSERequest = ksse.Request

// ServerSentEvents is a response backed by an event channel and an optional
// terminal failure channel.
type ServerSentEvents = ksse.Stream

// SSEConfig configures a streaming Encoder.
type SSEConfig = ksse.Config

// DecodeSSERequest reads the standard Last-Event-ID request header.
func DecodeSSERequest(request *nethttp.Request) (any, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidSSE)
	}
	values := request.Header.Values("Last-Event-ID")
	if len(values) > 1 {
		return nil, fmt.Errorf(
			"%w: multiple Last-Event-ID values",
			ErrInvalidSSE,
		)
	}
	value := ""
	if len(values) == 1 {
		value = values[0]
	}
	return ksse.NewRequest(value)
}

// NewSSEEvent validates and snapshots one event.
func NewSSEEvent(spec SSEEventSpec) (SSEEvent, error) {
	return ksse.NewEvent(spec)
}

// NewSSEJSONEvent JSON-encodes value as one immutable event.
func NewSSEJSONEvent(
	id string,
	name string,
	value any,
	retry time.Duration,
) (SSEEvent, error) {
	return ksse.NewJSONEvent(id, name, value, retry)
}

// NewSSEProtoEvent protojson-encodes message as one immutable event.
func NewSSEProtoEvent(
	id string,
	name string,
	message proto.Message,
	retry time.Duration,
) (SSEEvent, error) {
	return ksse.NewProtoEvent(id, name, message, retry)
}

// NewSSEProtoResponseBodyEvent protojson-encodes one
// google.api.HttpRule.response_body field path as immutable event data.
func NewSSEProtoResponseBodyEvent(
	id string,
	name string,
	message proto.Message,
	responseBody string,
	retry time.Duration,
) (SSEEvent, error) {
	payload, err := MarshalProtoResponseBody(message, responseBody)
	if err != nil {
		return SSEEvent{}, err
	}
	return ksse.NewEvent(ksse.EventSpec{
		ID: id, Name: name, Data: string(payload), Retry: retry,
	})
}

// NewServerSentEvents constructs a streaming response.
func NewServerSentEvents(
	events <-chan SSEEvent,
	failures <-chan error,
) (ServerSentEvents, error) {
	return ksse.NewStream(events, failures)
}

// NewSSEEncoder creates a bounded Server-Sent Events encoder. Routes using it
// must also opt into WithStreaming. The encoder waits for the first event,
// heartbeat, clean event-source close, or producer failure before committing
// HTTP headers so typed failures remain available to the Router error encoder.
func NewSSEEncoder(config SSEConfig) (Encoder, error) {
	settings, err := ksse.Resolve(config)
	if err != nil {
		return nil, err
	}
	return func(
		ctx context.Context,
		writer nethttp.ResponseWriter,
		response any,
	) error {
		if ctx == nil || writer == nil {
			return ErrInvalidSSE
		}
		stream, ok := response.(ServerSentEvents)
		if !ok || stream.Events() == nil {
			return fmt.Errorf(
				"%w: response type %T",
				ErrInvalidSSE,
				response,
			)
		}
		flusher, ok := writer.(nethttp.Flusher)
		if !ok {
			return ErrSSEUnsupported
		}
		var ticker *time.Ticker
		var heartbeat <-chan time.Time
		if settings.HeartbeatEnabled() {
			ticker = time.NewTicker(settings.HeartbeatInterval())
			heartbeat = ticker.C
			defer ticker.Stop()
		}
		events := stream.Events()
		failures := stream.Failures()
		firstPayload, firstSignal, failures, err := preflightSSE(
			ctx,
			events,
			failures,
			heartbeat,
			settings.MaximumEventBytes(),
		)
		if err != nil {
			return err
		}
		writer.Header().Set("Content-Type", ksse.ContentType)
		writer.Header().Set("Cache-Control", ksse.CacheControl)
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.WriteHeader(nethttp.StatusOK)
		switch firstSignal {
		case ssePreflightEvent:
			if _, err := writer.Write(firstPayload); err != nil {
				return err
			}
		case ssePreflightHeartbeat:
			if _, err := io.WriteString(writer, ksse.HeartbeatComment); err != nil {
				return err
			}
		case ssePreflightClean:
		}
		flusher.Flush()
		if firstSignal == ssePreflightClean {
			return nil
		}
		for events != nil {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case event, open := <-events:
				if !open {
					return nil
				}
				payload, err := ksse.Render(
					event,
					settings.MaximumEventBytes(),
				)
				if errors.Is(err, ksse.ErrEventTooLarge) {
					return ErrResponseTooLarge
				}
				if err != nil {
					return err
				}
				if _, err := writer.Write(payload); err != nil {
					return err
				}
				flusher.Flush()
			case failureErr, open := <-failures:
				if !open {
					failures = nil
					continue
				}
				if failureErr == nil {
					return ErrSSESource
				}
				return fmt.Errorf("%w: %w", ErrSSESource, failureErr)
			case <-heartbeat:
				if _, err := io.WriteString(
					writer,
					ksse.HeartbeatComment,
				); err != nil {
					return err
				}
				flusher.Flush()
			}
		}
		return nil
	}, nil
}

type ssePreflightSignal uint8

const (
	ssePreflightClean ssePreflightSignal = iota
	ssePreflightEvent
	ssePreflightHeartbeat
)

func preflightSSE(
	ctx context.Context,
	events <-chan SSEEvent,
	failures <-chan error,
	heartbeat <-chan time.Time,
	maximumEventBytes int,
) ([]byte, ssePreflightSignal, <-chan error, error) {
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, ssePreflightClean, failures, cause
		}
		if failures != nil {
			select {
			case failureErr, open := <-failures:
				if !open {
					failures = nil
					continue
				}
				return nil, ssePreflightClean, failures, precommitSSEFailure(failureErr)
			default:
			}
		}
		select {
		case <-ctx.Done():
			return nil, ssePreflightClean, failures, context.Cause(ctx)
		case failureErr, open := <-failures:
			if !open {
				failures = nil
				continue
			}
			return nil, ssePreflightClean, failures, precommitSSEFailure(failureErr)
		case event, open := <-events:
			if !open {
				if failures != nil {
					select {
					case failureErr, failureOpen := <-failures:
						if failureOpen {
							return nil, ssePreflightClean, failures, precommitSSEFailure(failureErr)
						}
						failures = nil
					default:
					}
				}
				if cause := context.Cause(ctx); cause != nil {
					return nil, ssePreflightClean, failures, cause
				}
				return nil, ssePreflightClean, failures, nil
			}
			payload, err := renderSSEEvent(event, maximumEventBytes)
			if err != nil {
				return nil, ssePreflightClean, failures, err
			}
			if cause := context.Cause(ctx); cause != nil {
				return nil, ssePreflightClean, failures, cause
			}
			return payload, ssePreflightEvent, failures, nil
		case <-heartbeat:
			if failures != nil {
				select {
				case failureErr, open := <-failures:
					if open {
						return nil, ssePreflightClean, failures, precommitSSEFailure(failureErr)
					}
					failures = nil
				default:
				}
			}
			if cause := context.Cause(ctx); cause != nil {
				return nil, ssePreflightClean, failures, cause
			}
			return nil, ssePreflightHeartbeat, failures, nil
		}
	}
}

func precommitSSEFailure(err error) error {
	if err == nil {
		return ErrSSESource
	}
	return err
}

func renderSSEEvent(event SSEEvent, maximumEventBytes int) ([]byte, error) {
	payload, err := ksse.Render(event, maximumEventBytes)
	if errors.Is(err, ksse.ErrEventTooLarge) {
		return nil, ErrResponseTooLarge
	}
	return payload, err
}
