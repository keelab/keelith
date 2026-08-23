package worker

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// Action is the broker-neutral disposition of one message or job execution.
type Action string

const (
	// ActionAck accepts and commits successful work.
	ActionAck Action = "ack"
	// ActionNack rejects work without asking Keelith to schedule a retry.
	ActionNack Action = "nack"
	// ActionRetry asks the adapter to redeliver after a bounded delay.
	ActionRetry Action = "retry"
	// ActionDeadLetter asks the adapter to quarantine the work.
	ActionDeadLetter Action = "dead-letter"
)

var (
	// ErrNacked is used when Nack is constructed without a cause.
	ErrNacked = errors.New("worker: message was negatively acknowledged")
	// ErrRetryRequested is used when Retry is constructed without a cause.
	ErrRetryRequested = errors.New("worker: retry requested")
	// ErrDeadLettered is used when DeadLetter is constructed without a cause.
	ErrDeadLettered = errors.New("worker: dead letter requested")
	// ErrInvalidResult reports a handler or middleware response with no valid
	// disposition.
	ErrInvalidResult = errors.New("worker: invalid result")
)

// Result is an immutable broker-neutral handling disposition.
type Result struct {
	action     Action
	cause      error
	retryAfter time.Duration
}

// Ack creates a successful disposition.
func Ack() Result {
	return Result{action: ActionAck}
}

// Nack creates a negative acknowledgement.
func Nack(cause error) Result {
	if cause == nil {
		cause = ErrNacked
	}
	return Result{action: ActionNack, cause: cause}
}

// Retry creates a retry disposition. Negative delays are normalized to zero.
func Retry(cause error, after time.Duration) Result {
	if cause == nil {
		cause = ErrRetryRequested
	}
	if after < 0 {
		after = 0
	}
	return Result{
		action:     ActionRetry,
		cause:      cause,
		retryAfter: after,
	}
}

// DeadLetter creates a quarantine disposition.
func DeadLetter(cause error) Result {
	if cause == nil {
		cause = ErrDeadLettered
	}
	return Result{action: ActionDeadLetter, cause: cause}
}

// Action returns the stable disposition.
func (r Result) Action() Action {
	return r.action
}

// Cause returns the handler or policy failure associated with the disposition.
func (r Result) Cause() error {
	return r.cause
}

// RetryAfter returns the requested redelivery delay.
func (r Result) RetryAfter() time.Duration {
	return r.retryAfter
}

func (r Result) valid() bool {
	switch r.action {
	case ActionAck:
		return r.cause == nil && r.retryAfter == 0
	case ActionNack, ActionDeadLetter:
		return r.cause != nil && r.retryAfter == 0
	case ActionRetry:
		return r.cause != nil && r.retryAfter >= 0
	default:
		return false
	}
}

// Message is an immutable broker-neutral delivery envelope.
type Message struct {
	id       string
	payload  []byte
	metadata metadata.Metadata
}

// NewMessage defensively copies a delivery payload and metadata.
func NewMessage(id string, payload []byte, inbound metadata.Metadata) Message {
	return Message{
		id:       strings.TrimSpace(id),
		payload:  append([]byte(nil), payload...),
		metadata: inbound.Clone(),
	}
}

// ID returns the adapter-provided message identity.
func (m Message) ID() string {
	return m.id
}

// Payload returns a defensive copy of the delivery body.
func (m Message) Payload() []byte {
	return append([]byte(nil), m.payload...)
}

// Metadata returns an immutable clone of inbound delivery metadata.
func (m Message) Metadata() metadata.Metadata {
	return m.metadata.Clone()
}

// ConsumerHandler maps one delivery to an explicit disposition.
type ConsumerHandler func(context.Context, Message) Result

// Consumer is implemented by broker adapters.
//
// Subscribe returns only after the broker subscription is ready. StopPulling
// prevents new callbacks, Drain waits for adapter-side commit/rollback, Close
// releases connections, and Wait reports runtime termination.
type Consumer interface {
	Subscribe(context.Context, ConsumerHandler) error
	StopPulling(context.Context) error
	Drain(context.Context) error
	Close(context.Context) error
	Wait() error
}

// ConsumerConfig constructs a broker-neutral Worker.
type ConsumerConfig struct {
	Name       string
	Operation  operation.Operation
	Consumer   Consumer
	Handler    ConsumerHandler
	Middleware *middleware.Bundle
	Health     *health.Registry
}

// NewConsumer constructs a Worker around a broker adapter.
func NewConsumer(config ConsumerConfig) (*Worker, error) {
	if isNil(config.Consumer) {
		return nil, invalidOption("consumer is nil")
	}
	if config.Handler == nil {
		return nil, invalidOption("consumer handler is nil")
	}
	source := runtimeSource{
		start: func(ctx context.Context, dispatch middleware.Handler) error {
			return config.Consumer.Subscribe(
				ctx,
				func(ctx context.Context, message Message) Result {
					ctx = metadata.WithInbound(ctx, message.Metadata())
					response, err := dispatch(ctx, message)
					return normalizeResult(response, err)
				},
			)
		},
		stopPulling: config.Consumer.StopPulling,
		drain:       config.Consumer.Drain,
		close:       config.Consumer.Close,
		wait:        config.Consumer.Wait,
	}
	final := middleware.Handler(func(ctx context.Context, request any) (any, error) {
		message, ok := request.(Message)
		if !ok {
			return nil, ErrInvalidResult
		}
		result := config.Handler(ctx, message)
		return result, result.Cause()
	})
	return newWorker(
		config.Name,
		config.Operation,
		operation.KindConsumer,
		source,
		final,
		config.Middleware,
		config.Health,
	)
}

func normalizeResult(response any, err error) Result {
	result, ok := response.(Result)
	if !ok || !result.valid() {
		if err == nil {
			err = ErrInvalidResult
		}
		return Nack(err)
	}
	if err == nil {
		return result
	}
	switch result.action {
	case ActionNack, ActionRetry, ActionDeadLetter:
		result.cause = err
		return result
	default:
		return Nack(err)
	}
}
