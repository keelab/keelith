package grpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/keelab/keelith/middleware"
)

type streamEvents struct {
	context context.Context
	side    middleware.StreamSide
	handler middleware.StreamHandler

	sendSequence    atomic.Uint64
	receiveSequence atomic.Uint64
	finishOnce      sync.Once
	finishErr       error
}

func newStreamEvents(
	ctx context.Context,
	side middleware.StreamSide,
	bundle *middleware.StreamBundle,
	send func(context.Context, any) error,
	receive func(context.Context, any) error,
) *streamEvents {
	terminal := middleware.StreamHandler(func(
		ctx context.Context,
		event middleware.StreamEvent,
	) error {
		switch event.Phase {
		case middleware.StreamPhaseCreate, middleware.StreamPhaseFinish:
			return nil
		case middleware.StreamPhaseSend:
			return send(ctx, event.Message)
		case middleware.StreamPhaseReceive:
			return receive(ctx, event.Message)
		default:
			return fmt.Errorf(
				"grpc transport: unsupported stream phase %q",
				event.Phase,
			)
		}
	})
	if bundle != nil {
		terminal = bundle.Chain()(terminal)
	}
	return &streamEvents{
		context: ctx,
		side:    side,
		handler: terminal,
	}
}

func (events *streamEvents) create() error {
	return events.handler(events.context, middleware.StreamEvent{
		Side:  events.side,
		Phase: middleware.StreamPhaseCreate,
	})
}

func (events *streamEvents) send(message any) error {
	return events.handler(events.context, middleware.StreamEvent{
		Side:     events.side,
		Phase:    middleware.StreamPhaseSend,
		Sequence: events.sendSequence.Add(1),
		Message:  message,
	})
}

func (events *streamEvents) receive(message any) error {
	return events.handler(events.context, middleware.StreamEvent{
		Side:     events.side,
		Phase:    middleware.StreamPhaseReceive,
		Sequence: events.receiveSequence.Add(1),
		Message:  message,
	})
}

func (events *streamEvents) finish(err error) error {
	events.finishOnce.Do(func() {
		events.finishErr = events.handler(
			events.context,
			middleware.StreamEvent{
				Side:  events.side,
				Phase: middleware.StreamPhaseFinish,
				Error: err,
			},
		)
	})
	return events.finishErr
}
