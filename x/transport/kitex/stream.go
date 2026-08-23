package kitex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cloudwego/kitex/pkg/endpoint/cep"
	"github.com/cloudwego/kitex/pkg/endpoint/sep"
	"github.com/cloudwego/kitex/pkg/streaming"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

type streamEvents struct {
	context context.Context
	side    middleware.StreamSide
	handler middleware.StreamHandler

	sendSequence    atomic.Uint64
	receiveSequence atomic.Uint64
	finishOnce      sync.Once
	finishErr       error
	done            chan struct{}
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
				"kitex profile: unsupported stream phase %q",
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
		done:    make(chan struct{}),
	}
}

func (events *streamEvents) create() error {
	return events.handler(events.context, middleware.StreamEvent{
		Side:  events.side,
		Phase: middleware.StreamPhaseCreate,
	})
}

func (events *streamEvents) send(
	ctx context.Context,
	message any,
) error {
	return events.handler(events.effectiveContext(ctx), middleware.StreamEvent{
		Side:     events.side,
		Phase:    middleware.StreamPhaseSend,
		Sequence: events.sendSequence.Add(1),
		Message:  message,
	})
}

func (events *streamEvents) receive(
	ctx context.Context,
	message any,
) error {
	return events.handler(events.effectiveContext(ctx), middleware.StreamEvent{
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
		close(events.done)
	})
	return events.finishErr
}

func (events *streamEvents) effectiveContext(
	ctx context.Context,
) context.Context {
	if ctx == nil {
		return events.context
	}
	if _, ok := operation.RequestInfoFromContext(ctx); ok {
		return ctx
	}
	if info, ok := operation.RequestInfoFromContext(events.context); ok {
		return operation.WithRequestInfo(ctx, info)
	}
	return ctx
}

func serverStreamMiddleware(
	bundle *middleware.StreamBundle,
) sep.StreamMiddleware {
	return func(next sep.StreamEndpoint) sep.StreamEndpoint {
		return func(
			ctx context.Context,
			stream streaming.ServerStream,
		) (resultErr error) {
			wrapped := newServerStream(ctx, stream, bundle)
			defer func() {
				if recovered := recover(); recovered != nil {
					_ = wrapped.events.finish(errors.New(
						"kitex profile: stream handler panic",
					))
					panic(recovered)
				}
				resultErr = errors.Join(
					resultErr,
					wrapped.events.finish(resultErr),
				)
			}()
			if err := wrapped.events.create(); err != nil {
				return err
			}
			return next(ctx, wrapped)
		}
	}
}

type serverStream struct {
	streaming.ServerStream
	context    context.Context
	events     *streamEvents
	grpcStream streaming.Stream
}

func newServerStream(
	ctx context.Context,
	stream streaming.ServerStream,
	bundle *middleware.StreamBundle,
) *serverStream {
	wrapped := &serverStream{
		ServerStream: stream,
		context:      ctx,
	}
	wrapped.events = newStreamEvents(
		ctx,
		middleware.StreamSideServer,
		bundle,
		func(ctx context.Context, message any) error {
			return stream.SendMsg(ctx, message)
		},
		func(ctx context.Context, message any) error {
			return stream.RecvMsg(ctx, message)
		},
	)
	if getter, ok := stream.(streaming.GRPCStreamGetter); ok {
		if raw := getter.GetGRPCStream(); raw != nil {
			wrapped.grpcStream = &grpcServerStream{
				Stream:  raw,
				context: ctx,
				events:  wrapped.events,
			}
		}
	}
	return wrapped
}

func (stream *serverStream) SendMsg(
	ctx context.Context,
	message any,
) error {
	return stream.events.send(ctx, message)
}

func (stream *serverStream) RecvMsg(
	ctx context.Context,
	message any,
) error {
	return stream.events.receive(ctx, message)
}

func (stream *serverStream) GetGRPCStream() streaming.Stream {
	return stream.grpcStream
}

type grpcServerStream struct {
	streaming.Stream
	context context.Context
	events  *streamEvents
}

func (stream *grpcServerStream) Context() context.Context {
	return stream.context
}

func (stream *grpcServerStream) SendMsg(message any) error {
	return stream.events.send(stream.context, message)
}

func (stream *grpcServerStream) RecvMsg(message any) error {
	return stream.events.receive(stream.context, message)
}

func clientStreamMiddleware(
	bundle *middleware.StreamBundle,
	codec *ErrorCodec,
	selectionEnabled bool,
) cep.StreamMiddleware {
	return func(next cep.StreamEndpoint) cep.StreamEndpoint {
		return func(
			ctx context.Context,
		) (streaming.ClientStream, error) {
			var selection *selectionState
			if selectionEnabled {
				selection = &selectionState{}
				ctx = withSelectionState(ctx, selection)
			}
			target, err := operationFromContext(ctx)
			if err != nil {
				return nil, err
			}
			raw, err := next(ctx)
			if err != nil {
				decoded := codec.Decode(
					clientResultError(ctx, err),
				)
				selection.finish(ctx, decoded)
				return nil, decoded
			}
			ctx, err = withRequestInfo(ctx, target, clientSide)
			if err != nil {
				selection.finish(ctx, err)
				streaming.FinishClientStream(raw, err)
				return nil, err
			}
			wrapped := newClientStream(
				ctx,
				raw,
				bundle,
				codec,
				target.Kind() == operation.KindClientStream,
				selection,
			)
			if err := wrapped.open(); err != nil {
				return nil, err
			}
			return wrapped, nil
		}
	}
}

type clientStream struct {
	streaming.ClientStream
	context         context.Context
	codec           *ErrorCodec
	events          *streamEvents
	grpcStream      streaming.Stream
	finishOnReceive bool
	completed       chan struct{}
	finishOnce      sync.Once
	finishErr       error
	selection       *selectionState
}

func newClientStream(
	ctx context.Context,
	stream streaming.ClientStream,
	bundle *middleware.StreamBundle,
	codec *ErrorCodec,
	finishOnReceive bool,
	selection *selectionState,
) *clientStream {
	wrapped := &clientStream{
		ClientStream:    stream,
		context:         ctx,
		codec:           codec,
		finishOnReceive: finishOnReceive,
		completed:       make(chan struct{}),
		selection:       selection,
	}
	wrapped.events = newStreamEvents(
		ctx,
		middleware.StreamSideClient,
		bundle,
		func(ctx context.Context, message any) error {
			return decodeStreamError(
				codec,
				stream.SendMsg(ctx, message),
			)
		},
		func(ctx context.Context, message any) error {
			return decodeStreamError(
				codec,
				stream.RecvMsg(ctx, message),
			)
		},
	)
	if getter, ok := stream.(streaming.GRPCStreamGetter); ok {
		if raw := getter.GetGRPCStream(); raw != nil {
			wrapped.grpcStream = &grpcClientStream{
				Stream:  raw,
				context: ctx,
				parent:  wrapped,
			}
		}
	}
	return wrapped
}

func (stream *clientStream) open() error {
	if err := stream.events.create(); err != nil {
		return errors.Join(err, stream.finish(err))
	}
	go stream.watchContext()
	return nil
}

func (stream *clientStream) Context() context.Context {
	return stream.context
}

func (stream *clientStream) Header() (
	streaming.Header,
	error,
) {
	header, err := stream.ClientStream.Header()
	err = decodeStreamError(stream.codec, err)
	if err != nil {
		return header, errors.Join(err, stream.finish(err))
	}
	return header, nil
}

func (stream *clientStream) Trailer() (
	streaming.Trailer,
	error,
) {
	trailer, err := stream.ClientStream.Trailer()
	err = decodeStreamError(stream.codec, err)
	if err != nil {
		return trailer, errors.Join(err, stream.finish(err))
	}
	return trailer, nil
}

func (stream *clientStream) CloseSend(ctx context.Context) error {
	err := decodeStreamError(
		stream.codec,
		stream.ClientStream.CloseSend(ctx),
	)
	if err != nil {
		return errors.Join(err, stream.finish(err))
	}
	return nil
}

func (stream *clientStream) SendMsg(
	ctx context.Context,
	message any,
) error {
	err := stream.events.send(ctx, message)
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.Join(err, stream.finish(err))
	}
	return err
}

func (stream *clientStream) RecvMsg(
	ctx context.Context,
	message any,
) error {
	err := stream.events.receive(ctx, message)
	switch {
	case errors.Is(err, io.EOF):
		if finishErr := stream.finish(nil); finishErr != nil {
			return finishErr
		}
		return err
	case err != nil:
		return errors.Join(err, stream.finish(err))
	case stream.finishOnReceive:
		return stream.finish(nil)
	default:
		return nil
	}
}

func (stream *clientStream) GetGRPCStream() streaming.Stream {
	return stream.grpcStream
}

func (stream *clientStream) DoFinish(err error) {
	_ = stream.finish(decodeStreamError(stream.codec, err))
}

func (stream *clientStream) finish(err error) error {
	stream.finishOnce.Do(func() {
		stream.finishErr = stream.events.finish(err)
		feedbackErr := err
		if stream.finishErr != nil {
			feedbackErr = errors.Join(feedbackErr, stream.finishErr)
		}
		stream.selection.finish(stream.context, feedbackErr)
		streaming.FinishClientStream(stream.ClientStream, err)
		close(stream.completed)
	})
	return stream.finishErr
}

func (stream *clientStream) watchContext() {
	select {
	case <-stream.context.Done():
		_ = stream.finish(context.Cause(stream.context))
	case <-stream.completed:
	}
}

type grpcClientStream struct {
	streaming.Stream
	context context.Context
	parent  *clientStream
}

func (stream *grpcClientStream) Context() context.Context {
	return stream.context
}

func (stream *grpcClientStream) SendMsg(message any) error {
	return stream.parent.SendMsg(stream.context, message)
}

func (stream *grpcClientStream) RecvMsg(message any) error {
	return stream.parent.RecvMsg(stream.context, message)
}

func (stream *grpcClientStream) Close() error {
	err := decodeStreamError(stream.parent.codec, stream.Stream.Close())
	if err != nil {
		return errors.Join(err, stream.parent.finish(err))
	}
	return nil
}

func (stream *grpcClientStream) DoFinish(err error) {
	stream.parent.DoFinish(
		decodeStreamError(stream.parent.codec, err),
	)
}

func decodeStreamError(codec *ErrorCodec, err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	decoded := codec.Decode(err)
	canonical := kitexFeedbackFailure(decoded)
	switch failure.Classify(canonical) {
	case failure.Canceled:
		if errors.Is(decoded, context.Canceled) {
			return decoded
		}
		return errors.Join(context.Canceled, decoded)
	case failure.Timeout:
		if errors.Is(decoded, context.DeadlineExceeded) {
			return decoded
		}
		return errors.Join(context.DeadlineExceeded, decoded)
	case failure.Transport:
		return failure.MarkTransport(decoded)
	default:
		return decoded
	}
}
