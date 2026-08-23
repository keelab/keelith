package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"go.opentelemetry.io/otel/propagation"
	ggrpc "google.golang.org/grpc"
)

// StreamInvocation is the transport-neutral middleware request for a stream.
type StreamInvocation struct {
	FullMethod    string
	ClientStreams bool
	ServerStreams bool
}

func unaryServerInterceptor(
	bundle *middleware.Bundle,
	policy metadata.Policy,
	codec *ErrorCodec,
	propagator propagation.TextMapPropagator,
) ggrpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		request any,
		info *ggrpc.UnaryServerInfo,
		handler ggrpc.UnaryHandler,
	) (any, error) {
		target, err := operationFromMethod(info.FullMethod, operation.KindUnary)
		if err != nil {
			return nil, codec.Encode(err)
		}
		ctx, err = inboundContext(ctx, policy, propagator)
		if err != nil {
			return nil, codec.Encode(kerrors.Wrap(
				err,
				429,
				"METADATA_TOO_LARGE",
				"request metadata is invalid",
			))
		}
		ctx, err = withInboundRequestInfo(ctx, target)
		if err != nil {
			return nil, codec.Encode(err)
		}
		invoke := middleware.Handler(func(
			ctx context.Context,
			request any,
		) (any, error) {
			return handler(ctx, request)
		})
		if bundle != nil {
			invoke = bundle.Chain()(invoke)
		}
		response, err := invoke(ctx, request)
		return response, codec.Encode(err)
	}
}

func streamServerInterceptor(
	bundle *middleware.Bundle,
	streamBundle *middleware.StreamBundle,
	policy metadata.Policy,
	codec *ErrorCodec,
	propagator propagation.TextMapPropagator,
) ggrpc.StreamServerInterceptor {
	return func(
		service any,
		stream ggrpc.ServerStream,
		info *ggrpc.StreamServerInfo,
		handler ggrpc.StreamHandler,
	) error {
		kind := streamOperationKind(info.IsClientStream, info.IsServerStream)
		target, err := operationFromMethod(info.FullMethod, kind)
		if err != nil {
			return codec.Encode(err)
		}
		ctx, err := inboundContext(stream.Context(), policy, propagator)
		if err != nil {
			return codec.Encode(kerrors.Wrap(
				err,
				429,
				"METADATA_TOO_LARGE",
				"request metadata is invalid",
			))
		}
		ctx, err = withInboundRequestInfo(ctx, target)
		if err != nil {
			return codec.Encode(err)
		}
		wrapped := newServerStream(ctx, stream, streamBundle)
		invoke := middleware.Handler(func(
			handlerContext context.Context,
			_ any,
		) (_ any, resultErr error) {
			wrapped.context = handlerContext
			wrapped.events.context = handlerContext
			defer func() {
				recovered := recover()
				if recovered != nil {
					_ = wrapped.events.finish(errors.New(
						"grpc transport: stream handler panic",
					))
					panic(recovered)
				}
				resultErr = errors.Join(
					resultErr,
					wrapped.events.finish(resultErr),
				)
			}()
			if err := wrapped.events.create(); err != nil {
				return nil, err
			}
			return nil, handler(service, wrapped)
		})
		if bundle != nil {
			invoke = bundle.Chain()(invoke)
		}
		_, err = invoke(ctx, StreamInvocation{
			FullMethod:    info.FullMethod,
			ClientStreams: info.IsClientStream,
			ServerStreams: info.IsServerStream,
		})
		return codec.Encode(err)
	}
}

type serverStream struct {
	ggrpc.ServerStream
	context context.Context
	events  *streamEvents
}

func newServerStream(
	ctx context.Context,
	stream ggrpc.ServerStream,
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
		func(_ context.Context, anyMessage any) error {
			return stream.SendMsg(anyMessage)
		},
		func(_ context.Context, anyMessage any) error {
			return stream.RecvMsg(anyMessage)
		},
	)
	return wrapped
}

func (stream *serverStream) Context() context.Context {
	return stream.context
}

func (stream *serverStream) SendMsg(message any) error {
	return stream.events.send(message)
}

func (stream *serverStream) RecvMsg(message any) error {
	return stream.events.receive(message)
}

func operationFromMethod(
	fullMethod string,
	kind operation.Kind,
) (operation.Operation, error) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	separator := strings.LastIndex(trimmed, "/")
	if separator <= 0 || separator == len(trimmed)-1 {
		return operation.Operation{}, fmt.Errorf(
			"grpc transport: invalid full method %q",
			fullMethod,
		)
	}
	return operation.New(
		"grpc",
		trimmed[:separator],
		trimmed[separator+1:],
		kind,
	)
}

func streamOperationKind(clientStreams, serverStreams bool) operation.Kind {
	switch {
	case clientStreams && serverStreams:
		return operation.KindBidiStream
	case clientStreams:
		return operation.KindClientStream
	default:
		return operation.KindServerStream
	}
}

type clientStream struct {
	ggrpc.ClientStream
	codec           *ErrorCodec
	policy          metadata.Policy
	context         context.Context
	cancel          context.CancelFunc
	events          *streamEvents
	finishOnReceive bool
	completed       chan struct{}
	finishOnce      sync.Once
	finishErr       error
}

func newClientStream(
	ctx context.Context,
	stream ggrpc.ClientStream,
	cancel context.CancelFunc,
	codec *ErrorCodec,
	policy metadata.Policy,
	bundle *middleware.StreamBundle,
	finishOnReceive bool,
) *clientStream {
	wrapped := &clientStream{
		ClientStream:    stream,
		codec:           codec,
		policy:          policy,
		context:         ctx,
		cancel:          cancel,
		finishOnReceive: finishOnReceive,
		completed:       make(chan struct{}),
	}
	wrapped.events = newStreamEvents(
		ctx,
		middleware.StreamSideClient,
		bundle,
		func(_ context.Context, anyMessage any) error {
			err := stream.SendMsg(anyMessage)
			if errors.Is(err, io.EOF) {
				return err
			}
			return codec.Decode(err)
		},
		func(_ context.Context, anyMessage any) error {
			err := stream.RecvMsg(anyMessage)
			if errors.Is(err, io.EOF) {
				return err
			}
			return codec.Decode(err)
		},
	)
	return wrapped
}

func (stream *clientStream) open() error {
	if err := stream.events.create(); err != nil {
		return errors.Join(err, stream.finish(err))
	}
	go stream.watchContext()
	return nil
}

func (stream *clientStream) Header() (mapMetadata, error) {
	header, err := stream.ClientStream.Header()
	if err != nil {
		decoded := stream.codec.Decode(err)
		return nil, errors.Join(decoded, stream.finish(decoded))
	}
	filtered, err := filterMetadata(header, stream.policy)
	return filtered, err
}

func (stream *clientStream) Trailer() mapMetadata {
	filtered, err := filterMetadata(stream.ClientStream.Trailer(), stream.policy)
	if err != nil {
		return nil
	}
	return filtered
}

func (stream *clientStream) CloseSend() error {
	err := stream.codec.Decode(stream.ClientStream.CloseSend())
	if err != nil {
		return errors.Join(err, stream.finish(err))
	}
	return nil
}

func (stream *clientStream) SendMsg(message any) error {
	err := stream.events.send(message)
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.Join(err, stream.finish(err))
	}
	return err
}

func (stream *clientStream) RecvMsg(message any) error {
	err := stream.events.receive(message)
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

func (stream *clientStream) finish(err error) error {
	stream.finishOnce.Do(func() {
		stream.finishErr = stream.events.finish(err)
		stream.cancel()
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
