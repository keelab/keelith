package grpc

import (
	"context"
	"fmt"
	"reflect"

	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"go.opentelemetry.io/otel/propagation"
	ggrpc "google.golang.org/grpc"
	gmetadata "google.golang.org/grpc/metadata"
)

// ClientOption configures a generated-client-compatible Client.
type ClientOption interface {
	applyClient(*clientOptions) error
}

type clientOptionFunc func(*clientOptions) error

func (function clientOptionFunc) applyClient(options *clientOptions) error {
	return function(options)
}

type clientOptions struct {
	bundle         *middleware.Bundle
	streamBundle   *middleware.StreamBundle
	metadataPolicy metadata.Policy
	errorCodec     *ErrorCodec
	propagator     propagation.TextMapPropagator
}

// WithClientStreamMiddleware configures outbound stream lifecycle hooks.
func WithClientStreamMiddleware(
	bundle *middleware.StreamBundle,
) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if bundle == nil {
			return fmt.Errorf("stream middleware bundle is nil")
		}
		options.streamBundle = bundle
		return nil
	})
}

// WithClientMiddleware configures the immutable outbound middleware Bundle.
func WithClientMiddleware(bundle *middleware.Bundle) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if bundle == nil {
			return fmt.Errorf("middleware bundle is nil")
		}
		options.bundle = bundle
		return nil
	})
}

// WithClientMetadataPolicy configures gRPC metadata propagation.
func WithClientMetadataPolicy(policy metadata.Policy) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		options.metadataPolicy = policy
		return nil
	})
}

// WithClientPropagator configures distributed context injection.
func WithClientPropagator(
	propagator propagation.TextMapPropagator,
) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if propagator == nil {
			return fmt.Errorf("propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithClientErrorCodec configures framework Error restoration.
func WithClientErrorCodec(codec *ErrorCodec) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if codec == nil {
			return fmt.Errorf("error codec is nil")
		}
		options.errorCodec = codec
		return nil
	})
}

// Client implements grpc.ClientConnInterface for generated clients.
type Client struct {
	connection     ggrpc.ClientConnInterface
	bundle         *middleware.Bundle
	streamBundle   *middleware.StreamBundle
	metadataPolicy metadata.Policy
	errorCodec     *ErrorCodec
	propagator     propagation.TextMapPropagator
}

// NewClient wraps connection with Keelith's outbound contracts.
func NewClient(
	connection ggrpc.ClientConnInterface,
	optionList ...ClientOption,
) (*Client, error) {
	if isNilConnection(connection) {
		return nil, fmt.Errorf("%w: client connection is nil", ErrInvalidOption)
	}
	defaultCodec, _ := NewErrorCodec()
	options := clientOptions{errorCodec: defaultCodec}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: client option %d is nil", ErrInvalidOption, index)
		}
		if err := option.applyClient(&options); err != nil {
			return nil, fmt.Errorf("%w: client option %d: %w", ErrInvalidOption, index, err)
		}
	}
	return &Client{
		connection:     connection,
		bundle:         options.bundle,
		streamBundle:   options.streamBundle,
		metadataPolicy: options.metadataPolicy,
		errorCodec:     options.errorCodec,
		propagator:     options.propagator,
	}, nil
}

// Invoke performs a unary RPC for generated clients.
func (client *Client) Invoke(
	ctx context.Context,
	method string,
	request any,
	reply any,
	options ...ggrpc.CallOption,
) error {
	if ctx == nil {
		return ErrNilContext
	}
	target, err := operationFromMethod(method, operation.KindUnary)
	if err != nil {
		return err
	}
	ctx, err = withOutboundRequestInfo(ctx, target)
	if err != nil {
		return err
	}
	invoke := middleware.Handler(func(
		ctx context.Context,
		request any,
	) (any, error) {
		outbound, outboundErr := outboundContext(
			ctx,
			client.metadataPolicy,
			client.propagator,
		)
		if outboundErr != nil {
			return nil, outboundErr
		}
		invokeErr := client.connection.Invoke(
			outbound,
			method,
			request,
			reply,
			options...,
		)
		return reply, client.errorCodec.Decode(invokeErr)
	})
	if client.bundle != nil {
		invoke = client.bundle.Chain()(invoke)
	}
	_, err = invoke(ctx, request)
	return err
}

// NewStream begins a streaming RPC for generated clients.
func (client *Client) NewStream(
	ctx context.Context,
	description *ggrpc.StreamDesc,
	method string,
	options ...ggrpc.CallOption,
) (ggrpc.ClientStream, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if description == nil {
		return nil, fmt.Errorf("%w: stream description is nil", ErrInvalidOption)
	}
	kind := streamOperationKind(description.ClientStreams, description.ServerStreams)
	target, err := operationFromMethod(method, kind)
	if err != nil {
		return nil, err
	}
	ctx, err = withOutboundRequestInfo(ctx, target)
	if err != nil {
		return nil, err
	}
	invoke := middleware.Handler(func(
		ctx context.Context,
		_ any,
	) (any, error) {
		outbound, outboundErr := outboundContext(
			ctx,
			client.metadataPolicy,
			client.propagator,
		)
		if outboundErr != nil {
			return nil, outboundErr
		}
		streamContext, cancelStream := context.WithCancel(outbound)
		stream, streamErr := client.connection.NewStream(
			streamContext,
			description,
			method,
			options...,
		)
		if streamErr != nil {
			cancelStream()
			return nil, client.errorCodec.Decode(streamErr)
		}
		wrapped := newClientStream(
			streamContext,
			stream,
			cancelStream,
			client.errorCodec,
			client.metadataPolicy,
			client.streamBundle,
			description.ClientStreams && !description.ServerStreams,
		)
		if openErr := wrapped.open(); openErr != nil {
			_ = stream.CloseSend()
			return nil, openErr
		}
		return wrapped, nil
	})
	if client.bundle != nil {
		invoke = client.bundle.Chain()(invoke)
	}
	result, err := invoke(ctx, StreamInvocation{
		FullMethod:    method,
		ClientStreams: description.ClientStreams,
		ServerStreams: description.ServerStreams,
	})
	if err != nil {
		return nil, err
	}
	stream, ok := result.(ggrpc.ClientStream)
	if !ok {
		return nil, fmt.Errorf(
			"grpc transport: middleware returned stream type %T",
			result,
		)
	}
	return stream, nil
}

func isNilConnection(connection ggrpc.ClientConnInterface) bool {
	if connection == nil {
		return true
	}
	reflected := reflect.ValueOf(connection)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}

type mapMetadata = gmetadata.MD
