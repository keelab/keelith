package kitex

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	kclient "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/transmeta"
	krouter "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"go.opentelemetry.io/otel/propagation"
)

// ClientOption configures the Kitex generated-client suite.
type ClientOption interface {
	applyClient(*clientOptions) error
}

type clientOptionFunc func(*clientOptions) error

func (function clientOptionFunc) applyClient(options *clientOptions) error {
	return function(options)
}

type clientOptions struct {
	bundle         *middleware.Bundle
	metadataPolicy metadata.Policy
	propagator     propagation.TextMapPropagator
	errorCodec     *ErrorCodec
	streamBundle   *middleware.StreamBundle
	picker         krouter.Picker
}

// WithClientMiddleware configures the common outbound Middleware Bundle.
func WithClientMiddleware(bundle *middleware.Bundle) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if bundle == nil {
			return fmt.Errorf("client middleware bundle is nil")
		}
		options.bundle = bundle
		return nil
	})
}

// WithClientStreamMiddleware configures per-stream lifecycle middleware.
func WithClientStreamMiddleware(
	bundle *middleware.StreamBundle,
) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if bundle == nil {
			return fmt.Errorf("client stream middleware bundle is nil")
		}
		options.streamBundle = bundle
		return nil
	})
}

// WithClientMetadataPolicy configures default-deny outbound metadata.
func WithClientMetadataPolicy(policy metadata.Policy) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		options.metadataPolicy = policy
		return nil
	})
}

// WithClientPropagator configures distributed trace-context injection.
func WithClientPropagator(
	propagator propagation.TextMapPropagator,
) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if propagator == nil {
			return fmt.Errorf("client propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithClientErrorCodec configures framework Error restoration.
func WithClientErrorCodec(codec *ErrorCodec) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if codec == nil {
			return fmt.Errorf("client error codec is nil")
		}
		options.errorCodec = codec
		return nil
	})
}

// WithClientErrorMetadata constructs an ErrorCodec with an allowlist.
func WithClientErrorMetadata(keys ...string) ClientOption {
	snapshot := append([]string(nil), keys...)
	return clientOptionFunc(func(options *clientOptions) error {
		codec, err := NewErrorCodec(snapshot...)
		if err != nil {
			return err
		}
		options.errorCodec = codec
		return nil
	})
}

// WithClientPicker enables Keelith discovery, selection, and node feedback.
//
// Picker results use the canonical kitex://host:port endpoint form. When
// configured, the selected instance takes precedence over Kitex resolver or
// WithHostPorts results for each invocation.
func WithClientPicker(picker krouter.Picker) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if isNilClientPicker(picker) {
			return fmt.Errorf("client picker is nil")
		}
		options.picker = picker
		return nil
	})
}

// ClientSuite supplies Keelith semantics to a generated Kitex client.
//
// Pass it through kitex/client.WithSuite. Native Kitex retry remains outside
// this suite and should not be enabled together with Keelith retry policy.
type ClientSuite struct {
	bundle         *middleware.Bundle
	metadataPolicy metadata.Policy
	propagator     propagation.TextMapPropagator
	errorCodec     *ErrorCodec
	streamBundle   *middleware.StreamBundle
	picker         krouter.Picker
}

// NewClientSuite validates and constructs a generated-client option suite.
func NewClientSuite(
	optionList ...ClientOption,
) (*ClientSuite, error) {
	defaultCodec, _ := NewErrorCodec()
	options := clientOptions{errorCodec: defaultCodec}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: client option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.applyClient(&options); err != nil {
			return nil, fmt.Errorf(
				"%w: client option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	return &ClientSuite{
		bundle:         options.bundle,
		metadataPolicy: options.metadataPolicy,
		propagator:     options.propagator,
		errorCodec:     options.errorCodec,
		streamBundle:   options.streamBundle,
		picker:         options.picker,
	}, nil
}

// Options implements kitex/client.Suite.
func (suite *ClientSuite) Options() []kclient.Option {
	if suite == nil {
		return nil
	}
	options := []kclient.Option{
		kclient.WithMetaHandler(transmeta.ClientTTHeaderHandler),
		kclient.WithMetaHandler(&clientStreamMetaHandler{
			policy:     suite.metadataPolicy,
			propagator: suite.propagator,
		}),
		kclient.WithMetaHandler(transmeta.MetainfoClientHandler),
		kclient.WithMiddleware(suite.middleware()),
	}
	if suite.streamBundle != nil || suite.picker != nil {
		options = append(
			options,
			kclient.WithStreamOptions(
				kclient.WithStreamMiddleware(
					clientStreamMiddleware(
						suite.streamBundle,
						suite.errorCodec,
						suite.picker != nil,
					),
				),
			),
		)
	}
	return options
}

func (suite *ClientSuite) middleware() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(
			ctx context.Context,
			request any,
			response any,
		) (resultErr error) {
			if ctx == nil {
				return fmt.Errorf("%w: client context is nil", ErrInvalidOption)
			}
			target, err := operationFromContext(ctx)
			if err != nil {
				return err
			}
			if err := rejectNativeRetryAttempt(ctx); err != nil {
				return err
			}
			var selection *selectionState
			finishOnReturn := target.Kind() == operation.KindUnary
			defer func() {
				recovered := recover()
				if recovered != nil {
					resultErr = errors.New(
						"kitex profile: client invocation panic",
					)
					finishOnReturn = true
				}
				if finishOnReturn || resultErr != nil {
					selection.finish(ctx, resultErr)
				}
				if recovered != nil {
					panic(recovered)
				}
			}()
			if suite.picker != nil {
				ctx, selection, err = selectClientNode(
					ctx,
					target,
					suite.picker,
				)
				if err != nil {
					return err
				}
			}
			ctx, err = withRequestInfo(ctx, target, clientSide)
			if err != nil {
				return err
			}
			invoke := middleware.Handler(func(
				ctx context.Context,
				request any,
			) (any, error) {
				outbound, outboundErr := outboundContext(
					ctx,
					suite.metadataPolicy,
					suite.propagator,
				)
				if outboundErr != nil {
					return nil, outboundErr
				}
				callErr := next(outbound, request, response)
				return response, suite.errorCodec.Decode(
					clientResultError(outbound, callErr),
				)
			})
			if suite.bundle != nil {
				invoke = suite.bundle.Chain()(invoke)
			}
			_, resultErr = invoke(ctx, request)
			return resultErr
		}
	}
}

func rejectNativeRetryAttempt(ctx context.Context) error {
	rpc := rpcinfo.GetRPCInfo(ctx)
	if rpc == nil || rpc.To() == nil {
		return nil
	}
	retryText, exists := rpc.To().Tag(rpcinfo.RetryTag)
	if !exists || retryText == "" || retryText == "0" {
		return nil
	}
	retryNumber, err := strconv.ParseUint(retryText, 10, 32)
	if err != nil || retryNumber > 0 {
		return fmt.Errorf(
			"%w: call-level retry attempt %q",
			ErrNativeGovernanceConflict,
			retryText,
		)
	}
	return nil
}

func clientResultError(ctx context.Context, callErr error) error {
	if callErr != nil {
		return callErr
	}
	rpc := rpcinfo.GetRPCInfo(ctx)
	if rpc == nil || rpc.Invocation() == nil {
		return nil
	}
	return rpc.Invocation().BizStatusErr()
}
