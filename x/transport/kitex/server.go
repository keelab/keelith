// Package kitex provides an experimental CloudWeGo Kitex profile.
package kitex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	kitexerrors "github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/transmeta"
	kserver "github.com/cloudwego/kitex/server"
	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"go.opentelemetry.io/otel/propagation"
)

const defaultName = "keelith.experimental.kitex"

var (
	// ErrInvalidOption means an experimental profile option is invalid.
	ErrInvalidOption = errors.New("kitex profile: invalid option")
	// ErrAlreadyStarted means Start was called more than once.
	ErrAlreadyStarted = errors.New("kitex profile: server already started")
)

// Factory creates a generated Kitex server with adapter-supplied options.
type Factory func(...kserver.Option) kserver.Server

// Option configures a Server.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

type options struct {
	name           string
	bundle         *middleware.Bundle
	exitWaitTime   time.Duration
	metadataPolicy metadata.Policy
	propagator     propagation.TextMapPropagator
	errorCodec     *ErrorCodec
	streamBundle   *middleware.StreamBundle
}

// WithName sets the stable diagnostic name.
func WithName(name string) Option {
	return optionFunc(func(options *options) error {
		normalized := strings.TrimSpace(name)
		if normalized == "" {
			return fmt.Errorf("server name is empty")
		}
		options.name = normalized
		return nil
	})
}

// WithMiddleware configures the common Middleware Bundle.
func WithMiddleware(bundle *middleware.Bundle) Option {
	return optionFunc(func(options *options) error {
		if bundle == nil {
			return fmt.Errorf("middleware bundle is nil")
		}
		options.bundle = bundle
		return nil
	})
}

// WithStreamMiddleware configures per-stream lifecycle middleware.
func WithStreamMiddleware(bundle *middleware.StreamBundle) Option {
	return optionFunc(func(options *options) error {
		if bundle == nil {
			return fmt.Errorf("stream middleware bundle is nil")
		}
		options.streamBundle = bundle
		return nil
	})
}

// WithMetadataPolicy configures default-deny inbound metadata extraction.
func WithMetadataPolicy(policy metadata.Policy) Option {
	return optionFunc(func(options *options) error {
		options.metadataPolicy = policy
		return nil
	})
}

// WithPropagator configures distributed trace-context extraction.
func WithPropagator(
	propagator propagation.TextMapPropagator,
) Option {
	return optionFunc(func(options *options) error {
		if propagator == nil {
			return fmt.Errorf("propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithErrorCodec configures framework Error projection.
func WithErrorCodec(codec *ErrorCodec) Option {
	return optionFunc(func(options *options) error {
		if codec == nil {
			return fmt.Errorf("error codec is nil")
		}
		options.errorCodec = codec
		return nil
	})
}

// WithErrorMetadata constructs an ErrorCodec with a wire allowlist.
func WithErrorMetadata(keys ...string) Option {
	snapshot := append([]string(nil), keys...)
	return optionFunc(func(options *options) error {
		codec, err := NewErrorCodec(snapshot...)
		if err != nil {
			return err
		}
		options.errorCodec = codec
		return nil
	})
}

// WithExitWaitTime sets Kitex's graceful exit budget.
func WithExitWaitTime(timeout time.Duration) Option {
	return optionFunc(func(options *options) error {
		if timeout <= 0 {
			return fmt.Errorf("exit wait time must be positive")
		}
		options.exitWaitTime = timeout
		return nil
	})
}

type state uint8

const (
	stateNew state = iota
	stateRunning
	stateStopping
	stateStopped
)

// Server adapts a generated Kitex Server to Keelith's lifecycle contract.
type Server struct {
	name     string
	listener net.Listener
	server   kserver.Server
	exit     chan error

	mu      sync.Mutex
	state   state
	runErr  error
	stopErr error

	done     chan struct{}
	started  chan struct{}
	stopDone chan struct{}
	doneOnce sync.Once
	stopOnce sync.Once
}

// New constructs an experimental Kitex Server around an owned listener.
func New(
	listener net.Listener,
	factory Factory,
	optionList ...Option,
) (*Server, error) {
	if isNilListener(listener) || factory == nil {
		return nil, fmt.Errorf("%w: listener or factory is nil", ErrInvalidOption)
	}
	defaultCodec, _ := NewErrorCodec()
	settings := options{
		name:         defaultName,
		exitWaitTime: 5 * time.Second,
		errorCodec:   defaultCodec,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	exit := make(chan error, 1)
	serverOptions := []kserver.Option{
		kserver.WithListener(listener),
		kserver.WithExitWaitTime(settings.exitWaitTime),
		kserver.WithExitSignal(func() <-chan error {
			return exit
		}),
		kserver.WithMetaHandler(transmeta.ServerTTHeaderHandler),
		kserver.WithMetaHandler(&serverStreamMetaHandler{
			policy:     settings.metadataPolicy,
			propagator: settings.propagator,
		}),
		kserver.WithMetaHandler(transmeta.MetainfoServerHandler),
		kserver.WithMiddleware(kitexMiddleware(
			settings.bundle,
			settings.metadataPolicy,
			settings.propagator,
			settings.errorCodec,
		)),
	}
	if settings.streamBundle != nil {
		serverOptions = append(
			serverOptions,
			kserver.WithStreamOptions(
				kserver.WithStreamMiddleware(
					serverStreamMiddleware(settings.streamBundle),
				),
			),
		)
	}
	rawServer := factory(serverOptions...)
	if rawServer == nil {
		_ = listener.Close()
		return nil, fmt.Errorf("%w: factory returned nil", ErrInvalidOption)
	}
	return &Server{
		name:     settings.name,
		listener: listener,
		server:   rawServer,
		exit:     exit,
		state:    stateNew,
		done:     make(chan struct{}),
		started:  make(chan struct{}),
		stopDone: make(chan struct{}),
	}, nil
}

// Name returns the experimental profile name.
func (server *Server) Name() string {
	return server.name
}

// Start waits until Kitex enters its accept loop.
func (server *Server) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	server.mu.Lock()
	if server.state != stateNew {
		server.mu.Unlock()
		return ErrAlreadyStarted
	}
	server.state = stateRunning
	server.mu.Unlock()
	go server.run()

	select {
	case <-server.started:
		select {
		case <-server.done:
			return server.Wait()
		default:
			return nil
		}
	case <-server.done:
		return server.Wait()
	case <-ctx.Done():
		_ = server.server.Stop()
		return context.Cause(ctx)
	}
}

// Stop triggers Kitex's configured graceful exit and waits for Run.
func (server *Server) Stop(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	server.stopOnce.Do(func() {
		server.mu.Lock()
		if server.state == stateNew {
			server.state = stateStopped
			server.stopErr = server.listener.Close()
			server.mu.Unlock()
			server.doneOnce.Do(func() { close(server.done) })
			close(server.stopDone)
			return
		}
		server.state = stateStopping
		server.mu.Unlock()
		server.exit <- nil
		go server.finishStop()
	})
	select {
	case <-server.stopDone:
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.stopErr
	case <-ctx.Done():
		_ = server.server.Stop()
		return context.Cause(ctx)
	}
}

// Wait blocks until Kitex Run returns.
func (server *Server) Wait() error {
	<-server.done
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.runErr
}

// Address returns the owned listener address.
func (server *Server) Address() string {
	return server.listener.Addr().String()
}

func (server *Server) run() {
	close(server.started)
	err := server.server.Run()
	server.mu.Lock()
	server.runErr = err
	if server.state != stateStopping {
		server.state = stateStopped
	}
	server.mu.Unlock()
	server.doneOnce.Do(func() { close(server.done) })
}

func (server *Server) finishStop() {
	<-server.done
	server.mu.Lock()
	server.stopErr = server.runErr
	server.state = stateStopped
	server.mu.Unlock()
	close(server.stopDone)
}

func kitexMiddleware(
	bundle *middleware.Bundle,
	policy metadata.Policy,
	propagator propagation.TextMapPropagator,
	codec *ErrorCodec,
) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request, response any) error {
			target, err := operationFromContext(ctx)
			if err != nil {
				return finishServerError(ctx, codec, err)
			}
			ctx, err = withRequestInfo(ctx, target, serverSide)
			if err != nil {
				return finishServerError(ctx, codec, err)
			}
			ctx, err = inboundContext(ctx, policy, propagator)
			if err != nil {
				return finishServerError(ctx, codec, kerrors.Wrap(
					err,
					429,
					"METADATA_TOO_LARGE",
					"request metadata is invalid",
				))
			}
			invoke := middleware.Handler(func(
				ctx context.Context,
				request any,
			) (any, error) {
				return response, next(ctx, request, response)
			})
			if bundle != nil {
				invoke = bundle.Chain()(invoke)
			}
			_, err = invoke(ctx, request)
			return finishServerError(ctx, codec, err)
		}
	}
}

func finishServerError(
	ctx context.Context,
	codec *ErrorCodec,
	err error,
) error {
	encoded := codec.Encode(err)
	if encoded == nil {
		return nil
	}
	business, ok := kitexerrors.FromBizStatusError(encoded)
	if !ok {
		return encoded
	}
	rpc := rpcinfo.GetRPCInfo(ctx)
	if rpc == nil || rpc.Invocation() == nil {
		return encoded
	}
	setter, ok := rpc.Invocation().(rpcinfo.InvocationSetter)
	if !ok {
		return encoded
	}
	setter.SetBizStatusErr(business)
	return nil
}

func operationFromContext(ctx context.Context) (operation.Operation, error) {
	rpc := rpcinfo.GetRPCInfo(ctx)
	if rpc == nil || rpc.Invocation() == nil {
		return operation.Operation{}, errors.New("kitex profile: RPCInfo is missing")
	}
	service := rpc.Invocation().ServiceName()
	if service == "" && rpc.To() != nil {
		service = rpc.To().ServiceName()
	}
	return operation.New(
		"kitex",
		service,
		rpc.Invocation().MethodName(),
		kitexOperationKind(rpc.Invocation().StreamingMode()),
	)
}

func kitexOperationKind(mode serviceinfo.StreamingMode) operation.Kind {
	switch mode {
	case serviceinfo.StreamingClient:
		return operation.KindClientStream
	case serviceinfo.StreamingServer:
		return operation.KindServerStream
	case serviceinfo.StreamingBidirectional:
		return operation.KindBidiStream
	default:
		return operation.KindUnary
	}
}

type endpointSide uint8

const (
	serverSide endpointSide = iota
	clientSide
)

func withRequestInfo(
	ctx context.Context,
	target operation.Operation,
	side endpointSide,
) (context.Context, error) {
	rpc := rpcinfo.GetRPCInfo(ctx)
	if rpc == nil {
		return nil, errors.New("kitex profile: RPCInfo is missing")
	}
	endpoint := rpc.From()
	if side == clientSide {
		endpoint = rpc.To()
	}
	options := make([]operation.RequestInfoOption, 0, 1)
	if endpoint != nil && endpoint.Address() != nil {
		address := endpoint.Address()
		peer, err := operation.NewPeer(
			address.Network(),
			address.String(),
		)
		if err != nil {
			return nil, err
		}
		options = append(options, operation.WithPeer(peer))
	}
	info, err := operation.NewRequestInfo(target, options...)
	if err != nil {
		return nil, err
	}
	return operation.WithRequestInfo(ctx, info), nil
}

func isNilListener(listener net.Listener) bool {
	if listener == nil {
		return true
	}
	value := reflect.ValueOf(listener)
	return value.Kind() == reflect.Pointer && value.IsNil()
}
