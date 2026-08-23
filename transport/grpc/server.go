package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"go.opentelemetry.io/otel/propagation"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultAddress    = "127.0.0.1:0"
	defaultServerName = "keelith.transport.grpc"
	defaultMessageMax = 4 * 1024 * 1024
)

type serverState uint8

const (
	serverStateNew serverState = iota
	serverStateStarting
	serverStateRunning
	serverStateStopping
	serverStateStopped
)

// ServerOption configures a Server.
type ServerOption interface {
	applyServer(*serverOptions) error
}

type serverOptionFunc func(*serverOptions) error

func (fn serverOptionFunc) applyServer(options *serverOptions) error {
	return fn(options)
}

type serverOptions struct {
	address          string
	addressSet       bool
	listener         net.Listener
	name             string
	bundle           *middleware.Bundle
	streamBundle     *middleware.StreamBundle
	metadataPolicy   metadata.Policy
	errorCodec       *ErrorCodec
	health           *health.Registry
	maxReceiveBytes  int
	maxSendBytes     int
	propagator       propagation.TextMapPropagator
	tlsConfig        *tls.Config
	keepalive        *ServerKeepaliveConfig
	localDiagnostics []localDiagnostic
}

// LocalDiagnosticServer is the bounded registration surface required by
// reflection and grpc-go administration services.
type LocalDiagnosticServer interface {
	ggrpc.ServiceRegistrar
	GetServiceInfo() map[string]ggrpc.ServiceInfo
}

// LocalDiagnosticRegistrar registers an opt-in local-only gRPC diagnostic.
//
// The returned cleanup runs after serving stops. Implementations must register
// synchronously and must not retain the ServiceRegistrar.
type LocalDiagnosticRegistrar func(
	LocalDiagnosticServer,
) (cleanup func(), err error)

type localDiagnostic struct {
	name     string
	register LocalDiagnosticRegistrar
}

// WithAddress sets the TCP listen address.
func WithAddress(address string) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("invalid listen address %q", address)
		}
		options.address = address
		options.addressSet = true
		return nil
	})
}

// WithListener uses listener instead of opening a TCP address.
func WithListener(listener net.Listener) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if isNilListener(listener) {
			return fmt.Errorf("listener is nil")
		}
		options.listener = listener
		return nil
	})
}

// WithName sets the stable diagnostic and health contributor name.
func WithName(name string) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		normalized := strings.TrimSpace(name)
		if normalized == "" {
			return fmt.Errorf("server name is empty")
		}
		options.name = normalized
		return nil
	})
}

// WithMiddleware configures the immutable inbound middleware Bundle.
func WithMiddleware(bundle *middleware.Bundle) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if bundle == nil {
			return fmt.Errorf("middleware bundle is nil")
		}
		options.bundle = bundle
		return nil
	})
}

// WithStreamMiddleware configures per-stream create/send/receive/finish hooks.
func WithStreamMiddleware(bundle *middleware.StreamBundle) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if bundle == nil {
			return fmt.Errorf("stream middleware bundle is nil")
		}
		options.streamBundle = bundle
		return nil
	})
}

// WithMetadataPolicy configures incoming gRPC metadata propagation.
func WithMetadataPolicy(policy metadata.Policy) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		options.metadataPolicy = policy
		return nil
	})
}

// WithPropagator configures distributed context extraction.
func WithPropagator(propagator propagation.TextMapPropagator) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if propagator == nil {
			return fmt.Errorf("propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithErrorCodec configures framework Error serialization.
func WithErrorCodec(codec *ErrorCodec) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if codec == nil {
			return fmt.Errorf("error codec is nil")
		}
		options.errorCodec = codec
		return nil
	})
}

// WithHealth exposes standard gRPC Health from registry readiness.
func WithHealth(registry *health.Registry) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if registry == nil {
			return fmt.Errorf("health registry is nil")
		}
		options.health = registry
		return nil
	})
}

// WithLocalDiagnostic registers an explicit loopback/Unix-only gRPC service.
//
// Optional reflection and admin adapters live outside the core transport.
func WithLocalDiagnostic(
	name string,
	register LocalDiagnosticRegistrar,
) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		normalized := strings.TrimSpace(name)
		if normalized != name || !validDiscoveryName(normalized) {
			return fmt.Errorf("local diagnostic name is malformed")
		}
		if register == nil {
			return fmt.Errorf("local diagnostic registrar is nil")
		}
		options.localDiagnostics = append(
			options.localDiagnostics,
			localDiagnostic{name: normalized, register: register},
		)
		return nil
	})
}

// WithMaxReceiveMessageBytes sets the decoded request message budget.
func WithMaxReceiveMessageBytes(maxBytes int) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max receive message bytes must be positive")
		}
		options.maxReceiveBytes = maxBytes
		return nil
	})
}

// WithMaxSendMessageBytes sets the encoded response message budget.
func WithMaxSendMessageBytes(maxBytes int) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max send message bytes must be positive")
		}
		options.maxSendBytes = maxBytes
		return nil
	})
}

// WithTLS enables the shared TLS/mTLS server profile.
func WithTLS(config *tls.Config) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if config == nil {
			return fmt.Errorf("TLS config is nil")
		}
		if config.MinVersion < tls.VersionTLS12 {
			return fmt.Errorf("TLS minimum version must be 1.2 or newer")
		}
		options.tlsConfig = config.Clone()
		return nil
	})
}

// Server is a grpc-go lifecycle component.
type Server struct {
	name       string
	address    string
	configured net.Listener
	grpcServer *ggrpc.Server

	mu                sync.Mutex
	state             serverState
	listener          net.Listener
	serveErr          error
	stopErr           error
	shutdownInitiated bool

	startDone chan struct{}
	done      chan struct{}
	stopDone  chan struct{}
	doneOnce  sync.Once
	stopOnce  sync.Once

	diagnosticCleanups    []func()
	diagnosticCleanupOnce sync.Once
}

// NewServer validates options and constructs a Server.
func NewServer(optionList ...ServerOption) (*Server, error) {
	defaultCodec, _ := NewErrorCodec()
	options := serverOptions{
		address:         defaultAddress,
		name:            defaultServerName,
		errorCodec:      defaultCodec,
		maxReceiveBytes: defaultMessageMax,
		maxSendBytes:    defaultMessageMax,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: server option %d is nil", ErrInvalidOption, index)
		}
		if err := option.applyServer(&options); err != nil {
			return nil, fmt.Errorf("%w: server option %d: %w", ErrInvalidOption, index, err)
		}
	}
	if options.listener != nil && options.addressSet {
		return nil, fmt.Errorf(
			"%w: address and listener are mutually exclusive",
			ErrInvalidOption,
		)
	}
	if len(options.localDiagnostics) > 0 &&
		!localDiagnosticListener(options.address, options.listener) {
		return nil, fmt.Errorf(
			"%w: local diagnostics require a loopback or Unix listener",
			ErrInvalidOption,
		)
	}

	transport := &Server{
		name:       options.name,
		address:    options.address,
		configured: options.listener,
		state:      serverStateNew,
		startDone:  make(chan struct{}),
		done:       make(chan struct{}),
		stopDone:   make(chan struct{}),
	}
	grpcOptions := []ggrpc.ServerOption{
		ggrpc.ChainUnaryInterceptor(unaryServerInterceptor(
			options.bundle,
			options.metadataPolicy,
			options.errorCodec,
			options.propagator,
		)),
		ggrpc.ChainStreamInterceptor(streamServerInterceptor(
			options.bundle,
			options.streamBundle,
			options.metadataPolicy,
			options.errorCodec,
			options.propagator,
		)),
		ggrpc.MaxRecvMsgSize(options.maxReceiveBytes),
		ggrpc.MaxSendMsgSize(options.maxSendBytes),
	}
	if options.tlsConfig != nil {
		grpcOptions = append(
			grpcOptions,
			ggrpc.Creds(credentials.NewTLS(options.tlsConfig)),
		)
	}
	if options.keepalive != nil {
		grpcOptions = append(
			grpcOptions,
			serverKeepaliveOptions(*options.keepalive)...,
		)
	}
	transport.grpcServer = ggrpc.NewServer(grpcOptions...)
	if options.health != nil {
		if err := options.health.Register(
			health.KindReadiness,
			options.name,
			transport.readiness,
		); err != nil {
			return nil, fmt.Errorf("%w: register health: %w", ErrInvalidOption, err)
		}
		grpc_health_v1.RegisterHealthServer(
			transport.grpcServer,
			&healthServer{registry: options.health},
		)
	}
	diagnosticNames := make(map[string]struct{}, len(options.localDiagnostics))
	for _, diagnostic := range options.localDiagnostics {
		if _, exists := diagnosticNames[diagnostic.name]; exists {
			transport.cleanupDiagnostics()
			return nil, fmt.Errorf(
				"%w: local diagnostic %q is duplicated",
				ErrInvalidOption,
				diagnostic.name,
			)
		}
		cleanup, err := registerLocalDiagnostic(
			transport.grpcServer,
			diagnostic,
		)
		if err != nil {
			transport.cleanupDiagnostics()
			return nil, fmt.Errorf(
				"%w: register local diagnostic %q: %w",
				ErrInvalidOption,
				diagnostic.name,
				err,
			)
		}
		diagnosticNames[diagnostic.name] = struct{}{}
		if cleanup != nil {
			transport.diagnosticCleanups = append(
				transport.diagnosticCleanups,
				cleanup,
			)
		}
	}
	return transport, nil
}

// Registrar returns the grpc-go service registrar.
func (s *Server) Registrar() ggrpc.ServiceRegistrar {
	return s.grpcServer
}

// Name returns the stable App diagnostic name.
func (s *Server) Name() string {
	return s.name
}

// Start opens or adopts a listener and begins serving in the background.
func (s *Server) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	s.mu.Lock()
	if s.state != serverStateNew {
		s.mu.Unlock()
		return ErrAlreadyStarted
	}
	s.state = serverStateStarting
	s.mu.Unlock()
	defer close(s.startDone)

	listener := s.configured
	var err error
	if listener == nil {
		var listenConfig net.ListenConfig
		listener, err = listenConfig.Listen(ctx, "tcp", s.address)
		if err != nil {
			s.completeServe(fmt.Errorf("grpc transport: listen %q: %w", s.address, err))
			return fmt.Errorf("grpc transport: listen %q: %w", s.address, err)
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		closeErr := listener.Close()
		result := errors.Join(cause, closeErr)
		s.completeServe(result)
		return result
	}

	s.mu.Lock()
	s.listener = listener
	s.state = serverStateRunning
	s.mu.Unlock()
	go s.serve(listener)
	return nil
}

// Stop gracefully drains active RPCs and force-stops at ctx deadline.
func (s *Server) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	for {
		s.mu.Lock()
		switch s.state {
		case serverStateNew:
			s.state = serverStateStopped
			s.doneOnce.Do(func() { close(s.done) })
			s.stopOnce.Do(func() { close(s.stopDone) })
			s.mu.Unlock()
			s.cleanupDiagnostics()
			return nil
		case serverStateStarting:
			startDone := s.startDone
			s.mu.Unlock()
			select {
			case <-startDone:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case serverStateRunning:
			s.state = serverStateStopping
			if !s.shutdownInitiated {
				s.shutdownInitiated = true
				go s.shutdown(ctx)
			}
			stopDone := s.stopDone
			s.mu.Unlock()
			return s.waitStop(ctx, stopDone)
		case serverStateStopping:
			stopDone := s.stopDone
			s.mu.Unlock()
			return s.waitStop(ctx, stopDone)
		case serverStateStopped:
			err := s.stopErr
			s.mu.Unlock()
			return err
		default:
			s.mu.Unlock()
			return nil
		}
	}
}

// Wait blocks until serving terminates.
func (s *Server) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serveErr
}

// Address returns the active listener address.
func (s *Server) Address() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return "", false
	}
	return s.listener.Addr().String(), true
}

func (s *Server) serve(listener net.Listener) {
	err := s.grpcServer.Serve(listener)
	if errors.Is(err, ggrpc.ErrServerStopped) {
		err = nil
	}
	s.completeServe(err)
}

func (s *Server) completeServe(err error) {
	s.mu.Lock()
	if s.serveErr == nil {
		s.serveErr = err
	}
	if s.state != serverStateStopping {
		s.state = serverStateStopped
	}
	s.mu.Unlock()
	s.cleanupDiagnostics()
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *Server) shutdown(ctx context.Context) {
	gracefulDone := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(gracefulDone)
	}()
	select {
	case <-gracefulDone:
	case <-ctx.Done():
		s.grpcServer.Stop()
		<-gracefulDone
	}
	<-s.done
	s.mu.Lock()
	s.stopErr = s.serveErr
	if cause := context.Cause(ctx); cause != nil {
		s.stopErr = errors.Join(cause, s.stopErr)
	}
	s.state = serverStateStopped
	s.mu.Unlock()
	s.stopOnce.Do(func() { close(s.stopDone) })
}

func (s *Server) waitStop(ctx context.Context, stopDone <-chan struct{}) error {
	select {
	case <-stopDone:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.stopErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (s *Server) readiness(context.Context) health.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == serverStateRunning {
		return health.Pass("gRPC listener is accepting requests")
	}
	return health.Fail("gRPC listener is not accepting requests")
}

func isNilListener(listener net.Listener) bool {
	if listener == nil {
		return true
	}
	value := reflect.ValueOf(listener)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func (s *Server) cleanupDiagnostics() {
	s.diagnosticCleanupOnce.Do(func() {
		for index := len(s.diagnosticCleanups) - 1; index >= 0; index-- {
			s.diagnosticCleanups[index]()
		}
	})
}

func registerLocalDiagnostic(
	registrar LocalDiagnosticServer,
	diagnostic localDiagnostic,
) (cleanup func(), err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			cleanup = nil
			err = fmt.Errorf("registrar panic: %v", recovered)
		}
	}()
	return diagnostic.register(registrar)
}

func localDiagnosticListener(
	address string,
	listener net.Listener,
) bool {
	if listener != nil {
		network := strings.ToLower(listener.Addr().Network())
		if network == "unix" || network == "unixpacket" {
			return true
		}
		address = listener.Addr().String()
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
