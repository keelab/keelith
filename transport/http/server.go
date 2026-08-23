package http

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/health"
)

const (
	defaultAddress         = "127.0.0.1:0"
	defaultServerName      = "keelith.transport.http"
	defaultMaxRequestBytes = int64(4 * 1024 * 1024)
	defaultMaxHeaderBytes  = 1 * 1024 * 1024
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

func (function serverOptionFunc) applyServer(options *serverOptions) error {
	return function(options)
}

// HandlerWrapper installs one application-owned outer HTTP boundary around
// the server's request and header limits plus Router. Construction fails when
// the wrapper rejects the inner handler or returns a nil handler.
type HandlerWrapper func(nethttp.Handler) (nethttp.Handler, error)

type serverOptions struct {
	address         string
	name            string
	maxRequestBytes int64
	maxHeaderBytes  int
	readTimeout     time.Duration
	readHeader      time.Duration
	writeTimeout    time.Duration
	idleTimeout     time.Duration
	health          *health.Registry
	tlsConfig       *tls.Config
	handlerWrapper  HandlerWrapper
}

// WithAddress sets the TCP listen address.
func WithAddress(address string) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("invalid listen address %q", address)
		}
		options.address = address
		return nil
	})
}

// WithName sets the stable App diagnostic and health contributor name.
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

// WithMaxRequestBodyBytes sets the request body budget.
func WithMaxRequestBodyBytes(maxBytes int64) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max request body bytes must be positive")
		}
		options.maxRequestBytes = maxBytes
		return nil
	})
}

// WithMaxHeaderBytes sets the request header budget.
func WithMaxHeaderBytes(maxBytes int) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max header bytes must be positive")
		}
		options.maxHeaderBytes = maxBytes
		return nil
	})
}

// WithReadHeaderTimeout bounds the time spent reading request headers.
func WithReadHeaderTimeout(timeout time.Duration) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("read header timeout must be positive")
		}
		options.readHeader = timeout
		return nil
	})
}

// WithReadTimeout bounds the time spent reading a complete request, including
// its body.
func WithReadTimeout(timeout time.Duration) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("read timeout must be positive")
		}
		options.readTimeout = timeout
		return nil
	})
}

// WithWriteTimeout bounds the time spent writing a response.
func WithWriteTimeout(timeout time.Duration) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("write timeout must be positive")
		}
		options.writeTimeout = timeout
		return nil
	})
}

// WithIdleTimeout bounds how long keep-alive connections remain idle.
func WithIdleTimeout(timeout time.Duration) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("idle timeout must be positive")
		}
		options.idleTimeout = timeout
		return nil
	})
}

// WithHandlerWrapper installs one outer HTTP handler wrapper at construction
// time. It is intended for application response contracts, security headers,
// and other boundaries that must also observe server-level request rejection.
func WithHandlerWrapper(wrapper HandlerWrapper) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if wrapper == nil {
			return fmt.Errorf("handler wrapper is nil")
		}
		if options.handlerWrapper != nil {
			return fmt.Errorf("handler wrapper is already configured")
		}
		options.handlerWrapper = wrapper
		return nil
	})
}

// WithHealth registers listener readiness in registry.
func WithHealth(registry *health.Registry) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if registry == nil {
			return fmt.Errorf("health registry is nil")
		}
		options.health = registry
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

// Server is a standard-library HTTP lifecycle component.
type Server struct {
	name            string
	address         string
	router          *Router
	maxRequestBytes int64
	maxHeaderBytes  int
	handler         nethttp.Handler
	httpServer      *nethttp.Server
	tlsConfig       *tls.Config

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
}

// NewServer validates options and constructs a Server.
func NewServer(router *Router, optionList ...ServerOption) (*Server, error) {
	if router == nil {
		return nil, fmt.Errorf("%w: router is nil", ErrInvalidOption)
	}
	options := serverOptions{
		address:         defaultAddress,
		name:            defaultServerName,
		maxRequestBytes: defaultMaxRequestBytes,
		maxHeaderBytes:  defaultMaxHeaderBytes,
		readHeader:      5 * time.Second,
		idleTimeout:     30 * time.Second,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: server option %d is nil", ErrInvalidOption, index)
		}
		if err := option.applyServer(&options); err != nil {
			return nil, fmt.Errorf("%w: server option %d: %w", ErrInvalidOption, index, err)
		}
	}

	transport := &Server{
		name:            options.name,
		address:         options.address,
		router:          router,
		maxRequestBytes: options.maxRequestBytes,
		maxHeaderBytes:  options.maxHeaderBytes,
		tlsConfig:       options.tlsConfig,
		state:           serverStateNew,
		startDone:       make(chan struct{}),
		done:            make(chan struct{}),
		stopDone:        make(chan struct{}),
	}
	handler := transport.baseHandler()
	if options.handlerWrapper != nil {
		var err error
		handler, err = options.handlerWrapper(handler)
		if err != nil {
			return nil, fmt.Errorf("%w: wrap HTTP handler: %w", ErrInvalidOption, err)
		}
		if isNilHTTPHandler(handler) {
			return nil, fmt.Errorf("%w: wrapped HTTP handler is nil", ErrInvalidOption)
		}
	}
	transport.handler = handler
	transport.httpServer = &nethttp.Server{
		Handler:           transport.handler,
		ReadTimeout:       options.readTimeout,
		ReadHeaderTimeout: options.readHeader,
		WriteTimeout:      options.writeTimeout,
		IdleTimeout:       options.idleTimeout,
		MaxHeaderBytes:    options.maxHeaderBytes,
		TLSConfig:         options.tlsConfig,
	}
	if options.health != nil {
		if err := options.health.Register(
			health.KindReadiness,
			options.name,
			transport.readiness,
		); err != nil {
			return nil, fmt.Errorf("%w: register health: %w", ErrInvalidOption, err)
		}
	}
	return transport, nil
}

func isNilHTTPHandler(handler nethttp.Handler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Name returns the stable App diagnostic name.
func (server *Server) Name() string {
	return server.name
}

// Handler returns the fully limited HTTP handler.
func (server *Server) Handler() nethttp.Handler {
	if server == nil {
		return nil
	}
	return server.handler
}

func (server *Server) baseHandler() nethttp.Handler {
	return nethttp.HandlerFunc(func(
		writer nethttp.ResponseWriter,
		request *nethttp.Request,
	) {
		if headerSize(request.Header) > int64(server.maxHeaderBytes) {
			server.router.writeError(request.Context(), writer, ErrHeaderTooLarge)
			return
		}
		if request.ContentLength > server.maxRequestBytes {
			server.router.writeError(request.Context(), writer, ErrRequestTooLarge)
			return
		}
		request.Body = nethttp.MaxBytesReader(
			writer,
			request.Body,
			server.maxRequestBytes,
		)
		server.router.ServeHTTP(writer, request)
	})
}

// Start listens synchronously and begins serving in the background.
func (server *Server) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	server.mu.Lock()
	if server.state != serverStateNew {
		server.mu.Unlock()
		return ErrAlreadyStarted
	}
	server.state = serverStateStarting
	server.mu.Unlock()
	defer close(server.startDone)

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", server.address)
	if err != nil {
		server.completeServe(fmt.Errorf("http transport: listen %q: %w", server.address, err))
		return fmt.Errorf("http transport: listen %q: %w", server.address, err)
	}
	if cause := context.Cause(ctx); cause != nil {
		closeErr := listener.Close()
		result := errors.Join(cause, closeErr)
		server.completeServe(result)
		return result
	}

	server.mu.Lock()
	server.listener = listener
	server.state = serverStateRunning
	server.mu.Unlock()
	go server.serve(listener)
	return nil
}

// Stop gracefully drains inflight requests. It is concurrent-safe and
// idempotent.
func (server *Server) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	for {
		server.mu.Lock()
		switch server.state {
		case serverStateNew:
			server.state = serverStateStopped
			server.doneOnce.Do(func() { close(server.done) })
			server.stopOnce.Do(func() { close(server.stopDone) })
			server.mu.Unlock()
			return nil
		case serverStateStarting:
			startDone := server.startDone
			server.mu.Unlock()
			select {
			case <-startDone:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case serverStateRunning:
			server.state = serverStateStopping
			if !server.shutdownInitiated {
				server.shutdownInitiated = true
				go server.shutdown(ctx)
			}
			stopDone := server.stopDone
			server.mu.Unlock()
			return server.waitStop(ctx, stopDone)
		case serverStateStopping:
			stopDone := server.stopDone
			server.mu.Unlock()
			return server.waitStop(ctx, stopDone)
		case serverStateStopped:
			err := server.stopErr
			server.mu.Unlock()
			return err
		default:
			server.mu.Unlock()
			return nil
		}
	}
}

// Wait blocks until serving terminates.
func (server *Server) Wait() error {
	<-server.done
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.serveErr
}

// Address returns the allocated listener address after Start.
func (server *Server) Address() (string, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.listener == nil {
		return "", false
	}
	return server.listener.Addr().String(), true
}

func (server *Server) serve(listener net.Listener) {
	var err error
	if server.tlsConfig != nil {
		err = server.httpServer.ServeTLS(listener, "", "")
	} else {
		err = server.httpServer.Serve(listener)
	}
	if errors.Is(err, nethttp.ErrServerClosed) {
		err = nil
	}
	server.completeServe(err)
}

func (server *Server) completeServe(err error) {
	server.mu.Lock()
	if server.serveErr == nil {
		server.serveErr = err
	}
	if server.state != serverStateStopping {
		server.state = serverStateStopped
	}
	server.mu.Unlock()
	server.doneOnce.Do(func() { close(server.done) })
}

func (server *Server) shutdown(ctx context.Context) {
	err := server.httpServer.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, server.httpServer.Close())
	}
	<-server.done
	server.mu.Lock()
	server.stopErr = errors.Join(err, server.serveErr)
	server.state = serverStateStopped
	server.mu.Unlock()
	server.stopOnce.Do(func() { close(server.stopDone) })
}

func (server *Server) waitStop(ctx context.Context, stopDone <-chan struct{}) error {
	select {
	case <-stopDone:
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.stopErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (server *Server) readiness(context.Context) health.Result {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.state == serverStateRunning {
		return health.Pass("HTTP listener is accepting requests")
	}
	return health.Fail("HTTP listener is not accepting requests")
}

func headerSize(header nethttp.Header) int64 {
	var size int64
	for key, values := range header {
		size += int64(len(key))
		for _, value := range values {
			size += int64(len(value))
		}
	}
	return size
}
