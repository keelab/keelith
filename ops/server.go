// Package ops provides an isolated operational HTTP server.
package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/keelab/keelith/health"
)

const (
	defaultAddress      = "127.0.0.1:0"
	defaultCheckTimeout = 2 * time.Second
)

var (
	// ErrAlreadyStarted means Start was called more than once.
	ErrAlreadyStarted = errors.New("ops: server has already been started")
	// ErrInvalidOption means New received an unsafe or incomplete option.
	ErrInvalidOption = errors.New("ops: invalid option")
	// ErrNilContext means Start or Stop received a nil context.
	ErrNilContext = errors.New("ops: nil context")
)

type state uint8

const (
	stateNew state = iota
	stateStarting
	stateRunning
	stateStopping
	stateStopped
)

// Option configures an operational Server.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

type options struct {
	address           string
	checkTimeout      time.Duration
	metrics           http.Handler
	pprof             bool
	cpuProfile        http.Handler
	accessPolicy      AccessPolicy
	buildInfo         *BuildInfo
	configStatus      ConfigStatusProvider
	contractStatus    ContractStatusProvider
	runtimeStatus     RuntimeDescriptionProvider
	programmableAdmin http.Handler
	serviceProfiles   http.Handler
	loggingAdmin      http.Handler
}

// WithAddress sets the dedicated operational listen address.
func WithAddress(address string) Option {
	return optionFunc(func(options *options) error {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("invalid listen address %q", address)
		}
		options.address = address
		return nil
	})
}

// WithCheckTimeout sets a per-request health check deadline.
func WithCheckTimeout(timeout time.Duration) Option {
	return optionFunc(func(options *options) error {
		if timeout <= 0 {
			return fmt.Errorf("check timeout must be positive")
		}
		options.checkTimeout = timeout
		return nil
	})
}

// WithMetrics explicitly exposes handler at /metrics.
func WithMetrics(handler http.Handler) Option {
	return optionFunc(func(options *options) error {
		if isNilHandler(handler) {
			return fmt.Errorf("metrics handler is nil")
		}
		options.metrics = handler
		return nil
	})
}

// WithPprof explicitly exposes runtime profiles below /debug/pprof/.
func WithPprof() Option {
	return optionFunc(func(options *options) error {
		options.pprof = true
		return nil
	})
}

// WithCPUProfile explicitly exposes a bounded CPU capture handler at
// /debug/pprof/profile. The handler shares the Ops diagnostic AccessPolicy but
// is independent from WithPprof so callers cannot accidentally enable CPU
// capture by requesting heap/goroutine snapshots.
func WithCPUProfile(handler http.Handler) Option {
	return optionFunc(func(options *options) error {
		if isNilHandler(handler) {
			return fmt.Errorf("CPU profile handler is nil")
		}
		options.cpuProfile = handler
		return nil
	})
}

// WithServiceProfiles explicitly exposes a bounded static service topology at
// GET /debug/service-profiles. It shares the Ops diagnostic AccessPolicy.
func WithServiceProfiles(handler http.Handler) Option {
	return optionFunc(func(options *options) error {
		if isNilHandler(handler) {
			return fmt.Errorf("service profiles handler is nil")
		}
		options.serviceProfiles = handler
		return nil
	})
}

// Server is an isolated health, metrics, and diagnostics HTTP server.
type Server struct {
	address string

	mu            sync.Mutex
	state         state
	listener      net.Listener
	httpServer    *http.Server
	serveErr      error
	stopErr       error
	stopInitiated bool

	startDone chan struct{}
	done      chan struct{}
	stopDone  chan struct{}
	doneOnce  sync.Once
	stopOnce  sync.Once
}

// New constructs a Server. Only health endpoints are enabled by default.
func New(registry *health.Registry, optionList ...Option) (*Server, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: health registry is nil", ErrInvalidOption)
	}
	settings := options{
		address:      defaultAddress,
		checkTimeout: defaultCheckTimeout,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	if hasOptionalDiagnostics(settings) &&
		!isLoopbackAddress(settings.address) &&
		settings.accessPolicy == nil {
		return nil, fmt.Errorf(
			"%w: non-loopback diagnostics require an access policy",
			ErrInvalidOption,
		)
	}
	if settings.programmableAdmin != nil && settings.accessPolicy == nil {
		return nil, fmt.Errorf(
			"%w: programmable admin requires an access policy",
			ErrInvalidOption,
		)
	}
	if settings.loggingAdmin != nil && settings.accessPolicy == nil {
		return nil, fmt.Errorf(
			"%w: logging admin requires an access policy",
			ErrInvalidOption,
		)
	}

	mux := http.NewServeMux()
	registerHealth(mux, registry, settings.checkTimeout)
	if settings.metrics != nil {
		mux.Handle(
			"GET /metrics",
			protectDiagnostic(settings.accessPolicy, settings.metrics),
		)
	}
	if settings.pprof {
		pprofMux := http.NewServeMux()
		registerPprof(pprofMux)
		mux.Handle(
			"GET /debug/pprof/",
			protectDiagnostic(settings.accessPolicy, pprofMux),
		)
	}
	if settings.cpuProfile != nil {
		mux.Handle(
			"GET /debug/pprof/profile",
			protectDiagnostic(
				settings.accessPolicy,
				settings.cpuProfile,
			),
		)
	}
	if settings.buildInfo != nil {
		mux.Handle(
			"GET /debug/build",
			protectDiagnostic(
				settings.accessPolicy,
				buildInfoHandler(*settings.buildInfo),
			),
		)
	}
	if settings.configStatus != nil {
		mux.Handle(
			"GET /debug/config",
			protectDiagnostic(
				settings.accessPolicy,
				configStatusHandler(settings.configStatus),
			),
		)
	}
	if settings.contractStatus != nil {
		mux.Handle(
			"GET /debug/contracts",
			protectDiagnostic(
				settings.accessPolicy,
				contractStatusHandler(settings.contractStatus),
			),
		)
	}
	if settings.runtimeStatus != nil {
		mux.Handle(
			"GET /debug/runtime",
			protectDiagnostic(
				settings.accessPolicy,
				runtimeStatusHandler(settings.runtimeStatus),
			),
		)
	}
	if settings.serviceProfiles != nil {
		mux.Handle(
			"GET /debug/service-profiles",
			protectDiagnostic(settings.accessPolicy, settings.serviceProfiles),
		)
	}
	if settings.programmableAdmin != nil {
		protected := protectDiagnostic(settings.accessPolicy, settings.programmableAdmin)
		mux.Handle("POST /admin/programmable/continuation/collect", protected)
		mux.Handle("POST /admin/programmable/projection/compact", protected)
		mux.Handle("POST /admin/programmable/projection/force-snapshot", protected)
	}
	if settings.loggingAdmin != nil {
		protected := protectDiagnostic(settings.accessPolicy, settings.loggingAdmin)
		mux.Handle("GET /debug/logging", protected)
		mux.Handle("PUT /admin/logging/level", protected)
	}

	return &Server{
		address:   settings.address,
		state:     stateNew,
		startDone: make(chan struct{}),
		done:      make(chan struct{}),
		stopDone:  make(chan struct{}),
		httpServer: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
	}, nil
}

// Name returns the stable App diagnostic name.
func (server *Server) Name() string {
	return "keelith.ops"
}

// Start listens synchronously and then serves in the background.
func (server *Server) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	server.mu.Lock()
	if server.state != stateNew {
		server.mu.Unlock()
		return ErrAlreadyStarted
	}
	server.state = stateStarting
	server.mu.Unlock()
	defer close(server.startDone)

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", server.address)
	if err != nil {
		server.completeServe(fmt.Errorf("ops: listen %q: %w", server.address, err))
		return fmt.Errorf("ops: listen %q: %w", server.address, err)
	}
	if cause := context.Cause(ctx); cause != nil {
		closeErr := listener.Close()
		result := errors.Join(cause, closeErr)
		server.completeServe(result)
		return result
	}

	server.mu.Lock()
	server.listener = listener
	server.state = stateRunning
	server.mu.Unlock()

	go server.serve(listener)
	return nil
}

// Stop gracefully shuts down the operational listener. It is concurrent-safe
// and idempotent.
func (server *Server) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	for {
		server.mu.Lock()
		current := server.state
		switch current {
		case stateNew:
			server.mu.Unlock()
			return nil
		case stateStarting:
			startDone := server.startDone
			server.mu.Unlock()
			select {
			case <-startDone:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case stateRunning, stateStopped:
			if !server.stopInitiated {
				server.stopInitiated = true
				server.state = stateStopping
				go server.shutdown(ctx)
			}
			stopDone := server.stopDone
			server.mu.Unlock()
			return waitForStop(ctx, stopDone, server.stopError)
		case stateStopping:
			stopDone := server.stopDone
			server.mu.Unlock()
			return waitForStop(ctx, stopDone, server.stopError)
		default:
			server.mu.Unlock()
			return nil
		}
	}
}

// Wait blocks until the listener terminates and returns its runtime error.
func (server *Server) Wait() error {
	<-server.done
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.serveErr
}

// Address returns the allocated listener address after Start succeeds.
func (server *Server) Address() (string, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.listener == nil {
		return "", false
	}
	return server.listener.Addr().String(), true
}

func (server *Server) serve(listener net.Listener) {
	err := server.httpServer.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	server.completeServe(err)
}

func (server *Server) completeServe(err error) {
	server.mu.Lock()
	server.serveErr = err
	if server.state == stateStarting || server.state == stateRunning {
		server.state = stateStopped
	}
	server.doneOnce.Do(func() {
		close(server.done)
	})
	server.mu.Unlock()
}

func (server *Server) shutdown(ctx context.Context) {
	shutdownErr := server.httpServer.Shutdown(ctx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.httpServer.Close())
	}
	<-server.done

	server.mu.Lock()
	server.stopErr = shutdownErr
	server.state = stateStopped
	server.stopOnce.Do(func() {
		close(server.stopDone)
	})
	server.mu.Unlock()
}

func (server *Server) stopError() error {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.stopErr
}

func waitForStop(
	ctx context.Context,
	stopDone <-chan struct{},
	result func() error,
) error {
	select {
	case <-stopDone:
		return result()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func registerHealth(
	mux *http.ServeMux,
	registry *health.Registry,
	timeout time.Duration,
) {
	endpoints := map[string]health.Kind{
		"GET /health/startup":      health.KindStartup,
		"GET /health/live":         health.KindLiveness,
		"GET /health/ready":        health.KindReadiness,
		"GET /health/dependencies": health.KindDependency,
	}
	for pattern, kind := range endpoints {
		mux.Handle(pattern, healthHandler(registry, kind, timeout))
	}
}

func healthHandler(
	registry *health.Registry,
	kind health.Kind,
	timeout time.Duration,
) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), timeout)
		defer cancel()
		report := registry.Check(ctx, kind)

		writer.Header().Set("Content-Type", "application/json")
		if report.Status != health.StatusPass {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(writer).Encode(report)
	})
}

func isNilHandler(handler http.Handler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func hasOptionalDiagnostics(settings options) bool {
	return settings.metrics != nil ||
		settings.pprof ||
		settings.cpuProfile != nil ||
		settings.buildInfo != nil ||
		settings.configStatus != nil ||
		settings.contractStatus != nil ||
		settings.runtimeStatus != nil ||
		settings.serviceProfiles != nil ||
		settings.loggingAdmin != nil ||
		settings.programmableAdmin != nil
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	addressIP := net.ParseIP(host)
	return addressIP != nil && addressIP.IsLoopback()
}
