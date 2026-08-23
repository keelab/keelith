// Package hertz provides an experimental CloudWeGo Hertz profile.
package hertz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	keelithws "github.com/keelab/keelith/transport/websocket"
	"go.opentelemetry.io/otel/propagation"
)

const (
	defaultName             = "keelith.experimental.hertz"
	defaultMaxHeaderBytes   = 1 * 1024 * 1024
	defaultMaxRequestBytes  = 4 * 1024 * 1024
	defaultMaxResponseBytes = 4 * 1024 * 1024
)

var (
	// ErrInvalidOption means an experimental profile option is invalid.
	ErrInvalidOption = errors.New("hertz profile: invalid option")
	// ErrAlreadyStarted means Start was called more than once.
	ErrAlreadyStarted = errors.New("hertz profile: server already started")
	// ErrRequestTooLarge means an inbound body exceeded its configured budget.
	ErrRequestTooLarge = errors.New("hertz profile: request body too large")
	// ErrResponseTooLarge means an ordinary response exceeded its budget.
	ErrResponseTooLarge = errors.New("hertz profile: response body too large")
	// ErrHeaderTooLarge means propagated inbound headers exceeded their budget.
	ErrHeaderTooLarge = errors.New("hertz profile: headers too large")
)

// Decoder converts a Hertz request to the common Middleware request.
type Decoder func(*app.RequestContext) (any, error)

// Encoder writes a common Middleware response through Hertz.
type Encoder func(context.Context, *app.RequestContext, any) error

// DecodeJSON returns a strict JSON Decoder for T.
func DecodeJSON[T any]() Decoder {
	return func(request *app.RequestContext) (any, error) {
		var value T
		decoder := json.NewDecoder(bytes.NewReader(request.Request.Body()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				return nil, errors.New(
					"hertz profile: request contains multiple JSON values",
				)
			}
			return nil, err
		}
		return value, nil
	}
}

// EncodeJSON writes a JSON response.
func EncodeJSON(
	_ context.Context,
	request *app.RequestContext,
	response any,
) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	request.Data(
		consts.StatusOK,
		"application/json; charset=utf-8",
		payload,
	)
	return nil
}

// Option configures a Server.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

// RouteOption configures one Hertz route.
type RouteOption interface {
	applyRoute(*routeOptions) error
}

type routeOptionFunc func(*routeOptions) error

func (function routeOptionFunc) applyRoute(options *routeOptions) error {
	return function(options)
}

type routeOptions struct {
	streaming bool
}

// WithStreaming keeps Middleware active while the Encoder owns the response
// stream and disables ordinary response buffering checks.
func WithStreaming() RouteOption {
	return routeOptionFunc(func(options *routeOptions) error {
		options.streaming = true
		return nil
	})
}

type options struct {
	name              string
	bundle            *middleware.Bundle
	exitWaitTime      time.Duration
	metadataPolicy    metadata.Policy
	propagator        propagation.TextMapPropagator
	maxHeaderBytes    int
	maxRequestBytes   int
	maxResponseBytes  int
	errorMetadataKeys []string
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

// WithMetadataPolicy configures default-deny inbound header propagation.
func WithMetadataPolicy(policy metadata.Policy) Option {
	return optionFunc(func(options *options) error {
		options.metadataPolicy = policy
		return nil
	})
}

// WithPropagator configures distributed trace-context extraction.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return optionFunc(func(options *options) error {
		if propagator == nil {
			return fmt.Errorf("propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithMaxHeaderBytes limits the complete parsed HTTP header block.
func WithMaxHeaderBytes(maxBytes int) Option {
	return optionFunc(func(options *options) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max header bytes must be positive")
		}
		options.maxHeaderBytes = maxBytes
		return nil
	})
}

// WithMaxRequestBytes limits ordinary request bodies.
func WithMaxRequestBytes(maxBytes int) Option {
	return optionFunc(func(options *options) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max request bytes must be positive")
		}
		options.maxRequestBytes = maxBytes
		return nil
	})
}

// WithMaxResponseBytes limits buffered ordinary responses and error envelopes.
func WithMaxResponseBytes(maxBytes int) Option {
	return optionFunc(func(options *options) error {
		if maxBytes < 256 {
			return fmt.Errorf("max response bytes must be at least 256")
		}
		options.maxResponseBytes = maxBytes
		return nil
	})
}

// WithErrorMetadata allows selected framework Error metadata on the wire.
func WithErrorMetadata(keys ...string) Option {
	snapshot := append([]string(nil), keys...)
	return optionFunc(func(options *options) error {
		normalized, err := normalizeErrorMetadataKeys(snapshot)
		if err != nil {
			return err
		}
		options.errorMetadataKeys = normalized
		return nil
	})
}

// WithExitWaitTime sets Hertz's graceful exit budget.
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

// Server adapts a Hertz Engine to Keelith's lifecycle contract.
type Server struct {
	name      string
	listener  *readyListener
	engine    *hserver.Hertz
	bundle    *middleware.Bundle
	semantics *requestSemantics

	mu      sync.Mutex
	state   state
	runErr  error
	stopErr error

	done     chan struct{}
	stopDone chan struct{}
	doneOnce sync.Once
	stopOnce sync.Once

	routeRegistry *routeRegistry
	websocketHubs map[*keelithws.Hub]struct{}
}

type requestSemantics struct {
	metadata          metadata.Policy
	propagator        propagation.TextMapPropagator
	maxRequestBytes   int
	maxResponseBytes  int
	errorMetadata     map[string]struct{}
	errorMetadataKeys []string
}

type routeRegistry struct {
	mu     sync.Mutex
	routes map[string]struct{}
}

// New constructs an experimental Hertz Server around an owned listener.
func New(listener net.Listener, optionList ...Option) (*Server, error) {
	if isNilListener(listener) {
		return nil, fmt.Errorf("%w: listener is nil", ErrInvalidOption)
	}
	settings := options{
		name:             defaultName,
		exitWaitTime:     5 * time.Second,
		maxHeaderBytes:   defaultMaxHeaderBytes,
		maxRequestBytes:  defaultMaxRequestBytes,
		maxResponseBytes: defaultMaxResponseBytes,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	ready := newReadyListener(listener)
	engine := hserver.New(
		hserver.WithListener(ready),
		hserver.WithTransport(standard.NewTransporter),
		hserver.WithExitWaitTime(settings.exitWaitTime),
		hserver.WithMaxHeaderBytes(settings.maxHeaderBytes),
		hserver.WithMaxRequestBodySize(settings.maxRequestBytes),
		hserver.WithDisablePrintRoute(true),
	)
	allowedErrorMetadata := make(
		map[string]struct{},
		len(settings.errorMetadataKeys),
	)
	for _, key := range settings.errorMetadataKeys {
		allowedErrorMetadata[key] = struct{}{}
	}
	return &Server{
		name:     settings.name,
		listener: ready,
		engine:   engine,
		bundle:   settings.bundle,
		semantics: &requestSemantics{
			metadata:         settings.metadataPolicy,
			propagator:       settings.propagator,
			maxRequestBytes:  settings.maxRequestBytes,
			maxResponseBytes: settings.maxResponseBytes,
			errorMetadata:    allowedErrorMetadata,
			errorMetadataKeys: append(
				[]string(nil),
				settings.errorMetadataKeys...,
			),
		},
		state:    stateNew,
		done:     make(chan struct{}),
		stopDone: make(chan struct{}),
		routeRegistry: &routeRegistry{
			routes: make(map[string]struct{}),
		},
		websocketHubs: make(map[*keelithws.Hub]struct{}),
	}, nil
}

// Handle registers one Hertz route with a stable HTTP Operation.
func (server *Server) Handle(
	method string,
	path string,
	target operation.Operation,
	decoder Decoder,
	handler middleware.Handler,
	encoder Encoder,
	optionList ...RouteOption,
) error {
	normalizedMethod := strings.ToUpper(strings.TrimSpace(method))
	if target.Transport() != "http" ||
		normalizedMethod == "" ||
		!strings.HasPrefix(path, "/") ||
		decoder == nil ||
		handler == nil ||
		encoder == nil {
		return fmt.Errorf("%w: route is incomplete", ErrInvalidOption)
	}
	settings := routeOptions{}
	for index, option := range optionList {
		if option == nil {
			return fmt.Errorf(
				"%w: route option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.applyRoute(&settings); err != nil {
			return fmt.Errorf(
				"%w: route option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	if settings.streaming &&
		target.Kind() != operation.KindServerStream {
		return fmt.Errorf(
			"%w: streaming route requires server-stream operation",
			ErrInvalidOption,
		)
	}
	var invoke middleware.Handler
	if !settings.streaming {
		invoke = handler
		if server.bundle != nil {
			invoke = server.bundle.Chain()(handler)
		}
	}
	routeHandler := func(
		ctx context.Context,
		request *app.RequestContext,
	) {
		if server.semantics.propagator != nil {
			ctx = server.semantics.propagator.Extract(
				ctx,
				hertzPropagationCarrier{header: &request.Request.Header},
			)
		}
		info, err := requestInfo(target, request.RemoteAddr())
		if err != nil {
			server.writeError(request, err)
			return
		}
		ctx = operation.WithRequestInfo(ctx, info)
		inbound, err := server.semantics.metadata.Extract(
			hertzMetadataCarrier{header: &request.Request.Header},
		)
		if err != nil {
			server.writeError(
				request,
				fmt.Errorf("%w: %w", ErrHeaderTooLarge, err),
			)
			return
		}
		ctx = metadata.WithInbound(ctx, inbound)
		if len(request.Request.Body()) > server.semantics.maxRequestBytes {
			server.writeError(request, ErrRequestTooLarge)
			return
		}
		decoded, err := decoder(request)
		if err != nil {
			server.writeError(
				request,
				kerrors.Wrap(
					err,
					consts.StatusBadRequest,
					"INVALID_REQUEST",
					"request body is invalid",
				),
			)
			return
		}
		if settings.streaming {
			streamHandler := middleware.Handler(func(
				ctx context.Context,
				decoded any,
			) (any, error) {
				response, err := handler(ctx, decoded)
				if err != nil {
					return nil, err
				}
				return nil, encoder(ctx, request, response)
			})
			if server.bundle != nil {
				streamHandler = server.bundle.Chain()(streamHandler)
			}
			if _, err := streamHandler(ctx, decoded); err != nil &&
				request.Response.GetHijackWriter() == nil {
				server.writeError(request, err)
			}
			return
		}

		response, err := invoke(ctx, decoded)
		if err != nil {
			server.writeError(request, err)
			return
		}
		if err := encoder(ctx, request, response); err != nil {
			server.writeError(request, err)
			return
		}
		if len(request.Response.Body()) > server.semantics.maxResponseBytes {
			server.writeError(request, ErrResponseTooLarge)
		}
	}
	return server.addRoute(normalizedMethod, path, routeHandler)
}

func (server *Server) addRoute(
	method string,
	path string,
	handler app.HandlerFunc,
) error {
	return server.addRouteWith(method, path, handler, nil)
}

func (server *Server) addRouteWith(
	method string,
	path string,
	handler app.HandlerFunc,
	registered func(),
) error {
	if server == nil || handler == nil {
		return fmt.Errorf("%w: server or route handler is nil", ErrInvalidOption)
	}
	routeKey := method + " " + path
	server.routeRegistry.mu.Lock()
	defer server.routeRegistry.mu.Unlock()
	if _, duplicate := server.routeRegistry.routes[routeKey]; duplicate {
		return fmt.Errorf("%w: duplicate route %s", ErrInvalidOption, routeKey)
	}
	server.mu.Lock()
	currentState := server.state
	server.mu.Unlock()
	if currentState != stateNew {
		return fmt.Errorf(
			"%w: routes cannot change after server start",
			ErrInvalidOption,
		)
	}
	if err := server.registerRoute(method, path, handler); err != nil {
		return err
	}
	if registered != nil {
		registered()
	}
	server.routeRegistry.routes[routeKey] = struct{}{}
	return nil
}

func (server *Server) registerRoute(
	method string,
	path string,
	handler app.HandlerFunc,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"%w: register %s %s: %v",
				ErrInvalidOption,
				method,
				path,
				recovered,
			)
		}
	}()
	server.engine.Handle(method, path, handler)
	return nil
}

// Name returns the experimental profile name.
func (server *Server) Name() string {
	return server.name
}

// Start waits until Hertz enters its accept loop.
func (server *Server) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	server.routeRegistry.mu.Lock()
	server.mu.Lock()
	if server.state != stateNew {
		server.mu.Unlock()
		server.routeRegistry.mu.Unlock()
		return ErrAlreadyStarted
	}
	for hub := range server.websocketHubs {
		if !hub.Describe().Ready {
			server.mu.Unlock()
			server.routeRegistry.mu.Unlock()
			return fmt.Errorf(
				"%w: websocket hub %q is not running",
				ErrInvalidOption,
				hub.Name(),
			)
		}
	}
	server.state = stateRunning
	server.mu.Unlock()
	server.routeRegistry.mu.Unlock()
	go server.run()

	select {
	case <-server.listener.ready:
		return nil
	case <-server.done:
		return server.Wait()
	case <-ctx.Done():
		_ = server.engine.Close()
		return context.Cause(ctx)
	}
}

// Stop gracefully shuts down Hertz and waits for Run to return.
func (server *Server) Stop(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	server.stopOnce.Do(func() {
		server.mu.Lock()
		if server.state == stateNew {
			server.state = stateStopped
			server.mu.Unlock()
			server.stopErr = errors.Join(
				server.stopWebSocketHubs(ctx),
				server.listener.Close(),
			)
			server.doneOnce.Do(func() { close(server.done) })
			close(server.stopDone)
			return
		}
		server.state = stateStopping
		server.mu.Unlock()
		go server.shutdown(ctx)
	})
	select {
	case <-server.stopDone:
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.stopErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Wait blocks until Hertz Run returns.
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
	err := server.engine.Run()
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	server.mu.Lock()
	if server.state == stateStopping && err != nil {
		err = nil
	}
	server.runErr = err
	if server.state != stateStopping {
		server.state = stateStopped
	}
	server.mu.Unlock()
	server.doneOnce.Do(func() { close(server.done) })
}

func (server *Server) shutdown(ctx context.Context) {
	hubErr := server.stopWebSocketHubs(ctx)
	err := server.engine.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, server.engine.Close())
	}
	<-server.done
	server.mu.Lock()
	server.stopErr = errors.Join(hubErr, err, server.runErr)
	server.state = stateStopped
	server.mu.Unlock()
	close(server.stopDone)
}

func (server *Server) stopWebSocketHubs(ctx context.Context) error {
	server.routeRegistry.mu.Lock()
	hubs := make([]*keelithws.Hub, 0, len(server.websocketHubs))
	for hub := range server.websocketHubs {
		hubs = append(hubs, hub)
	}
	server.routeRegistry.mu.Unlock()
	if len(hubs) == 0 {
		return nil
	}
	results := make(chan error, len(hubs))
	for _, hub := range hubs {
		go func(hub *keelithws.Hub) {
			results <- hub.Stop(ctx)
		}(hub)
	}
	var result error
	for range hubs {
		result = errors.Join(result, <-results)
	}
	return result
}

func isNilListener(listener net.Listener) bool {
	if listener == nil {
		return true
	}
	value := reflect.ValueOf(listener)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

type readyListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func newReadyListener(listener net.Listener) *readyListener {
	return &readyListener{
		Listener: listener,
		ready:    make(chan struct{}),
	}
}

func (listener *readyListener) Accept() (net.Conn, error) {
	listener.once.Do(func() {
		close(listener.ready)
	})
	return listener.Listener.Accept()
}
