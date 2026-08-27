package xds

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/secret"
	"github.com/keelab/keelith/transport/tlsconfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	defaultRotationReadyTimeout = 10 * time.Second
	minimumRotationReadyTimeout = 100 * time.Millisecond
	maximumRotationReadyTimeout = 5 * time.Minute
	rotationRetryInitial        = 250 * time.Millisecond
	rotationRetryMaximum        = 5 * time.Second
	maximumTargetLength         = 2_048
)

var (
	// ErrRuntimeNotStarted reports a Watch before the managed connection
	// runtime has entered the App component lifecycle.
	ErrRuntimeNotStarted = errors.New("xds registry: runtime not started")
	// ErrConnectionUnavailable reports that a replacement control-plane
	// connection did not become ready within its bounded attempt.
	ErrConnectionUnavailable = errors.New("xds registry: connection unavailable")
)

// ManagedOptions configure the owned ads connection and validated credential
// rotation sources. DialOptions must include explicit transport credentials.
type ManagedOptions struct {
	Target               string
	DialOptions          []grpc.DialOption
	TLSReloader          *tlsconfig.Reloader
	UpdateSources        []secret.UpdateSource
	RotationReadyTimeout time.Duration
}

// RuntimeDescription is a bounded, material-free managed discovery snapshot.
type RuntimeDescription struct {
	Started          bool
	Closed           bool
	Rotating         bool
	Degraded         bool
	RotationEnabled  bool
	Watchers         int
	Resources        int
	Responses        uint64
	Accepted         uint64
	Rejected         uint64
	Expired          uint64
	Rotations        uint64
	RotationFailures uint64
}

// Runtime owns one replaceable gRPC connection and presents a stable
// registry.Discovery facade. After a successful tls material update it first
// establishes a ready replacement connection, atomically publishes the new ads
// client, then retires old streams. Routers retain their last-good snapshot and
// reconnect through the stable facade.
type Runtime struct {
	options       Options
	managed       ManagedOptions
	newConnection func() (*grpc.ClientConn, error)

	mu            sync.RWMutex
	client        *Client
	connection    *grpc.ClientConn
	started       bool
	closed        bool
	rotating      bool
	degraded      bool
	subscriptions []secret.UpdateSubscription
	cancel        context.CancelFunc
	done          chan struct{}
	loopStarted   bool

	retiredResponses uint64
	retiredAccepted  uint64
	retiredRejected  uint64
	retiredExpired   uint64
	rotations        uint64
	rotationFailures uint64

	closeOnce sync.Once
	closeErr  error
}

// OpenRuntime constructs an owned, replaceable ads connection without opening
// network traffic. Start joins the App component lifecycle and begins rotation
// observation after credential lifecycles have loaded initial material.
func OpenRuntime(
	options Options,
	managed ManagedOptions,
) (*Runtime, error) {
	normalized, normalizedManaged, err := normalizeManagedOptions(options, managed)
	if err != nil {
		return nil, err
	}
	factory := func() (*grpc.ClientConn, error) {
		return grpc.NewClient(
			normalizedManaged.Target,
			normalizedManaged.DialOptions...,
		)
	}
	return newRuntime(normalized, normalizedManaged, factory)
}

func newRuntime(
	options Options,
	managed ManagedOptions,
	factory func() (*grpc.ClientConn, error),
) (*Runtime, error) {
	if factory == nil {
		return nil, fmt.Errorf("%w: connection factory is nil", ErrInvalidOption)
	}
	connection, client, err := newRuntimeCandidate(options, factory)
	if err != nil {
		return nil, err
	}
	return &Runtime{
		options:       options,
		managed:       managed,
		newConnection: factory,
		client:        client,
		connection:    connection,
		done:          make(chan struct{}),
	}, nil
}

// Start begins managed connection ownership and update observation.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return ErrClosed
	}
	if runtime.started {
		runtime.mu.Unlock()
		return fmt.Errorf("%w: runtime already started", ErrInvalidOption)
	}
	for _, source := range runtime.managed.UpdateSources {
		if !source.Ready() {
			runtime.mu.Unlock()
			return fmt.Errorf(
				"%w: credential material is not ready",
				ErrInvalidOption,
			)
		}
	}
	subscriptions := make(
		[]secret.UpdateSubscription,
		0,
		len(runtime.managed.UpdateSources),
	)
	for _, source := range runtime.managed.UpdateSources {
		subscription, err := source.SubscribeUpdates()
		if err != nil {
			for _, current := range subscriptions {
				current.Close()
			}
			runtime.mu.Unlock()
			return err
		}
		subscriptions = append(subscriptions, subscription)
	}
	runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	runtime.subscriptions = subscriptions
	runtime.cancel = cancel
	runtime.started = true
	runtime.loopStarted = len(subscriptions) != 0
	connection := runtime.connection
	runtime.mu.Unlock()

	connection.Connect()
	if len(subscriptions) != 0 {
		go runtime.run(runContext, subscriptions)
	}
	return nil
}

// Watch opens a subscription through the currently active ads client.
func (runtime *Runtime) Watch(
	ctx context.Context,
	service string,
) (registry.Watcher, error) {
	if runtime == nil || ctx == nil {
		return nil, fmt.Errorf("%w: runtime or context is nil", ErrInvalidOption)
	}
	runtime.mu.RLock()
	if runtime.closed {
		runtime.mu.RUnlock()
		return nil, ErrClosed
	}
	if !runtime.started {
		runtime.mu.RUnlock()
		return nil, ErrRuntimeNotStarted
	}
	client := runtime.client
	runtime.mu.RUnlock()
	return client.Watch(ctx, service)
}

// Shutdown stops rotation, closes active ads streams, and closes the owned
// control-plane connection. It is safe to call repeatedly.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	runtime.closeOnce.Do(func() {
		runtime.closeErr = runtime.shutdown(ctx)
	})
	return runtime.closeErr
}

// Describe returns bounded lifecycle and rotation state without target, TLS
// material, node, resource names, versions, nonces, endpoints, or errors.
func (runtime *Runtime) Describe() RuntimeDescription {
	if runtime == nil {
		return RuntimeDescription{Closed: true}
	}
	runtime.mu.RLock()
	client := runtime.client
	description := RuntimeDescription{
		Started:          runtime.started,
		Closed:           runtime.closed,
		Rotating:         runtime.rotating,
		Degraded:         runtime.degraded,
		RotationEnabled:  len(runtime.managed.UpdateSources) != 0,
		Responses:        runtime.retiredResponses,
		Accepted:         runtime.retiredAccepted,
		Rejected:         runtime.retiredRejected,
		Expired:          runtime.retiredExpired,
		Rotations:        runtime.rotations,
		RotationFailures: runtime.rotationFailures,
	}
	if client == nil {
		runtime.mu.RUnlock()
		return description
	}
	current := client.Describe()
	runtime.mu.RUnlock()
	description.Watchers = current.Watchers
	description.Resources = current.Resources
	description.Responses += current.Responses
	description.Accepted += current.Accepted
	description.Rejected += current.Rejected
	description.Expired += current.Expired
	description.Degraded = description.Degraded || client.degraded()
	return description
}

func (runtime *Runtime) run(
	ctx context.Context,
	subscriptions []secret.UpdateSubscription,
) {
	defer close(runtime.done)
	updates := mergeUpdates(ctx, subscriptions)
	for {
		select {
		case <-ctx.Done():
			return
		case _, open := <-updates:
			if !open {
				return
			}
		}
		if runtime.rotateUntilReady(ctx, updates) != nil {
			return
		}
	}
}

func (runtime *Runtime) rotateUntilReady(
	ctx context.Context,
	updates <-chan struct{},
) error {
	delay := rotationRetryInitial
	for {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		runtime.setRotating(true)
		err := runtime.rotate(ctx)
		runtime.setRotating(false)
		if err == nil {
			runtime.mu.Lock()
			runtime.degraded = false
			runtime.rotations++
			runtime.mu.Unlock()
			return nil
		}
		if context.Cause(ctx) != nil {
			return context.Cause(ctx)
		}
		runtime.mu.Lock()
		runtime.degraded = true
		runtime.rotationFailures++
		runtime.mu.Unlock()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopAndDrainTimer(timer)
			return context.Cause(ctx)
		case _, open := <-updates:
			stopAndDrainTimer(timer)
			if !open {
				return ErrClosed
			}
			delay = rotationRetryInitial
		case <-timer.C:
			delay *= 2
			if delay > rotationRetryMaximum {
				delay = rotationRetryMaximum
			}
		}
	}
}

func (runtime *Runtime) rotate(ctx context.Context) error {
	connection, client, err := newRuntimeCandidate(
		runtime.options,
		runtime.newConnection,
	)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if keep {
			return
		}
		_ = client.Shutdown(context.Background())
		_ = connection.Close()
	}()
	connection.Connect()
	readyContext, cancel := context.WithTimeout(
		ctx,
		runtime.managed.RotationReadyTimeout,
	)
	err = waitForReady(readyContext, connection)
	cancel()
	if err != nil {
		return err
	}

	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return ErrClosed
	}
	previousClient := runtime.client
	previousConnection := runtime.connection
	previousDescription := previousClient.Describe()
	runtime.retiredResponses += previousDescription.Responses
	runtime.retiredAccepted += previousDescription.Accepted
	runtime.retiredRejected += previousDescription.Rejected
	runtime.retiredExpired += previousDescription.Expired
	runtime.client = client
	runtime.connection = connection
	runtime.mu.Unlock()
	keep = true

	_ = previousClient.Shutdown(context.Background())
	_ = previousConnection.Close()
	return nil
}

func (runtime *Runtime) shutdown(ctx context.Context) error {
	runtime.mu.Lock()
	runtime.closed = true
	runtime.started = false
	cancel := runtime.cancel
	subscriptions := append(
		[]secret.UpdateSubscription(nil),
		runtime.subscriptions...,
	)
	loopStarted := runtime.loopStarted
	done := runtime.done
	runtime.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	for _, subscription := range subscriptions {
		subscription.Close()
	}
	var shutdownErr error
	if loopStarted {
		select {
		case <-done:
		case <-ctx.Done():
			shutdownErr = errors.Join(shutdownErr, context.Cause(ctx))
		}
	}
	runtime.mu.RLock()
	client := runtime.client
	connection := runtime.connection
	runtime.mu.RUnlock()
	if client != nil {
		shutdownErr = errors.Join(
			shutdownErr,
			client.Shutdown(context.Background()),
		)
	}
	if connection != nil {
		shutdownErr = errors.Join(shutdownErr, connection.Close())
	}
	return shutdownErr
}

func (runtime *Runtime) setRotating(rotating bool) {
	runtime.mu.Lock()
	runtime.rotating = rotating
	runtime.mu.Unlock()
}

func newRuntimeCandidate(
	options Options,
	factory func() (*grpc.ClientConn, error),
) (*grpc.ClientConn, *Client, error) {
	connection, err := factory()
	if err != nil {
		return nil, nil, fmt.Errorf("xds registry: create connection: %w", err)
	}
	if connection == nil {
		return nil, nil, fmt.Errorf("%w: connection factory returned nil", ErrInvalidOption)
	}
	client, err := New(connection, options)
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	return connection, client, nil
}

func normalizeManagedOptions(
	options Options,
	managed ManagedOptions,
) (Options, ManagedOptions, error) {
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return Options{}, ManagedOptions{}, err
	}
	target := strings.TrimSpace(managed.Target)
	if target == "" || target != managed.Target ||
		len(managed.Target) > maximumTargetLength ||
		!utf8.ValidString(managed.Target) ||
		strings.IndexFunc(managed.Target, unicode.IsControl) >= 0 {
		return Options{}, ManagedOptions{}, fmt.Errorf(
			"%w: managed target is invalid",
			ErrInvalidOption,
		)
	}
	managed.Target = target
	managed.DialOptions = append([]grpc.DialOption(nil), managed.DialOptions...)
	managed.UpdateSources = append(
		[]secret.UpdateSource(nil),
		managed.UpdateSources...,
	)
	if managed.TLSReloader != nil {
		managed.UpdateSources = append(
			managed.UpdateSources,
			managed.TLSReloader,
		)
	}
	if len(managed.UpdateSources) > 8 {
		return Options{}, ManagedOptions{}, fmt.Errorf(
			"%w: at most 8 credential update sources are allowed",
			ErrInvalidOption,
		)
	}
	for _, source := range managed.UpdateSources {
		if isNilUpdateSource(source) {
			return Options{}, ManagedOptions{}, fmt.Errorf(
				"%w: credential update source is nil",
				ErrInvalidOption,
			)
		}
	}
	if managed.RotationReadyTimeout == 0 {
		managed.RotationReadyTimeout = defaultRotationReadyTimeout
	}
	if managed.RotationReadyTimeout < minimumRotationReadyTimeout ||
		managed.RotationReadyTimeout > maximumRotationReadyTimeout {
		return Options{}, ManagedOptions{}, fmt.Errorf(
			"%w: rotation ready timeout is outside %s..%s",
			ErrInvalidOption,
			minimumRotationReadyTimeout,
			maximumRotationReadyTimeout,
		)
	}
	return normalized, managed, nil
}

func waitForReady(ctx context.Context, connection *grpc.ClientConn) error {
	for {
		state := connection.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return ErrConnectionUnavailable
		}
		connection.Connect()
		if !connection.WaitForStateChange(ctx, state) {
			cause := context.Cause(ctx)
			if cause == nil {
				cause = ErrConnectionUnavailable
			}
			return fmt.Errorf("%w: %w", ErrConnectionUnavailable, cause)
		}
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func mergeUpdates(
	ctx context.Context,
	subscriptions []secret.UpdateSubscription,
) <-chan struct{} {
	merged := make(chan struct{}, 1)
	var forwarders sync.WaitGroup
	forwarders.Add(len(subscriptions))
	for _, subscription := range subscriptions {
		go func(updates <-chan uint64) {
			defer forwarders.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case _, open := <-updates:
					if !open {
						return
					}
					select {
					case merged <- struct{}{}:
					default:
					}
				}
			}
		}(subscription.Updates())
	}
	go func() {
		forwarders.Wait()
		close(merged)
	}()
	return merged
}

func isNilUpdateSource(source secret.UpdateSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ registry.Discovery = (*Runtime)(nil)
