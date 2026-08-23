// Package client connects service discovery snapshots to feedback-aware
// selectors without owning transport connections.
package client

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

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/selector"
)

const (
	defaultMinReconnectDelay = 100 * time.Millisecond
	defaultMaxReconnectDelay = 5 * time.Second
	defaultMaxStale          = 30 * time.Second
)

var (
	// ErrInvalidOption reports an incomplete or unsafe Router configuration.
	ErrInvalidOption = errors.New("client: invalid router option")
	// ErrAlreadyStarted reports a second Router Start call.
	ErrAlreadyStarted = errors.New("client: router already started")
	// ErrNotRunning reports a Pick outside the running lifecycle.
	ErrNotRunning = errors.New("client: router not running")
	// ErrStale reports a disconnected last-good snapshot beyond MaxStale.
	ErrStale = errors.New("client: discovery snapshot is stale")
)

// Picker selects one node and returns its idempotent completion callback.
type Picker interface {
	Pick(context.Context, operation.Operation) (selector.Node, selector.Done, error)
}

// State is the observable Router lifecycle.
type State string

const (
	// StateNew means Start has not been called.
	StateNew State = "new"
	// StateStarting means the initial complete snapshot is loading.
	StateStarting State = "starting"
	// StateRunning means Pick can select from the current snapshot.
	StateRunning State = "running"
	// StateStopping means new Pick calls are rejected while Watch exits.
	StateStopping State = "stopping"
	// StateStopped means all Router resources have been released.
	StateStopped State = "stopped"
)

// RouterConfig defines one service-scoped discovery and selection pipeline.
type RouterConfig struct {
	Name         string
	Service      string
	Discovery    registry.Discovery
	Selector     selector.Selector
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	MaxStale     time.Duration
}

// Description is an immutable operational Router snapshot.
type Description struct {
	Name           string
	Service        string
	State          State
	Connected      bool
	Stale          bool
	Revision       string
	Instances      int
	Subscribers    int
	Reconnects     uint64
	UpdatedAt      time.Time
	DisconnectedAt time.Time
	LastError      string
}

// Router owns one service Watcher and updates an injected Selector.
//
// Router implements app.Component structurally.
type Router struct {
	name         string
	service      string
	discovery    registry.Discovery
	selector     selector.Selector
	reconnectMin time.Duration
	reconnectMax time.Duration
	maxStale     time.Duration

	mu             sync.Mutex
	state          State
	connected      bool
	revision       string
	reconnects     uint64
	updatedAt      time.Time
	disconnectedAt time.Time
	lastError      string
	watcher        registry.Watcher
	snapshot       registry.Snapshot
	changeWatchers map[*routerNodeChangeWatcher]struct{}
	cancel         context.CancelFunc

	startDone     chan struct{}
	done          chan struct{}
	startDoneOnce sync.Once
	doneOnce      sync.Once
}

// NewRouter validates configuration without starting discovery goroutines.
func NewRouter(config RouterConfig) (*Router, error) {
	name := strings.TrimSpace(config.Name)
	if !validName(name) {
		return nil, fmt.Errorf("%w: name is malformed", ErrInvalidOption)
	}
	service := strings.TrimSpace(config.Service)
	if !validName(service) {
		return nil, fmt.Errorf("%w: service is malformed", ErrInvalidOption)
	}
	if isNil(config.Discovery) {
		return nil, fmt.Errorf("%w: discovery is nil", ErrInvalidOption)
	}
	if isNil(config.Selector) {
		return nil, fmt.Errorf("%w: selector is nil", ErrInvalidOption)
	}
	reconnectMin := config.ReconnectMin
	if reconnectMin == 0 {
		reconnectMin = defaultMinReconnectDelay
	}
	reconnectMax := config.ReconnectMax
	if reconnectMax == 0 {
		reconnectMax = defaultMaxReconnectDelay
	}
	maxStale := config.MaxStale
	if maxStale == 0 {
		maxStale = defaultMaxStale
	}
	if reconnectMin <= 0 ||
		reconnectMax < reconnectMin ||
		maxStale <= 0 {
		return nil, fmt.Errorf("%w: reconnect and stale durations are invalid", ErrInvalidOption)
	}
	return &Router{
		name:           name,
		service:        service,
		discovery:      config.Discovery,
		selector:       config.Selector,
		reconnectMin:   reconnectMin,
		reconnectMax:   reconnectMax,
		maxStale:       maxStale,
		state:          StateNew,
		changeWatchers: make(map[*routerNodeChangeWatcher]struct{}),
		startDone:      make(chan struct{}),
		done:           make(chan struct{}),
	}, nil
}

// Name returns the stable App component name.
func (r *Router) Name() string {
	if r == nil {
		return ""
	}
	return r.name
}

// Start obtains and applies the initial complete snapshot before returning.
func (r *Router) Start(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("%w: router is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	runtimeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.mu.Lock()
	if r.state != StateNew {
		r.mu.Unlock()
		cancel()
		return ErrAlreadyStarted
	}
	r.state = StateStarting
	r.cancel = cancel
	r.mu.Unlock()
	defer r.startDoneOnce.Do(func() { close(r.startDone) })

	watcher, err := r.discovery.Watch(runtimeCtx, r.service)
	if err != nil {
		r.failStart(cancel, nil, err)
		return fmt.Errorf("client: watch %q: %w", r.service, err)
	}
	if isNil(watcher) {
		err = fmt.Errorf("client: discovery returned a nil watcher")
		r.failStart(cancel, nil, err)
		return err
	}
	initialContext, cancelInitial := mergeContexts(ctx, runtimeCtx)
	snapshot, err := watcher.Next(initialContext)
	cancelInitial()
	if err != nil {
		r.failStart(cancel, watcher, err)
		return fmt.Errorf("client: initial snapshot %q: %w", r.service, err)
	}
	if err := r.selector.Update(snapshot); err != nil {
		r.failStart(cancel, watcher, err)
		return fmt.Errorf("client: apply initial snapshot: %w", err)
	}

	now := time.Now()
	r.mu.Lock()
	if r.state != StateStarting || context.Cause(runtimeCtx) != nil {
		r.mu.Unlock()
		r.failStart(cancel, watcher, context.Canceled)
		return context.Canceled
	}
	r.state = StateRunning
	r.connected = true
	r.revision = snapshot.Revision()
	r.updatedAt = now
	r.disconnectedAt = time.Time{}
	r.lastError = ""
	r.watcher = watcher
	r.snapshot = snapshot.Clone()
	r.mu.Unlock()

	go r.watch(runtimeCtx, watcher)
	return nil
}

// Stop cancels Watch/Backoff, closes the current Watcher, and waits for exit.
func (r *Router) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}

	for {
		r.mu.Lock()
		switch r.state {
		case StateNew:
			r.state = StateStopped
			r.connected = false
			r.startDoneOnce.Do(func() { close(r.startDone) })
			r.doneOnce.Do(func() { close(r.done) })
			r.mu.Unlock()
			return nil
		case StateStarting:
			cancel := r.cancel
			startDone := r.startDone
			r.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			select {
			case <-startDone:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case StateRunning:
			r.state = StateStopping
			r.connected = false
			cancel := r.cancel
			watcher := r.watcher
			done := r.done
			r.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			if !isNil(watcher) {
				_ = watcher.Close()
			}
			return wait(ctx, done)
		case StateStopping:
			done := r.done
			r.mu.Unlock()
			return wait(ctx, done)
		case StateStopped:
			r.mu.Unlock()
			return nil
		default:
			r.mu.Unlock()
			return nil
		}
	}
}

// WatchNodeChanges subscribes to accepted full-state topology changes.
//
// A new watcher receives the current snapshot first. Pending updates are
// latest-wins because every NodeChange includes the complete current topology.
func (r *Router) WatchNodeChanges(ctx context.Context) (NodeChangeWatcher, error) {
	if r == nil {
		return nil, ErrNotRunning
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	r.mu.Lock()
	if r.state != StateRunning {
		r.mu.Unlock()
		return nil, ErrNotRunning
	}
	watcher := &routerNodeChangeWatcher{
		router:  r,
		parent:  ctx,
		updates: make(chan NodeChange, 1),
		done:    make(chan struct{}),
	}
	r.changeWatchers[watcher] = struct{}{}
	current := r.snapshot.Clone()
	watcher.updates <- newNodeChange(registry.Snapshot{}, current)
	r.mu.Unlock()
	watcher.setStop(context.AfterFunc(ctx, func() {
		_ = watcher.Close()
	}))
	return watcher, nil
}

// Pick selects from the current or bounded last-good snapshot.
func (r *Router) Pick(ctx context.Context, target operation.Operation) (selector.Node, selector.Done, error) {
	if r == nil {
		return selector.Node{}, nil, ErrNotRunning
	}
	if ctx == nil {
		return selector.Node{}, nil, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return selector.Node{}, nil, cause
	}
	r.mu.Lock()
	state := r.state
	connected := r.connected
	disconnectedAt := r.disconnectedAt
	maxStale := r.maxStale
	r.mu.Unlock()
	if state != StateRunning {
		return selector.Node{}, nil, ErrNotRunning
	}
	if !connected &&
		!disconnectedAt.IsZero() &&
		time.Since(disconnectedAt) >= maxStale {
		return selector.Node{}, nil, ErrStale
	}
	return r.selector.Select(ctx, target)
}

// Describe returns lifecycle, connection, revision, and reconnect diagnostics.
func (r *Router) Describe() Description {
	if r == nil {
		return Description{State: StateStopped, LastError: "router is nil"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stale := r.state == StateRunning &&
		!r.connected &&
		!r.disconnectedAt.IsZero() &&
		time.Since(r.disconnectedAt) >= r.maxStale
	return Description{
		Name:           r.name,
		Service:        r.service,
		State:          r.state,
		Connected:      r.connected,
		Stale:          stale,
		Revision:       r.revision,
		Instances:      len(r.snapshot.Instances()),
		Subscribers:    len(r.changeWatchers),
		Reconnects:     r.reconnects,
		UpdatedAt:      r.updatedAt,
		DisconnectedAt: r.disconnectedAt,
		LastError:      r.lastError,
	}
}

func (r *Router) watch(ctx context.Context, initial registry.Watcher) {
	defer r.finish()
	watcher := initial
	backoff := r.reconnectMin
	for {
		snapshot, err := watcher.Next(ctx)
		if err == nil {
			r.applySnapshot(snapshot)
			continue
		}
		_ = watcher.Close()
		if context.Cause(ctx) != nil {
			return
		}
		r.disconnected(err)

		for {
			if !waitBackoff(ctx, backoff) {
				return
			}
			next, watchErr := r.discovery.Watch(ctx, r.service)
			if watchErr != nil || isNil(next) {
				if watchErr == nil {
					watchErr = errors.New("discovery returned a nil watcher")
				}
				r.recordError(watchErr)
				backoff = nextBackoff(backoff, r.reconnectMax)
				continue
			}
			snapshot, nextErr := next.Next(ctx)
			if nextErr != nil {
				_ = next.Close()
				if context.Cause(ctx) != nil {
					return
				}
				r.recordError(nextErr)
				backoff = nextBackoff(backoff, r.reconnectMax)
				continue
			}
			r.reconnected(next)
			r.applySnapshot(snapshot)
			watcher = next
			backoff = r.reconnectMin
			break
		}
	}
}

func (r *Router) applySnapshot(snapshot registry.Snapshot) {
	if err := r.selector.Update(snapshot); err != nil {
		r.recordError(err)
		return
	}
	r.mu.Lock()
	change := newNodeChange(r.snapshot, snapshot)
	watchers := make([]*routerNodeChangeWatcher, 0, len(r.changeWatchers))
	for watcher := range r.changeWatchers {
		watchers = append(watchers, watcher)
	}
	r.snapshot = snapshot.Clone()
	r.revision = snapshot.Revision()
	r.updatedAt = time.Now()
	r.lastError = ""
	r.mu.Unlock()
	for _, watcher := range watchers {
		watcher.publish(change)
	}
}

func (r *Router) removeNodeChangeWatcher(watcher *routerNodeChangeWatcher) {
	r.mu.Lock()
	delete(r.changeWatchers, watcher)
	r.mu.Unlock()
}

func (r *Router) disconnected(err error) {
	r.mu.Lock()
	r.connected = false
	r.watcher = nil
	r.disconnectedAt = time.Now()
	if err != nil {
		r.lastError = err.Error()
	}
	r.mu.Unlock()
}

func (r *Router) reconnected(watcher registry.Watcher) {
	r.mu.Lock()
	r.connected = true
	r.disconnectedAt = time.Time{}
	r.watcher = watcher
	r.reconnects++
	r.mu.Unlock()
}

func (r *Router) recordError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.lastError = err.Error()
	r.mu.Unlock()
}

func (r *Router) failStart(cancel context.CancelFunc, watcher registry.Watcher, err error) {
	cancel()
	if !isNil(watcher) {
		_ = watcher.Close()
	}
	r.mu.Lock()
	r.state = StateStopped
	r.connected = false
	r.watcher = nil
	if err != nil {
		r.lastError = err.Error()
	}
	r.doneOnce.Do(func() { close(r.done) })
	r.mu.Unlock()
}

func (r *Router) finish() {
	r.mu.Lock()
	r.state = StateStopped
	r.connected = false
	r.watcher = nil
	watchers := make([]*routerNodeChangeWatcher, 0, len(r.changeWatchers))
	for watcher := range r.changeWatchers {
		watchers = append(watchers, watcher)
	}
	r.changeWatchers = make(map[*routerNodeChangeWatcher]struct{})
	r.doneOnce.Do(func() { close(r.done) })
	r.mu.Unlock()
	for _, watcher := range watchers {
		_ = watcher.Close()
	}
}

func wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func waitBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}

func mergeContexts(primary context.Context, secondary context.Context) (context.Context, func()) {
	merged, cancel := context.WithCancelCause(primary)
	stop := context.AfterFunc(secondary, func() {
		cancel(context.Cause(secondary))
	})
	return merged, func() {
		stop()
		cancel(nil)
	}
}

func validName(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
