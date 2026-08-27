package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/keelab/keelith/config"
)

const (
	defaultReconnectFloor      = 100 * time.Millisecond
	defaultReconnectMax        = 5 * time.Second
	defaultWatchConnectTimeout = 5 * time.Second
)

// SourceOptions configure bounded upstream Watch recovery.
type SourceOptions struct {
	ReconnectMin        time.Duration
	ReconnectMax        time.Duration
	WatchConnectTimeout time.Duration
}

// SourceDescription is a bounded, value-free LKG runtime snapshot.
type SourceDescription struct {
	Closed     bool
	Fallback   bool
	Watchers   int
	Reconnects uint64
	LastError  string
}

// Source wraps an authoritative Source with encrypted last-good fallback and
// bounded Watch reconnection.
type Source struct {
	upstream config.Source
	store    *Store
	options  SourceOptions

	mu         sync.Mutex
	closed     bool
	fallback   bool
	reconnects uint64
	lastErr    error
	watchers   map[*resilientWatcher]struct{}

	initialLoad     chan struct{}
	initialLoadOnce sync.Once
	closeOnce       sync.Once
	closeErr        error
}

// Wrap creates a last-good caching Source with bounded production defaults.
func Wrap(upstream config.Source, store *Store) (*Source, error) {
	return WrapWithOptions(upstream, store, SourceOptions{})
}

// WrapWithOptions creates a last-good caching Source with explicit reconnect
// bounds. It preserves the existing Wrap contract while allowing deterministic
// integration tests and profile-owned settings.
func WrapWithOptions(
	upstream config.Source,
	store *Store,
	options SourceOptions,
) (*Source, error) {
	if isNil(upstream) || store == nil {
		return nil, fmt.Errorf(
			"%w: upstream and store are required",
			ErrInvalidOption,
		)
	}
	normalized, err := normalizeSourceOptions(options)
	if err != nil {
		return nil, err
	}
	return &Source{
		upstream:    upstream,
		store:       store,
		options:     normalized,
		watchers:    make(map[*resilientWatcher]struct{}),
		initialLoad: make(chan struct{}),
	}, nil
}

// Load prefers the authoritative Source and falls back only when it fails.
func (source *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if err := source.require(ctx); err != nil {
		return config.Snapshot{}, err
	}
	defer source.initialLoadOnce.Do(func() { close(source.initialLoad) })

	snapshot, upstreamErr := source.upstream.Load(ctx)
	if upstreamErr == nil {
		if err := source.store.Save(ctx, snapshot); err != nil {
			source.record(false, err)
		} else {
			source.record(false, nil)
		}
		return snapshot, nil
	}
	cached, cacheErr := source.store.Load(ctx)
	if cacheErr != nil {
		combined := errors.Join(
			fmt.Errorf("config cache: upstream: %w", upstreamErr),
			fmt.Errorf("config cache: fallback: %w", cacheErr),
		)
		source.record(false, combined)
		return config.Snapshot{}, combined
	}
	source.record(true, upstreamErr)
	return cached, nil
}

// Watch returns a lifecycle-owned watcher immediately. Upstream construction
// and recovery happen in the background so Manager can load an authenticated
// LKG before deciding readiness.
func (source *Source) Watch(ctx context.Context) (config.Watcher, error) {
	if err := source.require(ctx); err != nil {
		return nil, err
	}
	watchContext, cancel := context.WithCancel(ctx)
	watcher := &resilientWatcher{
		source:   source,
		context:  watchContext,
		cancel:   cancel,
		results:  make(chan watcherResult, 1),
		done:     make(chan struct{}),
		terminal: config.ErrWatcherClosed,
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	source.watchers[watcher] = struct{}{}
	source.mu.Unlock()
	go watcher.run()
	return watcher, nil
}

// LastError returns the latest upstream or persistence diagnostic.
func (source *Source) LastError() error {
	if source == nil {
		return fmt.Errorf("%w: source is nil", ErrInvalidOption)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.lastErr
}

// Describe returns bounded state without paths, revisions, values, references,
// keys, or provider error text.
func (source *Source) Describe() SourceDescription {
	if source == nil {
		return SourceDescription{Closed: true, LastError: "closed"}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return SourceDescription{
		Closed: source.closed, Fallback: source.fallback,
		Watchers: len(source.watchers), Reconnects: source.reconnects,
		LastError: cacheErrorClass(source.lastErr),
	}
}

// Shutdown stops resilient watchers before delegating ownership to an
// upstream Runtime that exposes Shutdown(context.Context).
func (source *Source) Shutdown(ctx context.Context) error {
	if source == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	source.closeOnce.Do(func() {
		source.mu.Lock()
		source.closed = true
		watchers := make([]*resilientWatcher, 0, len(source.watchers))
		for watcher := range source.watchers {
			watchers = append(watchers, watcher)
		}
		source.mu.Unlock()
		for _, watcher := range watchers {
			source.closeErr = errors.Join(source.closeErr, watcher.Close())
		}
		if upstream, ok := source.upstream.(interface {
			Shutdown(context.Context) error
		}); ok && !isNil(upstream) {
			source.closeErr = errors.Join(source.closeErr, upstream.Shutdown(ctx))
		}
	})
	return source.closeErr
}

func (source *Source) require(ctx context.Context) error {
	if source == nil || ctx == nil {
		return fmt.Errorf("%w: source or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return ErrClosed
	}
	return nil
}

func (source *Source) waitInitialLoad(ctx context.Context) error {
	select {
	case <-source.initialLoad:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (source *Source) validateFallback(ctx context.Context, cause error) error {
	if _, err := source.store.Load(ctx); err != nil {
		combined := errors.Join(cause, err)
		source.record(false, combined)
		return combined
	}
	source.record(true, cause)
	return nil
}

func (source *Source) publish(
	ctx context.Context,
	snapshot config.Snapshot,
	results chan<- watcherResult,
) error {
	persistenceErr := source.store.Save(ctx, snapshot)
	source.record(false, persistenceErr)
	select {
	case results <- watcherResult{snapshot: snapshot}:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (source *Source) record(fallback bool, err error) {
	source.mu.Lock()
	source.fallback = fallback
	source.lastErr = err
	source.mu.Unlock()
}

func (source *Source) reconnect() {
	source.mu.Lock()
	source.reconnects++
	source.mu.Unlock()
}

func (source *Source) removeWatcher(watcher *resilientWatcher) {
	source.mu.Lock()
	delete(source.watchers, watcher)
	source.mu.Unlock()
}

func normalizeSourceOptions(options SourceOptions) (SourceOptions, error) {
	if options.ReconnectMin == 0 {
		options.ReconnectMin = defaultReconnectFloor
	}
	if options.ReconnectMax == 0 {
		options.ReconnectMax = defaultReconnectMax
	}
	if options.WatchConnectTimeout == 0 {
		options.WatchConnectTimeout = defaultWatchConnectTimeout
	}
	if options.ReconnectMin <= 0 || options.ReconnectMax < options.ReconnectMin ||
		options.WatchConnectTimeout <= 0 {
		return SourceOptions{}, fmt.Errorf(
			"%w: reconnect and watch timeouts are invalid",
			ErrInvalidOption,
		)
	}
	return options, nil
}

func cacheErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNotFound):
		return "not-found"
	case errors.Is(err, ErrExpired):
		return "expired"
	case errors.Is(err, ErrCorrupt):
		return "corrupt"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "context"
	default:
		return "upstream"
	}
}

type watcherResult struct {
	snapshot config.Snapshot
}

type watchConnection struct {
	watcher config.Watcher
	cancel  context.CancelFunc
}

type watchAttempt struct {
	watcher config.Watcher
	err     error
}

type resilientWatcher struct {
	source  *Source
	context context.Context
	cancel  context.CancelFunc
	results chan watcherResult
	done    chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	terminal  error
}

func (watcher *resilientWatcher) run() {
	defer close(watcher.done)
	defer close(watcher.results)
	defer watcher.source.removeWatcher(watcher)
	backoff := watcher.source.options.ReconnectMin
	for {
		connection, err := watcher.connect()
		if err == nil {
			backoff = watcher.source.options.ReconnectMin
			err = watcher.consume(connection)
		}
		if cause := context.Cause(watcher.context); cause != nil {
			watcher.setTerminal(cause)
			return
		}
		watcher.source.reconnect()
		if waitErr := watcher.source.waitInitialLoad(watcher.context); waitErr != nil {
			watcher.setTerminal(waitErr)
			return
		}
		if fallbackErr := watcher.source.validateFallback(watcher.context, err); fallbackErr != nil {
			watcher.setTerminal(fallbackErr)
			return
		}
		if err := waitBackoff(watcher.context, backoff); err != nil {
			watcher.setTerminal(err)
			return
		}
		if backoff < watcher.source.options.ReconnectMax {
			backoff *= 2
			if backoff > watcher.source.options.ReconnectMax {
				backoff = watcher.source.options.ReconnectMax
			}
		}
	}
}

func (watcher *resilientWatcher) connect() (watchConnection, error) {
	attemptContext, cancel := context.WithCancel(watcher.context)
	result := make(chan watchAttempt, 1)
	go func() {
		upstream, err := watcher.source.upstream.Watch(attemptContext)
		attempt := watchAttempt{watcher: upstream, err: err}
		select {
		case result <- attempt:
		case <-attemptContext.Done():
			if !isNil(upstream) {
				_ = upstream.Close()
			}
		}
	}()

	timer := time.NewTimer(watcher.source.options.WatchConnectTimeout)
	defer timer.Stop()
	select {
	case attempt := <-result:
		if attempt.err != nil {
			cancel()
			return watchConnection{}, attempt.err
		}
		if isNil(attempt.watcher) {
			cancel()
			return watchConnection{}, fmt.Errorf(
				"%w: upstream returned nil watcher",
				ErrInvalidOption,
			)
		}
		return watchConnection{watcher: attempt.watcher, cancel: cancel}, nil
	case <-timer.C:
		cancel()
		return watchConnection{}, fmt.Errorf(
			"config cache: upstream Watch: %w",
			context.DeadlineExceeded,
		)
	case <-watcher.context.Done():
		cancel()
		return watchConnection{}, context.Cause(watcher.context)
	}
}

func (watcher *resilientWatcher) consume(connection watchConnection) error {
	defer connection.cancel()
	defer func() { _ = connection.watcher.Close() }()

	initial, err := watcher.source.upstream.Load(watcher.context)
	if err != nil {
		return err
	}
	if err := watcher.source.publish(watcher.context, initial, watcher.results); err != nil {
		return err
	}
	for {
		snapshot, err := connection.watcher.Next(watcher.context)
		if err != nil {
			return err
		}
		if err := watcher.source.publish(watcher.context, snapshot, watcher.results); err != nil {
			return err
		}
	}
}

func (watcher *resilientWatcher) Next(
	ctx context.Context,
) (config.Snapshot, error) {
	if watcher == nil || ctx == nil {
		return config.Snapshot{}, fmt.Errorf("%w: watcher or context is nil", ErrInvalidOption)
	}
	select {
	case <-ctx.Done():
		return config.Snapshot{}, context.Cause(ctx)
	case result, open := <-watcher.results:
		if open {
			return result.snapshot, nil
		}
		return config.Snapshot{}, watcher.terminalError()
	}
}

func (watcher *resilientWatcher) Close() error {
	if watcher == nil {
		return nil
	}
	watcher.closeOnce.Do(watcher.cancel)
	<-watcher.done
	err := watcher.terminalError()
	if errors.Is(err, context.Canceled) || errors.Is(err, config.ErrWatcherClosed) {
		return nil
	}
	return err
}

func (watcher *resilientWatcher) setTerminal(err error) {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if err == nil {
		err = config.ErrWatcherClosed
	}
	watcher.terminal = err
}

func (watcher *resilientWatcher) terminalError() error {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if watcher.terminal == nil {
		return config.ErrWatcherClosed
	}
	return watcher.terminal
}

func waitBackoff(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

var _ config.Source = (*Source)(nil)
var _ config.Watcher = (*resilientWatcher)(nil)
