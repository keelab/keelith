// Package grpcauth provides Secret-backed outbound gRPC credentials without
// coupling application config to plaintext authentication material.
package grpcauth

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/keelab/keelith/secret"
)

// Bearer lifecycle states.
const (
	defaultMaxTokenBytes = 8 * 1024
	maximumTokenBytes    = 64 * 1024
	reconnectInitial     = 250 * time.Millisecond
	reconnectMaximum     = 5 * time.Second
)

var (
	// ErrInvalidOption reports an invalid manager, reference, or token budget.
	ErrInvalidOption = errors.New("grpc auth: invalid option")
	// ErrCredentialUnavailable reports a request before valid last-good
	// credentials have been loaded.
	ErrCredentialUnavailable = errors.New("grpc auth: credential unavailable")
	// ErrStopped reports an operation after lifecycle shutdown.
	ErrStopped = errors.New("grpc auth: stopped")
)

// SecretManager resolves and watches complete credential replacements.
type SecretManager interface {
	Resolve(context.Context, secret.Reference) (secret.Value, error)
	Watch(context.Context, secret.Reference) (secret.Watcher, error)
}

// Config defines one strict bearer token credential.
type Config struct {
	Reference     secret.Reference
	MaxTokenBytes int
}

// State is a bounded lifecycle state.
type State string

const (
	// StateNew is the state before the credential starts.
	StateNew State = "new"
	// StateStarting is the state while the initial secret is resolving.
	StateStarting State = "starting"
	// StateRunning is the active credential state.
	StateRunning State = "running"
	// StateStopped is the terminal stopped state.
	StateStopped State = "stopped"
	// StateFailed is the state after a terminal credential failure.
	StateFailed State = "failed"
)

// Description is a token-free operational snapshot.
type Description struct {
	State      State
	Ready      bool
	Degraded   bool
	Reloads    uint64
	Reconnects uint64
	Failures   uint64
}

type credential struct {
	authorization string
}

// Bearer implements gRPC PerRPCCredentials, App Lifecycle, and
// secret.UpdateSource. Every request receives the current last-good token;
// consumers such as long-lived ADS streams can subscribe to successful
// replacements and reconnect proactively.
type Bearer struct {
	manager       SecretManager
	reference     secret.Reference
	maxTokenBytes int
	current       atomic.Pointer[credential]

	mu             sync.Mutex
	state          State
	lastError      error
	version        string
	generation     uint64
	nextSubscriber uint64
	subscribers    map[uint64]chan uint64
	cancel         context.CancelFunc
	watcher        secret.Watcher
	done           chan struct{}

	reloads    atomic.Uint64
	reconnects atomic.Uint64
	failures   atomic.Uint64
}

// Subscription observes successful token generations without token material,
// provider revision, reference, or errors.
type Subscription struct {
	bearer    *Bearer
	id        uint64
	baseline  uint64
	updates   <-chan uint64
	closeOnce sync.Once
}

// New validates and constructs a dormant bearer credential lifecycle.
func New(manager SecretManager, config Config) (*Bearer, error) {
	if isNil(manager) {
		return nil, fmt.Errorf("%w: secret manager is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return &Bearer{
		manager:       manager,
		reference:     normalized.Reference,
		maxTokenBytes: normalized.MaxTokenBytes,
		state:         StateNew,
		subscribers:   make(map[uint64]chan uint64),
	}, nil
}

// NormalizeConfig applies stable budgets and validates the material-free
// credential contract without resolving the referenced token.
func NormalizeConfig(config Config) (Config, error) {
	if _, err := secret.NewReference(
		config.Reference.Provider(),
		config.Reference.Key(),
	); err != nil {
		return Config{}, err
	}
	if config.MaxTokenBytes == 0 {
		config.MaxTokenBytes = defaultMaxTokenBytes
	}
	if config.MaxTokenBytes < 1 || config.MaxTokenBytes > maximumTokenBytes {
		return Config{}, fmt.Errorf(
			"%w: token budget is outside 1..%d bytes",
			ErrInvalidOption,
			maximumTokenBytes,
		)
	}
	return config, nil
}

// ValidateConfig validates bearer settings without resolving credentials.
func ValidateConfig(config Config) error {
	_, err := NormalizeConfig(config)
	return err
}

// Start resolves a valid initial token and then watches complete replacements.
func (bearer *Bearer) Start(ctx context.Context) error {
	if bearer == nil || ctx == nil {
		return fmt.Errorf("%w: bearer or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	bearer.mu.Lock()
	if bearer.state != StateNew {
		bearer.mu.Unlock()
		return fmt.Errorf("%w: bearer already started or stopped", ErrInvalidOption)
	}
	bearer.state = StateStarting
	bearer.mu.Unlock()

	watchContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	source, err := bearer.manager.Watch(watchContext, bearer.reference)
	if err != nil {
		cancel()
		bearer.failStart(err)
		return fmt.Errorf("grpc auth: establish token watch: %w", err)
	}
	value, err := bearer.manager.Resolve(ctx, bearer.reference)
	if err == nil {
		err = bearer.apply(value)
	}
	if err != nil {
		cancel()
		_ = source.Close()
		bearer.failStart(err)
		return fmt.Errorf("grpc auth: load token: %w", err)
	}
	bearer.mu.Lock()
	bearer.state = StateRunning
	bearer.cancel = cancel
	bearer.watcher = source
	bearer.done = make(chan struct{})
	done := bearer.done
	bearer.mu.Unlock()
	go bearer.run(watchContext, source, done)
	return nil
}

// Shutdown stops the credential watch and clears the active pointer. It is
// safe to call repeatedly.
func (bearer *Bearer) Shutdown(ctx context.Context) error {
	if bearer == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	bearer.mu.Lock()
	switch bearer.state {
	case StateNew, StateFailed:
		bearer.state = StateStopped
		bearer.current.Store(nil)
		bearer.closeSubscribersLocked()
		bearer.mu.Unlock()
		return nil
	case StateStopped:
		bearer.mu.Unlock()
		return nil
	}
	cancel := bearer.cancel
	source := bearer.watcher
	done := bearer.done
	bearer.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if source != nil {
		_ = source.Close()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// GetRequestMetadata returns the current Authorization header. Errors never
// contain token material, references, provider versions, or request URIs.
func (bearer *Bearer) GetRequestMetadata(
	ctx context.Context,
	_ ...string,
) (map[string]string, error) {
	if bearer == nil || ctx == nil {
		return nil, fmt.Errorf("%w: bearer or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	current := bearer.current.Load()
	if current == nil {
		bearer.mu.Lock()
		stopped := bearer.state == StateStopped || bearer.state == StateFailed
		bearer.mu.Unlock()
		if stopped {
			return nil, ErrStopped
		}
		return nil, ErrCredentialUnavailable
	}
	return map[string]string{"authorization": current.authorization}, nil
}

// RequireTransportSecurity prevents bearer tokens from crossing plaintext
// gRPC connections.
func (*Bearer) RequireTransportSecurity() bool { return true }

// Ready reports whether valid last-good credentials are available.
func (bearer *Bearer) Ready() bool {
	return bearer != nil && bearer.current.Load() != nil
}

// SubscribeUpdates implements secret.UpdateSource.
func (bearer *Bearer) SubscribeUpdates() (secret.UpdateSubscription, error) {
	if bearer == nil {
		return nil, fmt.Errorf("%w: bearer is nil", ErrInvalidOption)
	}
	bearer.mu.Lock()
	defer bearer.mu.Unlock()
	if bearer.state == StateStopped || bearer.state == StateFailed {
		return nil, ErrStopped
	}
	bearer.nextSubscriber++
	id := bearer.nextSubscriber
	updates := make(chan uint64, 1)
	bearer.subscribers[id] = updates
	return &Subscription{
		bearer:   bearer,
		id:       id,
		baseline: bearer.generation,
		updates:  updates,
	}, nil
}

// Description returns bounded lifecycle and aggregate counters.
func (bearer *Bearer) Description() Description {
	if bearer == nil {
		return Description{State: StateStopped}
	}
	bearer.mu.Lock()
	description := Description{
		State:    bearer.state,
		Ready:    bearer.current.Load() != nil,
		Degraded: bearer.lastError != nil,
	}
	bearer.mu.Unlock()
	description.Reloads = bearer.reloads.Load()
	description.Reconnects = bearer.reconnects.Load()
	description.Failures = bearer.failures.Load()
	return description
}

// Baseline returns the generation active when the subscription was created.
func (subscription *Subscription) Baseline() uint64 {
	if subscription == nil {
		return 0
	}
	return subscription.baseline
}

// Updates returns a single-slot latest-generation stream.
func (subscription *Subscription) Updates() <-chan uint64 {
	if subscription == nil {
		return nil
	}
	return subscription.updates
}

// Close unregisters the subscription. It is safe to call repeatedly.
func (subscription *Subscription) Close() {
	if subscription == nil || subscription.bearer == nil {
		return
	}
	subscription.closeOnce.Do(func() {
		subscription.bearer.removeSubscriber(subscription.id)
	})
}

func (bearer *Bearer) run(
	ctx context.Context,
	source secret.Watcher,
	done chan struct{},
) {
	defer func() {
		bearer.mu.Lock()
		bearer.state = StateStopped
		bearer.cancel = nil
		bearer.watcher = nil
		bearer.current.Store(nil)
		bearer.closeSubscribersLocked()
		close(done)
		bearer.mu.Unlock()
	}()
	for {
		value, err := source.Next(ctx)
		if err != nil {
			if context.Cause(ctx) != nil {
				return
			}
			bearer.record(err)
			_ = source.Close()
			source, err = bearer.reconnect(ctx)
			if err != nil {
				return
			}
			continue
		}
		if err := bearer.apply(value); err != nil {
			bearer.record(err)
			continue
		}
		bearer.record(nil)
	}
}

func (bearer *Bearer) reconnect(ctx context.Context) (secret.Watcher, error) {
	delay := reconnectInitial
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		source, err := bearer.manager.Watch(ctx, bearer.reference)
		if err == nil {
			value, resolveErr := bearer.manager.Resolve(ctx, bearer.reference)
			if resolveErr == nil {
				resolveErr = bearer.apply(value)
			}
			if resolveErr == nil {
				bearer.mu.Lock()
				bearer.watcher = source
				bearer.mu.Unlock()
				bearer.reconnects.Add(1)
				bearer.record(nil)
				return source, nil
			}
			_ = source.Close()
			err = resolveErr
		}
		bearer.record(err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopAndDrain(timer)
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
		delay *= 2
		if delay > reconnectMaximum {
			delay = reconnectMaximum
		}
	}
}

func (bearer *Bearer) apply(value secret.Value) error {
	authorization, err := validateToken(value, bearer.maxTokenBytes)
	if err != nil {
		return err
	}
	bearer.mu.Lock()
	defer bearer.mu.Unlock()
	if bearer.version == value.Version() {
		return nil
	}
	bearer.current.Store(&credential{authorization: authorization})
	bearer.version = value.Version()
	bearer.generation++
	for _, updates := range bearer.subscribers {
		publishGeneration(updates, bearer.generation)
	}
	bearer.reloads.Add(1)
	return nil
}

func validateToken(value secret.Value, maxBytes int) (string, error) {
	if err := value.Validate(); err != nil || value.Expired(time.Now()) {
		return "", fmt.Errorf("%w: resolved token is invalid", ErrInvalidOption)
	}
	content := value.Bytes()
	defer clear(content)
	tokenContent := secret.TrimLineBreaks(content)
	if len(tokenContent) == 0 ||
		len(tokenContent) > maxBytes ||
		!utf8.Valid(tokenContent) {
		return "", fmt.Errorf("%w: token violates size or UTF-8 budget", ErrInvalidOption)
	}
	token := string(tokenContent)
	if strings.TrimSpace(token) != token || !validBearerToken(token) {
		return "", fmt.Errorf("%w: token violates bearer syntax", ErrInvalidOption)
	}
	return "Bearer " + token, nil
}

func validBearerToken(token string) bool {
	padding := false
	for _, character := range token {
		if character == '=' {
			padding = true
			continue
		}
		if padding ||
			(character < 'a' || character > 'z') &&
				(character < 'A' || character > 'Z') &&
				(character < '0' || character > '9') &&
				!strings.ContainsRune("-._~+/", character) {
			return false
		}
	}
	return token != ""
}

func (bearer *Bearer) failStart(err error) {
	bearer.mu.Lock()
	bearer.state = StateFailed
	bearer.lastError = err
	bearer.failures.Add(1)
	bearer.mu.Unlock()
}

func (bearer *Bearer) record(err error) {
	bearer.mu.Lock()
	bearer.lastError = err
	bearer.mu.Unlock()
	if err != nil {
		bearer.failures.Add(1)
	}
}

func (bearer *Bearer) removeSubscriber(id uint64) {
	bearer.mu.Lock()
	updates, exists := bearer.subscribers[id]
	if exists {
		delete(bearer.subscribers, id)
		close(updates)
	}
	bearer.mu.Unlock()
}

func (bearer *Bearer) closeSubscribersLocked() {
	for id, updates := range bearer.subscribers {
		delete(bearer.subscribers, id)
		close(updates)
	}
}

func publishGeneration(updates chan uint64, generation uint64) {
	select {
	case <-updates:
	default:
	}
	select {
	case updates <- generation:
	default:
	}
}

func stopAndDrain(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ secret.UpdateSource = (*Bearer)(nil)
