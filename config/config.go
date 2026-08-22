package config

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Option configures a Manager.
type Option interface {
	apply(*managerOptions) error
}

type optionFunc func(*managerOptions) error

func (function optionFunc) apply(options *managerOptions) error {
	return function(options)
}

type managerOptions struct {
	sources       []Source
	validators    []Validator
	bindings      []Binding
	unknownPolicy UnknownFieldPolicy
	knownFields   []string
}

// WithSources registers Sources from low to high priority.
func WithSources(sources ...Source) Option {
	snapshot := append([]Source(nil), sources...)
	return optionFunc(func(options *managerOptions) error {
		for index, source := range snapshot {
			if isNil(source) {
				return fmt.Errorf("source %d is nil", index)
			}
		}
		options.sources = append(options.sources, snapshot...)
		return nil
	})
}

// WithValidator registers a merged-snapshot Validator.
func WithValidator(validator Validator) Option {
	return optionFunc(func(options *managerOptions) error {
		if validator == nil {
			return fmt.Errorf("validator is nil")
		}
		options.validators = append(options.validators, validator)
		return nil
	})
}

// WithBindings registers typed component configuration as both pre-publish
// validators and post-publish named subscribers.
func WithBindings(bindings ...Binding) Option {
	snapshot := append([]Binding(nil), bindings...)
	return optionFunc(func(options *managerOptions) error {
		for index, binding := range snapshot {
			if isNil(binding) {
				return fmt.Errorf("binding %d is nil", index)
			}
		}
		options.bindings = append(options.bindings, snapshot...)
		return nil
	})
}

// WithUnknownFieldPolicy selects allow or reject behavior.
func WithUnknownFieldPolicy(policy UnknownFieldPolicy) Option {
	return optionFunc(func(options *managerOptions) error {
		if policy != UnknownAllow && policy != UnknownReject {
			return fmt.Errorf("unknown field policy %d is invalid", policy)
		}
		options.unknownPolicy = policy
		return nil
	})
}

// WithKnownFields adds dot-separated known paths. A final * allows one
// arbitrary child subtree.
func WithKnownFields(paths ...string) Option {
	snapshot := append([]string(nil), paths...)
	return optionFunc(func(options *managerOptions) error {
		for _, path := range snapshot {
			if strings.TrimSpace(path) == "" {
				return fmt.Errorf("known field path is empty")
			}
		}
		options.knownFields = append(options.knownFields, snapshot...)
		return nil
	})
}

type publishedSnapshot struct {
	snapshot Snapshot
}

// Manager loads, validates, watches, and atomically publishes configuration.
type Manager struct {
	sources       []Source
	validators    []Validator
	unknownPolicy UnknownFieldPolicy
	schema        *schemaNode

	current atomic.Pointer[publishedSnapshot]

	updateMu sync.Mutex

	subscriberMu sync.Mutex
	subscribers  map[string]Subscriber
	statuses     map[string]SubscriberStatus

	rejectedMu   sync.Mutex
	lastRejected string

	watchMu  sync.Mutex
	watching bool
}

// New validates options and constructs an isolated Manager.
func New(optionList ...Option) (*Manager, error) {
	settings := managerOptions{unknownPolicy: UnknownAllow}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	if len(settings.sources) == 0 {
		return nil, fmt.Errorf("%w: at least one Source is required", ErrInvalidOption)
	}
	if settings.unknownPolicy == UnknownReject && len(settings.knownFields) == 0 {
		return nil, fmt.Errorf("%w: reject policy requires known fields", ErrInvalidOption)
	}
	schema, err := buildSchema(settings.knownFields)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidOption, err)
	}

	validators := append([]Validator(nil), settings.validators...)
	subscribers := make(map[string]Subscriber, len(settings.bindings))
	for index, binding := range settings.bindings {
		name := strings.TrimSpace(binding.Name())
		if name == "" {
			return nil, fmt.Errorf("%w: binding %d has an empty name", ErrInvalidOption, index)
		}
		if _, duplicate := subscribers[name]; duplicate {
			return nil, fmt.Errorf("%w: binding %q", ErrDuplicateSubscriber, name)
		}
		subject := binding
		validators = append(validators, subject.Validate)
		subscribers[name] = subject.Apply
	}

	return &Manager{
		sources:       append([]Source(nil), settings.sources...),
		validators:    validators,
		unknownPolicy: settings.unknownPolicy,
		schema:        schema,
		subscribers:   subscribers,
		statuses:      make(map[string]SubscriberStatus),
	}, nil
}

// Load reads every Source, merges them, and atomically publishes a valid
// Snapshot.
func (manager *Manager) Load(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	snapshots, err := manager.loadSources(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	merged, err := Merge(snapshots...)
	if err != nil {
		manager.recordRejected(err)
		return Snapshot{}, err
	}
	published, _, err := manager.publish(ctx, merged)
	return published, err
}

// Watch establishes all Source watchers before loading and then processes
// complete snapshot updates until ctx ends or a watcher fails.
func (manager *Manager) Watch(ctx context.Context) error {
	return manager.watch(ctx, nil)
}

func (manager *Manager) watch(ctx context.Context, ready chan<- error) (result error) {
	readySent := false
	notifyReady := func(err error) {
		if readySent {
			return
		}
		readySent = true
		if ready != nil {
			ready <- err
		}
	}
	defer func() {
		if !readySent {
			notifyReady(result)
		}
	}()
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	if !manager.beginWatch() {
		return ErrAlreadyWatching
	}
	defer manager.endWatch()

	watchContext, cancel := context.WithCancel(ctx)
	watchers := make([]Watcher, 0, len(manager.sources))
	for index, source := range manager.sources {
		watcher, err := source.Watch(watchContext)
		if err != nil {
			cancel()
			closeWatchers(watchers)
			return fmt.Errorf("config: watch source %d: %w", index, err)
		}
		if isNil(watcher) {
			cancel()
			closeWatchers(watchers)
			return fmt.Errorf("config: watch source %d returned nil", index)
		}
		watchers = append(watchers, watcher)
	}

	events := make(chan watchEvent, len(watchers)*2)
	var readers sync.WaitGroup
	readers.Add(len(watchers))
	for index, watcher := range watchers {
		go readWatcher(watchContext, &readers, events, index, watcher)
	}
	defer func() {
		cancel()
		closeWatchers(watchers)
		readers.Wait()
	}()

	sourceSnapshots, err := manager.loadSources(watchContext)
	if err != nil {
		return err
	}
	merged, err := Merge(sourceSnapshots...)
	if err != nil {
		manager.recordRejected(err)
		return err
	}
	if _, _, err := manager.publish(watchContext, merged); err != nil {
		return err
	}
	notifyReady(nil)

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case event := <-events:
			if event.err != nil {
				if cause := context.Cause(watchContext); cause != nil {
					return cause
				}
				return fmt.Errorf("config: watch source %d: %w", event.index, event.err)
			}

			candidateSources := append([]Snapshot(nil), sourceSnapshots...)
			candidateSources[event.index] = event.snapshot
			candidate, mergeErr := Merge(candidateSources...)
			if mergeErr != nil {
				manager.recordRejected(mergeErr)
				continue
			}
			_, changed, publishErr := manager.publish(watchContext, candidate)
			if publishErr != nil {
				continue
			}
			if changed {
				sourceSnapshots = candidateSources
			}
		}
	}
}

// Current returns a deep copy of the current complete Snapshot.
func (manager *Manager) Current() (Snapshot, bool) {
	published := manager.current.Load()
	if published == nil {
		return Snapshot{}, false
	}
	return published.snapshot.Clone(), true
}

// Subscribe registers a uniquely named Subscriber.
func (manager *Manager) Subscribe(name string, subscriber Subscriber) error {
	normalizedName := strings.TrimSpace(name)
	if normalizedName == "" || subscriber == nil {
		return fmt.Errorf("%w: subscriber name or function is empty", ErrInvalidOption)
	}
	manager.subscriberMu.Lock()
	defer manager.subscriberMu.Unlock()
	if _, duplicate := manager.subscribers[normalizedName]; duplicate {
		return fmt.Errorf("%w: %s", ErrDuplicateSubscriber, normalizedName)
	}
	manager.subscribers[normalizedName] = subscriber
	return nil
}

// Unsubscribe removes a Subscriber and its status.
func (manager *Manager) Unsubscribe(name string) {
	manager.subscriberMu.Lock()
	defer manager.subscriberMu.Unlock()
	delete(manager.subscribers, strings.TrimSpace(name))
	delete(manager.statuses, strings.TrimSpace(name))
}

// SubscriberStatuses returns statuses in lexical name order.
func (manager *Manager) SubscriberStatuses() []SubscriberStatus {
	manager.subscriberMu.Lock()
	defer manager.subscriberMu.Unlock()
	statuses := make([]SubscriberStatus, 0, len(manager.statuses))
	for _, status := range manager.statuses {
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(first, second int) bool {
		return statuses[first].Name < statuses[second].Name
	})
	return statuses
}

// LastRejected returns the latest rejected snapshot error for diagnostics.
func (manager *Manager) LastRejected() string {
	manager.rejectedMu.Lock()
	defer manager.rejectedMu.Unlock()
	return manager.lastRejected
}

func (manager *Manager) loadSources(ctx context.Context) ([]Snapshot, error) {
	snapshots := make([]Snapshot, len(manager.sources))
	for index, source := range manager.sources {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		snapshot, err := source.Load(ctx)
		if err != nil {
			return nil, fmt.Errorf("config: load source %d: %w", index, err)
		}
		if err := snapshot.validate(); err != nil {
			return nil, fmt.Errorf("config: load source %d: %w", index, err)
		}
		snapshots[index] = snapshot
	}
	return snapshots, nil
}

func (manager *Manager) publish(
	ctx context.Context,
	candidate Snapshot,
) (Snapshot, bool, error) {
	manager.updateMu.Lock()
	defer manager.updateMu.Unlock()

	if current := manager.current.Load(); current != nil &&
		current.snapshot.revision == candidate.revision {
		return current.snapshot.Clone(), false, nil
	}
	if manager.unknownPolicy == UnknownReject {
		if err := manager.schema.validate(candidate.values); err != nil {
			manager.recordRejected(err)
			return Snapshot{}, false, err
		}
	}
	for index, validator := range manager.validators {
		if err := validator(candidate.Clone()); err != nil {
			wrapped := fmt.Errorf("%w: validator %d: %w", ErrValidation, index, err)
			manager.recordRejected(wrapped)
			return Snapshot{}, false, wrapped
		}
	}

	stored := candidate.Clone()
	manager.current.Store(&publishedSnapshot{snapshot: stored})
	manager.notifySubscribers(ctx, stored)
	return stored.Clone(), true, nil
}

func (manager *Manager) notifySubscribers(ctx context.Context, snapshot Snapshot) {
	manager.subscriberMu.Lock()
	names := make([]string, 0, len(manager.subscribers))
	subscribers := make(map[string]Subscriber, len(manager.subscribers))
	for name, subscriber := range manager.subscribers {
		names = append(names, name)
		subscribers[name] = subscriber
	}
	manager.subscriberMu.Unlock()
	sort.Strings(names)

	for _, name := range names {
		err := subscribers[name](ctx, snapshot.Clone())
		status := SubscriberStatus{
			Name:      name,
			Revision:  snapshot.revision,
			AppliedAt: time.Now().UTC(),
		}
		if err != nil {
			status.RestartRequired = errors.Is(err, ErrRestartRequired)
			status.LastError = err.Error()
		}
		manager.subscriberMu.Lock()
		if manager.subscribers[name] != nil {
			manager.statuses[name] = status
		}
		manager.subscriberMu.Unlock()
	}
}

func (manager *Manager) recordRejected(err error) {
	if err == nil {
		return
	}
	manager.rejectedMu.Lock()
	manager.lastRejected = err.Error()
	manager.rejectedMu.Unlock()
}

func (manager *Manager) beginWatch() bool {
	manager.watchMu.Lock()
	defer manager.watchMu.Unlock()
	if manager.watching {
		return false
	}
	manager.watching = true
	return true
}

func (manager *Manager) endWatch() {
	manager.watchMu.Lock()
	manager.watching = false
	manager.watchMu.Unlock()
}

type watchEvent struct {
	index    int
	snapshot Snapshot
	err      error
}

func readWatcher(ctx context.Context, readers *sync.WaitGroup, events chan<- watchEvent, index int, watcher Watcher) {
	defer readers.Done()
	for {
		snapshot, err := watcher.Next(ctx)
		event := watchEvent{index: index, snapshot: snapshot, err: err}
		select {
		case events <- event:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func closeWatchers(watchers []Watcher) {
	for _, watcher := range watchers {
		_ = watcher.Close()
	}
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

type schemaNode struct {
	children map[string]*schemaNode
	terminal bool
}

func buildSchema(paths []string) (*schemaNode, error) {
	root := &schemaNode{children: make(map[string]*schemaNode)}
	for _, path := range paths {
		segments := strings.Split(path, ".")
		current := root
		for index, segment := range segments {
			if segment == "" || segment == "*" && index != len(segments)-1 {
				return nil, fmt.Errorf("config: invalid known field path %q", path)
			}
			next := current.children[segment]
			if next == nil {
				next = &schemaNode{children: make(map[string]*schemaNode)}
				current.children[segment] = next
			}
			current = next
		}
		current.terminal = true
	}
	return root, nil
}

func (schema *schemaNode) validate(values map[string]any) error {
	return schema.validateAt(values, nil)
}

func (schema *schemaNode) validateAt(values map[string]any, path []string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		child := schema.children[key]
		if child == nil {
			child = schema.children["*"]
		}
		currentPath := append(append([]string(nil), path...), key)
		if child == nil {
			return fmt.Errorf("%w: %s", ErrUnknownField, strings.Join(currentPath, "."))
		}
		if child.terminal {
			continue
		}
		if nested, ok := values[key].(map[string]any); ok {
			if err := child.validateAt(nested, currentPath); err != nil {
				return err
			}
		}
	}
	return nil
}
