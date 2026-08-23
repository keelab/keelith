// Package memory provides an instance-scoped in-memory secret provider for
// development, conformance, and bootstrap use.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/keelab/keelith/secret"
)

type event struct {
	value secret.Value
	err   error
}

// Provider stores immutable secret values and broadcasts replacements.
type Provider struct {
	mu       sync.RWMutex
	values   map[string]secret.Value
	watchers map[string]map[*watcher]struct{}
	closed   bool
}

// New validates and snapshots initial values.
func New(initial map[string]secret.Value) (*Provider, error) {
	values := make(map[string]secret.Value, len(initial))
	for key, value := range initial {
		if _, err := secret.NewReference("memory", key); err != nil {
			return nil, err
		}
		if err := value.Validate(); err != nil {
			return nil, fmt.Errorf("secret/memory: key %q: %w", key, err)
		}
		values[key] = value
	}
	return &Provider{
		values:   values,
		watchers: make(map[string]map[*watcher]struct{}),
	}, nil
}

// Resolve returns an immutable value copy.
func (provider *Provider) Resolve(
	ctx context.Context,
	key string,
) (secret.Value, error) {
	if ctx == nil {
		return secret.Value{}, fmt.Errorf("secret/memory: context is nil")
	}
	if err := context.Cause(ctx); err != nil {
		return secret.Value{}, err
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.closed {
		return secret.Value{}, secret.ErrWatcherClosed
	}
	value, exists := provider.values[key]
	if !exists {
		return secret.Value{}, secret.ErrNotFound
	}
	return value, nil
}

// Watch subscribes to future complete replacements.
func (provider *Provider) Watch(
	ctx context.Context,
	key string,
) (secret.Watcher, error) {
	if ctx == nil {
		return nil, fmt.Errorf("secret/memory: context is nil")
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closed {
		return nil, secret.ErrWatcherClosed
	}
	if _, exists := provider.values[key]; !exists {
		return nil, secret.ErrNotFound
	}
	result := &watcher{
		provider: provider,
		key:      key,
		events:   make(chan event, 1),
		closed:   make(chan struct{}),
	}
	if provider.watchers[key] == nil {
		provider.watchers[key] = make(map[*watcher]struct{})
	}
	provider.watchers[key][result] = struct{}{}
	return result, nil
}

// Store atomically replaces one key and notifies watchers.
func (provider *Provider) Store(key string, value secret.Value) error {
	reference, err := secret.NewReference("memory", key)
	if err != nil {
		return err
	}
	_ = reference
	if err := value.Validate(); err != nil {
		return err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closed {
		return secret.ErrWatcherClosed
	}
	provider.values[key] = value
	for watcher := range provider.watchers[key] {
		watcher.publish(event{value: value})
	}
	return nil
}

// Delete removes one key and notifies watchers with ErrNotFound.
func (provider *Provider) Delete(key string) error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closed {
		return secret.ErrWatcherClosed
	}
	if _, exists := provider.values[key]; !exists {
		return secret.ErrNotFound
	}
	delete(provider.values, key)
	for watcher := range provider.watchers[key] {
		watcher.publish(event{err: secret.ErrNotFound})
	}
	return nil
}

// Close closes all watchers and rejects future use.
func (provider *Provider) Close() error {
	provider.mu.Lock()
	if provider.closed {
		provider.mu.Unlock()
		return nil
	}
	provider.closed = true
	watchers := make([]*watcher, 0)
	for _, values := range provider.watchers {
		for watcher := range values {
			watchers = append(watchers, watcher)
		}
	}
	provider.watchers = nil
	provider.mu.Unlock()
	for _, watcher := range watchers {
		watcher.close()
	}
	return nil
}

func (provider *Provider) remove(target *watcher) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if watchers := provider.watchers[target.key]; watchers != nil {
		delete(watchers, target)
		if len(watchers) == 0 {
			delete(provider.watchers, target.key)
		}
	}
}

type watcher struct {
	provider *Provider
	key      string
	events   chan event
	closed   chan struct{}
	once     sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (secret.Value, error) {
	if ctx == nil {
		return secret.Value{}, fmt.Errorf("secret/memory: context is nil")
	}
	select {
	case <-ctx.Done():
		return secret.Value{}, context.Cause(ctx)
	case <-watcher.closed:
		return secret.Value{}, secret.ErrWatcherClosed
	case event := <-watcher.events:
		if event.err != nil {
			return secret.Value{}, event.err
		}
		return event.value, nil
	}
}

func (watcher *watcher) Close() error {
	watcher.once.Do(func() {
		watcher.provider.remove(watcher)
		close(watcher.closed)
	})
	return nil
}

func (watcher *watcher) close() {
	watcher.once.Do(func() { close(watcher.closed) })
}

func (watcher *watcher) publish(update event) {
	select {
	case watcher.events <- update:
	default:
		select {
		case <-watcher.events:
		default:
		}
		select {
		case watcher.events <- update:
		default:
		}
	}
}
