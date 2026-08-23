// Package memory provides an in-memory Registry and Discovery implementation.
package memory

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/keelab/keelith/registry"
)

// Registry is a concurrent in-memory registration and discovery backend.
type Registry struct {
	mu       sync.Mutex
	services map[string]*serviceState
	watchers map[string]map[*watcher]struct{}
}

type serviceState struct {
	revision  uint64
	instances map[string]registry.Instance
}

// New creates an empty Registry.
func New() *Registry {
	return &Registry{
		services: make(map[string]*serviceState),
		watchers: make(map[string]map[*watcher]struct{}),
	}
}

// Register inserts or replaces an Instance and publishes a full Snapshot.
func (r *Registry) Register(ctx context.Context, instance registry.Instance) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := instance.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.service(instance.Service())
	for id, existing := range state.instances {
		if id == instance.ID() {
			continue
		}
		for _, existingEndpoint := range existing.Endpoints() {
			for _, candidateEndpoint := range instance.Endpoints() {
				if existingEndpoint == candidateEndpoint {
					return fmt.Errorf(
						"%w: %s",
						registry.ErrDuplicateEndpoint,
						candidateEndpoint,
					)
				}
			}
		}
	}
	if existing, ok := state.instances[instance.ID()]; ok && existing.Equal(instance) {
		return nil
	}
	state.instances[instance.ID()] = instance.Clone()
	state.revision++
	r.notify(instance.Service(), r.snapshot(instance.Service(), state))
	return nil
}

// Deregister removes an Instance by service and ID.
func (r *Registry) Deregister(ctx context.Context, instance registry.Instance) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := instance.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.service(instance.Service())
	if _, exists := state.instances[instance.ID()]; !exists {
		return nil
	}
	delete(state.instances, instance.ID())
	state.revision++
	r.notify(instance.Service(), r.snapshot(instance.Service(), state))
	return nil
}

// Watch creates a watcher whose first value is the current full Snapshot.
func (r *Registry) Watch(ctx context.Context, service string) (registry.Watcher, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if service == "" {
		return nil, fmt.Errorf("%w: service is empty", registry.ErrInvalidSnapshot)
	}

	subscription := &watcher{
		backend: r,
		service: service,
		parent:  ctx,
		updates: make(chan registry.Snapshot, 1),
		done:    make(chan struct{}),
	}
	r.mu.Lock()
	state := r.service(service)
	serviceWatchers := r.watchers[service]
	if serviceWatchers == nil {
		serviceWatchers = make(map[*watcher]struct{})
		r.watchers[service] = serviceWatchers
	}
	serviceWatchers[subscription] = struct{}{}
	subscription.updates <- r.snapshot(service, state)
	r.mu.Unlock()
	return subscription, nil
}

// WatcherCount returns the number of open watchers for service.
func (r *Registry) WatcherCount(service string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.watchers[service])
}

func (r *Registry) service(name string) *serviceState {
	state := r.services[name]
	if state == nil {
		state = &serviceState{instances: make(map[string]registry.Instance)}
		r.services[name] = state
	}
	return state
}

func (r *Registry) snapshot(
	service string,
	state *serviceState,
) registry.Snapshot {
	instances := make([]registry.Instance, 0, len(state.instances))
	for _, instance := range state.instances {
		instances = append(instances, instance.Clone())
	}
	snapshot, _ := registry.NewSnapshot(
		service,
		strconv.FormatUint(state.revision, 10),
		instances,
	)
	return snapshot
}

func (r *Registry) notify(service string, snapshot registry.Snapshot) {
	for watcher := range r.watchers[service] {
		select {
		case <-watcher.updates:
		default:
		}
		select {
		case watcher.updates <- snapshot.Clone():
		default:
		}
	}
}

type watcher struct {
	backend *Registry
	service string
	parent  context.Context
	updates chan registry.Snapshot
	done    chan struct{}
	once    sync.Once
}

func (w *watcher) Next(ctx context.Context) (registry.Snapshot, error) {
	select {
	case snapshot := <-w.updates:
		return snapshot.Clone(), nil
	case <-w.done:
		return registry.Snapshot{}, registry.ErrWatcherClosed
	case <-w.parent.Done():
		return registry.Snapshot{}, context.Cause(w.parent)
	case <-ctx.Done():
		return registry.Snapshot{}, context.Cause(ctx)
	}
}

func (w *watcher) Close() error {
	w.once.Do(func() {
		w.backend.mu.Lock()
		delete(w.backend.watchers[w.service], w)
		close(w.done)
		w.backend.mu.Unlock()
	})
	return nil
}
