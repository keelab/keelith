// Package configured provides atomically reloadable, configuration-backed
// service discovery.
package configured

import (
	"context"
	"errors"
	"fmt"
	"sync"

	kconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/registry"
)

const (
	maxServices  = 4096
	maxInstances = 64 * 1024
)

var (
	// ErrInvalidConfig reports malformed configured discovery input.
	ErrInvalidConfig = errors.New("configured discovery: invalid config")
	// ErrInvalidDiscovery reports invalid discovery construction or use.
	ErrInvalidDiscovery = errors.New("configured discovery: invalid discovery")
)

// InstanceConfig is one statically described service instance.
type InstanceConfig struct {
	ID        string            `config:"id"`
	Endpoints []string          `config:"endpoints"`
	Metadata  map[string]string `config:"metadata"`
}

// ServiceConfig is the complete instance set for one logical service.
type ServiceConfig struct {
	Service   string           `config:"service"`
	Instances []InstanceConfig `config:"instances"`
}

// Config is a full configured-discovery topology.
type Config struct {
	Services []ServiceConfig `config:"services"`
}

// Description is a bounded, value-free discovery diagnostic.
type Description struct {
	Revision  string
	Services  int
	Instances int
	Watchers  int
	Updates   uint64
}

// ConfigDescription combines typed-config and discovery diagnostics without
// exposing service identities, endpoints, metadata, or configuration values.
type ConfigDescription struct {
	Name      string
	Path      string
	Loaded    bool
	Failed    bool
	Revision  string
	Services  int
	Instances int
	Watchers  int
	Updates   uint64
}

// Discovery serves full revisioned snapshots from the latest applied Config.
//
// Replace is intentionally private: ConfigBinding is the single validated
// writer, while any number of managed clients may watch concurrently.
type Discovery struct {
	mu        sync.Mutex
	revision  string
	snapshots map[string]registry.Snapshot
	watchers  map[string]map[*watcher]struct{}
	instances int
	updates   uint64
}

// ConfigBinding validates and atomically publishes a configured topology.
//
// It implements config.Binding.
type ConfigBinding struct {
	component *kconfig.Component[Config]
	discovery *Discovery
}

// NewConfigBinding creates an empty discovery and a hot-reloadable typed
// configuration binding.
func NewConfigBinding(name, path string) (*ConfigBinding, error) {
	component, err := kconfig.NewComponent[Config](
		name,
		path,
		kconfig.WithComponentDefault(Config{}),
		kconfig.WithComponentValidator(func(value Config) error {
			_, _, err := compile("candidate", value)
			return err
		}),
		kconfig.WithReloadableFields[Config]("services"),
	)
	if err != nil {
		return nil, err
	}
	return &ConfigBinding{
		component: component,
		discovery: &Discovery{
			revision:  "bootstrap",
			snapshots: make(map[string]registry.Snapshot),
			watchers:  make(map[string]map[*watcher]struct{}),
		},
	}, nil
}

// Name returns the Manager subscriber identity.
func (b *ConfigBinding) Name() string {
	if b == nil || b.component == nil {
		return ""
	}
	return b.component.Name()
}

// Validate strictly decodes and validates the candidate topology before
// Manager publication.
func (b *ConfigBinding) Validate(snapshot kconfig.Snapshot) error {
	if b == nil || b.component == nil || b.discovery == nil {
		return fmt.Errorf("%w: binding is nil", ErrInvalidConfig)
	}
	return b.component.Validate(snapshot)
}

// Apply atomically publishes the complete candidate topology.
func (b *ConfigBinding) Apply(
	ctx context.Context,
	snapshot kconfig.Snapshot,
) error {
	if b == nil || b.component == nil || b.discovery == nil {
		return fmt.Errorf("%w: binding is nil", ErrInvalidConfig)
	}
	if err := b.component.Apply(ctx, snapshot); err != nil {
		return err
	}
	current, ok := b.component.Current()
	if !ok {
		return fmt.Errorf("%w: config was not published", ErrInvalidConfig)
	}
	snapshots, instances, err := compile(snapshot.Revision(), current)
	if err != nil {
		return err
	}
	b.discovery.replace(snapshot.Revision(), snapshots, instances)
	return nil
}

// Discovery returns the stable discovery instance updated by this binding.
func (b *ConfigBinding) Discovery() *Discovery {
	if b == nil {
		return nil
	}
	return b.discovery
}

// Description returns aggregate topology and typed-binding state.
func (b *ConfigBinding) Description() ConfigDescription {
	if b == nil || b.component == nil || b.discovery == nil {
		return ConfigDescription{}
	}
	component := b.component.Description()
	discovery := b.discovery.Describe()
	return ConfigDescription{
		Name:      component.Name,
		Path:      component.Path,
		Loaded:    component.Loaded,
		Failed:    component.Failed,
		Revision:  discovery.Revision,
		Services:  discovery.Services,
		Instances: discovery.Instances,
		Watchers:  discovery.Watchers,
		Updates:   discovery.Updates,
	}
}

// Watch creates a full-snapshot watcher whose first value is the latest
// topology for service, including an empty snapshot for an unknown service.
func (d *Discovery) Watch(
	ctx context.Context,
	service string,
) (registry.Watcher, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: discovery is nil", ErrInvalidDiscovery)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidDiscovery)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}

	d.mu.Lock()
	initial, ok := d.snapshots[service]
	if !ok {
		var err error
		initial, err = registry.NewSnapshot(service, d.revision, nil)
		if err != nil {
			d.mu.Unlock()
			return nil, err
		}
	}
	subscription := &watcher{
		discovery: d,
		service:   service,
		parent:    ctx,
		updates:   make(chan registry.Snapshot, 1),
		done:      make(chan struct{}),
	}
	serviceWatchers := d.watchers[service]
	if serviceWatchers == nil {
		serviceWatchers = make(map[*watcher]struct{})
		d.watchers[service] = serviceWatchers
	}
	serviceWatchers[subscription] = struct{}{}
	subscription.updates <- initial.Clone()
	d.mu.Unlock()
	return subscription, nil
}

// Describe returns aggregate discovery state.
func (d *Discovery) Describe() Description {
	if d == nil {
		return Description{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	watchers := 0
	for _, serviceWatchers := range d.watchers {
		watchers += len(serviceWatchers)
	}
	return Description{
		Revision:  d.revision,
		Services:  len(d.snapshots),
		Instances: d.instances,
		Watchers:  watchers,
		Updates:   d.updates,
	}
}

func (d *Discovery) replace(
	revision string,
	snapshots map[string]registry.Snapshot,
	instances int,
) {
	d.mu.Lock()
	d.revision = revision
	d.snapshots = snapshots
	d.instances = instances
	d.updates++
	for service, subscriptions := range d.watchers {
		snapshot, ok := snapshots[service]
		if !ok {
			snapshot, _ = registry.NewSnapshot(service, revision, nil)
		}
		for subscription := range subscriptions {
			select {
			case <-subscription.updates:
			default:
			}
			select {
			case subscription.updates <- snapshot.Clone():
			default:
			}
		}
	}
	d.mu.Unlock()
}

func compile(
	revision string,
	value Config,
) (map[string]registry.Snapshot, int, error) {
	if len(value.Services) > maxServices {
		return nil, 0, fmt.Errorf(
			"%w: service count exceeds %d",
			ErrInvalidConfig,
			maxServices,
		)
	}
	snapshots := make(map[string]registry.Snapshot, len(value.Services))
	totalInstances := 0
	for _, service := range value.Services {
		if _, duplicate := snapshots[service.Service]; duplicate {
			return nil, 0, fmt.Errorf(
				"%w: duplicate service %q",
				ErrInvalidConfig,
				service.Service,
			)
		}
		totalInstances += len(service.Instances)
		if totalInstances > maxInstances {
			return nil, 0, fmt.Errorf(
				"%w: instance count exceeds %d",
				ErrInvalidConfig,
				maxInstances,
			)
		}
		instances := make([]registry.Instance, 0, len(service.Instances))
		for _, candidate := range service.Instances {
			instance, err := registry.NewInstance(
				candidate.ID,
				service.Service,
				candidate.Endpoints,
				candidate.Metadata,
			)
			if err != nil {
				return nil, 0, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
			}
			instances = append(instances, instance)
		}
		snapshot, err := registry.NewSnapshot(
			service.Service,
			revision,
			instances,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
		}
		snapshots[service.Service] = snapshot
	}
	return snapshots, totalInstances, nil
}

type watcher struct {
	discovery *Discovery
	service   string
	parent    context.Context
	updates   chan registry.Snapshot
	done      chan struct{}
	once      sync.Once
}

func (w *watcher) Next(
	ctx context.Context,
) (registry.Snapshot, error) {
	if ctx == nil {
		return registry.Snapshot{}, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidDiscovery,
		)
	}
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
	if w == nil {
		return nil
	}
	w.once.Do(func() {
		w.discovery.mu.Lock()
		delete(
			w.discovery.watchers[w.service],
			w,
		)
		if len(w.discovery.watchers[w.service]) == 0 {
			delete(w.discovery.watchers, w.service)
		}
		close(w.done)
		w.discovery.mu.Unlock()
	})
	return nil
}
