// Package registry defines infrastructure-neutral registration and discovery
// contracts.
package registry

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"sort"
	"strings"
	"unicode"
)

var (
	// ErrInvalidInstance means an instance identity or endpoint is malformed.
	ErrInvalidInstance = errors.New("registry: invalid instance")
	// ErrInvalidSnapshot means a discovery snapshot is malformed.
	ErrInvalidSnapshot = errors.New("registry: invalid snapshot")
	// ErrDuplicateEndpoint means two instances claim the same endpoint.
	ErrDuplicateEndpoint = errors.New("registry: duplicate endpoint")
	// ErrDuplicateInstance means a snapshot contains the same ID twice.
	ErrDuplicateInstance = errors.New("registry: duplicate instance")
	// ErrWatcherClosed means a discovery Watcher was closed.
	ErrWatcherClosed = errors.New("registry: watcher closed")
)

// Registrar changes registered service instances.
type Registrar interface {
	// Register adds a new instance to the registry.
	Register(context.Context, Instance) error
	// Deregister removes an instance from the registry.
	Deregister(context.Context, Instance) error
}

// Discovery creates full-snapshot service watchers.
type Discovery interface {
	// Watch returns a Watcher for the given service.
	Watch(context.Context, string) (Watcher, error)
}

// Watcher returns complete revisioned Snapshots.
type Watcher interface {
	// Next returns the next Snapshot from the watcher.
	Next(context.Context) (Snapshot, error)
	// Close closes the watcher.
	Close() error
}

// Instance is an immutable service instance.
type Instance struct {
	id        string
	service   string            // the service name
	endpoints []string          // the service endpoints
	metadata  map[string]string // the service metadata
}

// NewInstance validates and defensively copies an Instance.
func NewInstance(id string, service string, endpoints []string, metadata map[string]string) (Instance, error) {
	if !validName(id) {
		return Instance{}, fmt.Errorf("%w: id %q", ErrInvalidInstance, id)
	}
	if !validName(service) {
		return Instance{}, fmt.Errorf("%w: service %q", ErrInvalidInstance, service)
	}
	if len(endpoints) == 0 {
		return Instance{}, fmt.Errorf("%w: no endpoints", ErrInvalidInstance)
	}

	normalizedEndpoints := make([]string, 0, len(endpoints))
	seenEndpoints := make(map[string]struct{}, len(endpoints))

	for _, endpoint := range endpoints {
		normalized, err := normalizeEndpoint(endpoint)
		if err != nil {
			return Instance{}, err
		}
		if _, duplicate := seenEndpoints[normalized]; duplicate {
			return Instance{}, fmt.Errorf("%w: %s", ErrDuplicateEndpoint, normalized)
		}
		seenEndpoints[normalized] = struct{}{}
		normalizedEndpoints = append(normalizedEndpoints, normalized)
	}

	clonedMetadata := make(map[string]string, len(metadata))

	for key, value := range metadata {
		if strings.TrimSpace(key) == "" {
			return Instance{}, fmt.Errorf("%w: empty metadata key", ErrInvalidInstance)
		}
		clonedMetadata[key] = value
	}

	return Instance{
		id:        id,
		service:   service,
		endpoints: normalizedEndpoints,
		metadata:  clonedMetadata,
	}, nil
}

// ID returns the stable instance identity.
func (i Instance) ID() string {
	return i.id
}

// Service returns the logical service name.
func (i Instance) Service() string {
	return i.service
}

// Endpoints returns an independent endpoint list.
func (i Instance) Endpoints() []string {
	return append([]string(nil), i.endpoints...)
}

// Endpoint returns the first endpoint matching scheme.
func (i Instance) Endpoint(scheme string) (string, bool) {
	normalizedScheme := strings.ToLower(scheme)
	for _, endpoint := range i.endpoints {
		parsed, err := url.Parse(endpoint)
		if err == nil && parsed.Scheme == normalizedScheme {
			return endpoint, true
		}
	}

	return "", false
}

// Metadata returns an independent metadata map.
func (i Instance) Metadata() map[string]string {
	return cloneMetadata(i.metadata)
}

// Validate verifies a possibly zero-valued Instance.
func (i Instance) Validate() error {
	_, err := NewInstance(i.id, i.service, i.endpoints, i.metadata)
	return err
}

// Equal reports whether two immutable Instances have identical content.
func (i Instance) Equal(other Instance) bool {
	if i.id != other.id || i.service != other.service || len(i.endpoints) != len(other.endpoints) || len(i.metadata) != len(other.metadata) {
		return false
	}

	for index, endpoint := range i.endpoints {
		if endpoint != other.endpoints[index] {
			return false
		}
	}

	for key, value := range i.metadata {
		if other.metadata[key] != value {
			return false
		}
	}

	return true
}

// Clone returns an independent immutable copy.
func (i Instance) Clone() Instance {
	return Instance{id: i.id, service: i.service, endpoints: i.Endpoints(), metadata: i.Metadata()}
}

// Snapshot is an immutable full service discovery snapshot.
type Snapshot struct {
	service   string
	revision  string
	instances []Instance
}

// NewSnapshot validates uniqueness and defensively copies instances.
func NewSnapshot(service, revision string, instances []Instance) (Snapshot, error) {
	if !validName(service) {
		return Snapshot{}, fmt.Errorf("%w: service %q", ErrInvalidSnapshot, service)
	}
	if strings.TrimSpace(revision) == "" {
		return Snapshot{}, fmt.Errorf("%w: revision is empty", ErrInvalidSnapshot)
	}

	cloned := make([]Instance, 0, len(instances))
	ids := make(map[string]struct{}, len(instances))
	endpoints := make(map[string]string)

	for _, instance := range instances {
		if err := instance.Validate(); err != nil {
			return Snapshot{}, fmt.Errorf("%w: %w", ErrInvalidSnapshot, err)
		}
		if instance.service != service {
			return Snapshot{}, fmt.Errorf(
				"%w: instance %q belongs to %q",
				ErrInvalidSnapshot,
				instance.id,
				instance.service,
			)
		}
		if _, duplicate := ids[instance.id]; duplicate {
			return Snapshot{}, fmt.Errorf("%w: %s", ErrDuplicateInstance, instance.id)
		}
		ids[instance.id] = struct{}{}
		for _, endpoint := range instance.endpoints {
			if owner, duplicate := endpoints[endpoint]; duplicate {
				return Snapshot{}, fmt.Errorf("%w: %s is owned by %s and %s", ErrDuplicateEndpoint, endpoint, owner, instance.id)
			}
			endpoints[endpoint] = instance.id
		}
		cloned = append(cloned, instance.Clone())
	}
	sort.Slice(cloned, func(first, second int) bool {
		return cloned[first].id < cloned[second].id
	})

	return Snapshot{service: service, revision: revision, instances: cloned}, nil
}

// Service returns the watched service.
func (s Snapshot) Service() string {
	return s.service
}

// Revision returns the backend revision.
func (s Snapshot) Revision() string {
	return s.revision
}

// Instances returns independent immutable instances.
func (s Snapshot) Instances() []Instance {
	instances := make([]Instance, len(s.instances))

	for index, instance := range s.instances {
		instances[index] = instance.Clone()
	}

	return instances
}

// Clone returns a deep immutable copy.
func (s Snapshot) Clone() Snapshot {
	return Snapshot{
		service:   s.service,
		revision:  s.revision,
		instances: s.Instances(),
	}
}

// Validate verifies a possibly zero-valued Snapshot.
func (s Snapshot) Validate() error {
	_, err := NewSnapshot(s.service, s.revision, s.instances)
	return err
}

func normalizeEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Host == "" && parsed.Path == "" {
		return "", fmt.Errorf("%w: endpoint %q", ErrInvalidInstance, endpoint)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	return parsed.String(), nil
}

func validName(name string) bool {
	if strings.TrimSpace(name) != name || name == "" {
		return false
	}

	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}

	return true
}

func cloneMetadata(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]string, len(source))

	maps.Copy(clone, source)

	return clone
}
