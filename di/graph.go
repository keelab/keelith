package di

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/keelab/keelith/app"
)

// In marks a struct as a dependency parameter object.
type In struct{}

// Out marks a struct as a provider result object.
type Out struct{}

// Cleanup releases a resource constructed while building a graph.
type Cleanup func(context.Context) error

// Scope controls provider instance reuse.
type Scope uint8

const (
	// ApplicationScope constructs one value for the graph lifetime.
	ApplicationScope Scope = iota
	// TransientScope constructs a fresh value for every dependency edge.
	TransientScope
)

// Lazy defers construction until Get is called. The result is memoized by an
// application-scoped provider and remains transient for a transient provider.
type Lazy[T any] struct {
	Resolver func() (T, error)
}

// Get resolves the lazy dependency.
func (lazy Lazy[T]) Get() (T, error) {
	if lazy.Resolver == nil {
		var zero T
		return zero, ErrInvalidLazy
	}
	return lazy.Resolver()
}

func (Lazy[T]) dependencyType() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

type lazyDescriptor interface {
	dependencyType() reflect.Type
}

// ProviderDescription is a bounded, secret-free provider snapshot.
type ProviderDescription struct {
	ID           string        `json:"id"`
	Module       string        `json:"module"`
	Type         string        `json:"type"`
	Name         string        `json:"name,omitempty"`
	Group        string        `json:"group,omitempty"`
	Scope        string        `json:"scope"`
	Dependencies []string      `json:"dependencies,omitempty"`
	Decorator    bool          `json:"decorator,omitempty"`
	Override     bool          `json:"override,omitempty"`
	Constructed  bool          `json:"constructed"`
	State        string        `json:"state"`
	Constructs   uint64        `json:"constructs"`
	Duration     time.Duration `json:"duration,omitempty"`
}

// Description is an immutable dependency graph snapshot.
type Description struct {
	Root       string                 `json:"root"`
	Generator  string                 `json:"generator,omitempty"`
	Providers  []ProviderDescription  `json:"providers"`
	Edges      []EdgeDescription      `json:"edges,omitempty"`
	Components []ComponentDescription `json:"components,omitempty"`
	Roots      []RootDescription      `json:"roots,omitempty"`
	Services   []ServiceDescription   `json:"services,omitempty"`
}

// ComponentDescription identifies a CLI-discovered application component
// without embedding configuration values or runtime objects.
type ComponentDescription struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// RootDescription identifies one process entrypoint sharing the graph.
type RootDescription struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Provider string `json:"provider,omitempty"`
}

// ServiceDescription links generated transport contracts to the wiring graph.
type ServiceDescription struct {
	Name       string   `json:"name"`
	Operations []string `json:"operations,omitempty"`
	Transports []string `json:"transports,omitempty"`
	HTTPRoutes int      `json:"httpRoutes,omitempty"`
}

// EdgeDescription is one resolved provider dependency edge.
type EdgeDescription struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

// Graph owns construction cleanup and exposes lifecycle components.
type Graph struct {
	description Description
	components  []app.Component
	cleanups    []Cleanup
	mu          sync.Mutex
	closed      bool
}

// Description returns an independent graph snapshot.
func (graph *Graph) Description() Description {
	if graph == nil {
		return Description{}
	}
	result := graph.description
	result.Providers = append([]ProviderDescription(nil), result.Providers...)
	result.Edges = append([]EdgeDescription(nil), result.Edges...)
	result.Components = append([]ComponentDescription(nil), result.Components...)
	result.Roots = append([]RootDescription(nil), result.Roots...)
	result.Services = append([]ServiceDescription(nil), result.Services...)
	for index := range result.Services {
		result.Services[index].Operations = append([]string(nil), result.Services[index].Operations...)
		result.Services[index].Transports = append([]string(nil), result.Services[index].Transports...)
	}
	for index := range result.Providers {
		result.Providers[index].Dependencies = append(
			[]string(nil), result.Providers[index].Dependencies...,
		)
	}
	return result
}

// Closed reports whether construction-owned resources have been released.
func (graph *Graph) Closed() bool {
	if graph == nil {
		return true
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	return graph.closed
}

// Components returns the app lifecycle graph discovered during construction.
func (graph *Graph) Components() []app.Component {
	if graph == nil {
		return nil
	}
	return append([]app.Component(nil), graph.components...)
}
