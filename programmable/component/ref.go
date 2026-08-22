// Package component binds typed local or remote providers to frozen topology.
package component

import "github.com/keelab/keelith/programmable/topology"

// Ref is an immutable typed component reference.
//
// Get always returns the provider selected when Bind captured the topology
// binding. A Ref never re-resolves against a newer plan epoch.
type Ref[T any] struct {
	provider T
	binding  topology.Binding
}

// Get returns the frozen typed provider.
func (reference Ref[T]) Get() T {
	return reference.provider
}

// Binding returns the frozen topology decision.
func (reference Ref[T]) Binding() topology.Binding {
	return reference.binding
}

// Mode returns the frozen local or remote dispatch mode.
func (reference Ref[T]) Mode() topology.BindingMode {
	return reference.binding.Mode
}

// Epoch returns the frozen plan epoch for diagnostics.
func (reference Ref[T]) Epoch() uint64 {
	return reference.binding.Epoch
}

// PlanHash returns the frozen canonical plan identity for diagnostics.
func (reference Ref[T]) PlanHash() string {
	return reference.binding.PlanHash
}
