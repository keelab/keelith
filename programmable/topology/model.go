// Package topology defines immutable component placement plans and bindings.
package topology

import "errors"

var (
	// ErrInvalidPlan reports a malformed or incomplete placement plan.
	ErrInvalidPlan = errors.New("topology: invalid plan")
	// ErrUnboundDependency reports a component call absent from the plan.
	ErrUnboundDependency = errors.New("topology: unbound dependency")
)

// ComponentID is the stable identity of one business component.
type ComponentID string

// PlacementID is the stable identity of one process placement group.
type PlacementID string

// BindingMode controls or describes how a dependency call is dispatched.
type BindingMode string

const (
	// BindingAuto derives the resolved mode from component placement.
	BindingAuto BindingMode = "AUTO"
	// BindingLocal dispatches to a component in the same placement.
	BindingLocal BindingMode = "LOCAL"
	// BindingRemote dispatches through a remote transport.
	BindingRemote BindingMode = "REMOTE"
)

// Plan declares one complete component placement epoch.
//
// Components maps every active component to an existing placement.
// Dependencies maps callers to their declared targets and constraints.
type Plan struct {
	Epoch        uint64
	Placements   map[PlacementID]struct{}
	Components   map[ComponentID]PlacementID
	Dependencies map[ComponentID]map[ComponentID]BindingMode
	// Traffic declares the Ready epochs eligible for new call leases. A nil
	// value preserves the legacy behavior of routing 100% to Epoch.
	Traffic []EpochWeight
}

// Snapshot is one validated and immutable activated plan.
type Snapshot struct {
	epoch        uint64
	hash         string
	placements   map[PlacementID]struct{}
	components   map[ComponentID]PlacementID
	dependencies map[ComponentID]map[ComponentID]BindingMode
	traffic      []EpochWeight
}

// Epoch returns the activated plan epoch.
func (snapshot Snapshot) Epoch() uint64 {
	return snapshot.epoch
}

// Hash returns the activated plan's canonical SHA-256 identity.
func (snapshot Snapshot) Hash() string {
	return snapshot.hash
}

// Binding is the resolved dispatch decision for one dependency edge.
type Binding struct {
	Source          ComponentID
	Target          ComponentID
	SourcePlacement PlacementID
	TargetPlacement PlacementID
	Mode            BindingMode
	Epoch           uint64
	PlanHash        string
}
