package continuation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var (
	// ErrInvalidMachine reports an invalid Operation or nil Machine.
	ErrInvalidMachine = errors.New("continuation: invalid machine registration")
	// ErrAlreadyRegistered reports duplicate ownership of one Operation.
	ErrAlreadyRegistered = errors.New("continuation: machine already registered")
	// ErrRegistryFrozen reports registration after Freeze.
	ErrRegistryFrozen = errors.New("continuation: registry is frozen")
	// ErrRegistryEmpty reports freezing a registry without Machines.
	ErrRegistryEmpty = errors.New("continuation: registry is empty")
	// ErrMachineNotFound reports a durable call without a registered Machine.
	ErrMachineNotFound = errors.New("continuation: machine not found")
)

// Machine advances one immutable durable Snapshot by one Transition.
//
// Advance has at-least-once execution semantics: the process may stop after
// an external side effect succeeds but before Transition commits. Every
// external effect must therefore use CallID and Revision as an idempotency
// identity or provide an equivalent application-owned guarantee.
type Machine interface {
	Advance(context.Context, Snapshot) (Transition, error)
}

// MachineFunc adapts a function to Machine.
type MachineFunc func(context.Context, Snapshot) (Transition, error)

// Advance implements Machine.
func (fn MachineFunc) Advance(
	ctx context.Context,
	snapshot Snapshot,
) (Transition, error) {
	return fn(ctx, snapshot)
}

// Registry owns the immutable Operation-to-Machine mapping.
type Registry struct {
	mu        sync.RWMutex
	machines  map[string]Machine
	workflows map[string]map[string]*WorkflowDefinition
	frozen    bool
}

// NewRegistry constructs an empty mutable Registry.
func NewRegistry() *Registry {
	return &Registry{
		machines:  make(map[string]Machine),
		workflows: make(map[string]map[string]*WorkflowDefinition),
	}
}

// RegisterWorkflow assigns one immutable definition version before Freeze.
func (registry *Registry) RegisterWorkflow(definition *WorkflowDefinition) error {
	if registry == nil || definition == nil ||
		!validOperation(definition.operation.value) ||
		definition.version == "" || definition.fingerprint == "" {
		return ErrInvalidWorkflow
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.frozen {
		return ErrRegistryFrozen
	}
	if registry.workflows == nil {
		registry.workflows = make(map[string]map[string]*WorkflowDefinition)
	}
	versions := registry.workflows[definition.operation.String()]
	if versions == nil {
		versions = make(map[string]*WorkflowDefinition)
		registry.workflows[definition.operation.String()] = versions
	}
	if _, exists := versions[definition.version]; exists {
		return fmt.Errorf(
			"%w: %s@%s",
			ErrAlreadyRegistered,
			definition.operation.String(),
			definition.version,
		)
	}
	versions[definition.version] = definition
	return nil
}

// Register assigns one Machine before Freeze.
func (registry *Registry) Register(
	operation Operation,
	machine Machine,
) error {
	if registry == nil ||
		!validOperation(operation.value) ||
		isNilMachine(machine) {
		return ErrInvalidMachine
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.frozen {
		return ErrRegistryFrozen
	}
	key := operation.String()
	if _, exists := registry.machines[key]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyRegistered, key)
	}
	registry.machines[key] = machine
	return nil
}

// Freeze rejects further registration and publishes an immutable mapping.
func (registry *Registry) Freeze() error {
	if registry == nil {
		return ErrInvalidMachine
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.frozen {
		return nil
	}
	if len(registry.machines) == 0 && len(registry.workflows) == 0 {
		return ErrRegistryEmpty
	}
	registry.frozen = true
	return nil
}

// ResolveWorkflow returns one exact frozen operation/version definition.
func (registry *Registry) ResolveWorkflow(
	operation Operation,
	version string,
) (*WorkflowDefinition, bool) {
	if registry == nil || !validOperation(operation.value) || !validIdentity(version) {
		return nil, false
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	definition, exists := registry.workflows[operation.String()][version]
	return definition, exists
}

// Frozen reports whether registration has ended.
func (registry *Registry) Frozen() bool {
	if registry == nil {
		return false
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return registry.frozen
}

// Resolve returns the exact registered Machine.
func (registry *Registry) Resolve(operation Operation) (Machine, bool) {
	if registry == nil || !validOperation(operation.value) {
		return nil, false
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	machine, exists := registry.machines[operation.String()]
	return machine, exists
}

func isNilMachine(machine Machine) bool {
	if machine == nil {
		return true
	}
	value := reflect.ValueOf(machine)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
