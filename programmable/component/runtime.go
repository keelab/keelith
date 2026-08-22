package component

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/programmable/topology"
)

const maxComponentIDBytes = 512

var (
	// ErrInvalidRuntime reports a nil runtime, provider, or component identity.
	ErrInvalidRuntime = errors.New("component: invalid runtime")
	// ErrRuntimeFrozen reports registration or refreezing after Freeze.
	ErrRuntimeFrozen = errors.New("component: runtime frozen")
	// ErrRuntimeNotFrozen reports Bind before Freeze.
	ErrRuntimeNotFrozen = errors.New("component: runtime not frozen")
	// ErrRuntimeActivating reports mutation while factories are being built.
	ErrRuntimeActivating = errors.New("component: runtime activating")
	// ErrRuntimeClosed reports use after provider shutdown.
	ErrRuntimeClosed = errors.New("component: runtime closed")
	// ErrDuplicateBinding reports a repeated provider for one mode and target.
	ErrDuplicateBinding = errors.New("component: duplicate binding")
	// ErrMissingBinding reports that the selected mode has no provider.
	ErrMissingBinding = errors.New("component: missing binding")
	// ErrBindingType reports that the selected provider does not implement T.
	ErrBindingType = errors.New("component: binding type mismatch")
)

type runtimeState uint8

const (
	runtimeMutable runtimeState = iota
	runtimeActivating
	runtimeFrozen
	runtimeClosed
)

// Runtime registers providers during construction and freezes exactly once.
//
// It does not watch a topology Manager. Loading a new epoch requires a new
// Runtime owned by the new process epoch.
type Runtime struct {
	mu              sync.RWMutex
	state           runtimeState
	snapshot        topology.Snapshot
	local           map[topology.ComponentID]any
	remote          map[topology.ComponentID]any
	localFactories  map[topology.ComponentID]ProviderFactory
	remoteFactories map[topology.ComponentID]ProviderFactory
	closers         []providerCloser
}

// NewRuntime creates an unfrozen provider registry.
func NewRuntime() *Runtime {
	return &Runtime{
		local:           make(map[topology.ComponentID]any),
		remote:          make(map[topology.ComponentID]any),
		localFactories:  make(map[topology.ComponentID]ProviderFactory),
		remoteFactories: make(map[topology.ComponentID]ProviderFactory),
	}
}

// RegisterLocal registers one in-process provider before Freeze.
func (runtime *Runtime) RegisterLocal(
	component topology.ComponentID,
	provider any,
) error {
	return runtime.register(component, provider, topology.BindingLocal)
}

// RegisterRemote registers one remote provider before Freeze.
func (runtime *Runtime) RegisterRemote(
	component topology.ComponentID,
	provider any,
) error {
	return runtime.register(component, provider, topology.BindingRemote)
}

// Freeze captures one activated topology Snapshot for the Runtime lifetime.
func (runtime *Runtime) Freeze(snapshot topology.Snapshot) error {
	if runtime == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidRuntime)
	}
	if snapshot.Epoch() == 0 || snapshot.Hash() == "" {
		return fmt.Errorf(
			"%w: topology snapshot is not activated",
			ErrInvalidRuntime,
		)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	switch runtime.state {
	case runtimeMutable:
	case runtimeActivating:
		return ErrRuntimeActivating
	case runtimeFrozen:
		return ErrRuntimeFrozen
	case runtimeClosed:
		return ErrRuntimeClosed
	default:
		return ErrInvalidRuntime
	}
	runtime.snapshot = snapshot
	runtime.state = runtimeFrozen
	return nil
}

func (runtime *Runtime) register(
	component topology.ComponentID,
	provider any,
	mode topology.BindingMode,
) error {
	if runtime == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidRuntime)
	}
	if !validComponentID(component) {
		return fmt.Errorf(
			"%w: component %q is malformed",
			ErrInvalidRuntime,
			component,
		)
	}
	if isNilProvider(provider) {
		return fmt.Errorf(
			"%w: provider for %q is nil",
			ErrInvalidRuntime,
			component,
		)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	switch runtime.state {
	case runtimeMutable:
	case runtimeActivating:
		return ErrRuntimeActivating
	case runtimeFrozen:
		return ErrRuntimeFrozen
	case runtimeClosed:
		return ErrRuntimeClosed
	default:
		return ErrInvalidRuntime
	}
	providers := runtime.local
	factories := runtime.localFactories
	if mode == topology.BindingRemote {
		providers = runtime.remote
		factories = runtime.remoteFactories
	}
	if _, duplicate := providers[component]; duplicate ||
		factories[component] != nil {
		return fmt.Errorf(
			"%w: %s provider for %q",
			ErrDuplicateBinding,
			mode,
			component,
		)
	}
	providers[component] = provider
	return nil
}

// Bind resolves one declared edge and returns only its selected typed provider.
func Bind[T any](
	runtime *Runtime,
	source topology.ComponentID,
	target topology.ComponentID,
) (Ref[T], error) {
	if runtime == nil {
		return Ref[T]{}, fmt.Errorf("%w: runtime is nil", ErrInvalidRuntime)
	}
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	switch runtime.state {
	case runtimeFrozen:
	case runtimeClosed:
		return Ref[T]{}, ErrRuntimeClosed
	default:
		return Ref[T]{}, ErrRuntimeNotFrozen
	}
	binding, err := runtime.snapshot.Resolve(source, target)
	if err != nil {
		return Ref[T]{}, err
	}
	providers := runtime.local
	if binding.Mode == topology.BindingRemote {
		providers = runtime.remote
	}
	value, exists := providers[target]
	if !exists {
		return Ref[T]{}, fmt.Errorf(
			"%w: %s provider for %q",
			ErrMissingBinding,
			binding.Mode,
			target,
		)
	}
	provider, ok := value.(T)
	if !ok {
		return Ref[T]{}, fmt.Errorf(
			"%w: %s provider for %q",
			ErrBindingType,
			binding.Mode,
			target,
		)
	}
	return Ref[T]{provider: provider, binding: binding}, nil
}

func validComponentID(component topology.ComponentID) bool {
	value := string(component)
	if value == "" ||
		len(value) > maxComponentIDBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func isNilProvider(provider any) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
