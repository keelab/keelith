package component

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/keelab/keelith/programmable/topology"
)

const defaultFactoryRollbackTimeout = 5 * time.Second

var (
	// ErrInvalidFactory reports a nil factory or nil constructed provider.
	ErrInvalidFactory = errors.New("component: invalid provider factory")
	// ErrProviderFactory reports a failed or panicking provider factory.
	ErrProviderFactory = errors.New("component: provider factory failed")
	// ErrProviderClose reports one or more failed provider shutdown hooks.
	ErrProviderClose = errors.New("component: provider close failed")
)

// CloseFunc releases one factory-owned provider.
type CloseFunc func(context.Context) error

// Factory is a typed lazy provider constructor.
//
// The Runtime owns a non-nil CloseFunc after a successful construction and
// calls it exactly once during rollback or Runtime.Close.
type Factory[T any] func(context.Context) (T, CloseFunc, error)

// ProviderFactory is the type-erased constructor stored by Runtime.
type ProviderFactory func(context.Context) (any, CloseFunc, error)

type providerKey struct {
	component topology.ComponentID
	mode      topology.BindingMode
}

type providerBuild struct {
	key     providerKey
	factory ProviderFactory
}

type providerCloser struct {
	key   providerKey
	close CloseFunc
}

// RegisterLocalFactory registers one typed local constructor before
// activation.
func RegisterLocalFactory[T any](
	runtime *Runtime,
	component topology.ComponentID,
	factory Factory[T],
) error {
	return registerTypedFactory(
		runtime,
		component,
		factory,
		topology.BindingLocal,
	)
}

// RegisterRemoteFactory registers one typed remote constructor before
// activation.
func RegisterRemoteFactory[T any](
	runtime *Runtime,
	component topology.ComponentID,
	factory Factory[T],
) error {
	return registerTypedFactory(
		runtime,
		component,
		factory,
		topology.BindingRemote,
	)
}

func registerTypedFactory[T any](
	runtime *Runtime,
	component topology.ComponentID,
	factory Factory[T],
	mode topology.BindingMode,
) error {
	if factory == nil {
		return ErrInvalidFactory
	}
	adapted := ProviderFactory(func(
		ctx context.Context,
	) (any, CloseFunc, error) {
		provider, closeProvider, err := factory(ctx)
		return provider, closeProvider, err
	})
	if mode == topology.BindingRemote {
		return runtime.RegisterRemoteFactory(component, adapted)
	}
	return runtime.RegisterLocalFactory(component, adapted)
}

// RegisterLocalFactory registers one type-erased local constructor.
func (runtime *Runtime) RegisterLocalFactory(
	component topology.ComponentID,
	factory ProviderFactory,
) error {
	return runtime.registerFactory(
		component,
		factory,
		topology.BindingLocal,
	)
}

// RegisterRemoteFactory registers one type-erased remote constructor.
func (runtime *Runtime) RegisterRemoteFactory(
	component topology.ComponentID,
	factory ProviderFactory,
) error {
	return runtime.registerFactory(
		component,
		factory,
		topology.BindingRemote,
	)
}

func (runtime *Runtime) registerFactory(
	component topology.ComponentID,
	factory ProviderFactory,
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
	if factory == nil {
		return fmt.Errorf(
			"%w: %s factory for %q is nil",
			ErrInvalidFactory,
			mode,
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
	factories[component] = factory
	return nil
}

// Activate builds only providers selected by snapshot and freezes Runtime.
//
// Selected factories are de-duplicated by component and binding mode. A
// failed activation closes every constructed provider in reverse order and
// leaves Runtime mutable for a retry.
func (runtime *Runtime) Activate(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if runtime == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidRuntime)
	}
	bindings, err := snapshot.Bindings()
	if err != nil {
		return fmt.Errorf("%w: topology snapshot: %w", ErrInvalidRuntime, err)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	runtime.mu.Lock()
	switch runtime.state {
	case runtimeMutable:
	case runtimeActivating:
		runtime.mu.Unlock()
		return ErrRuntimeActivating
	case runtimeFrozen:
		runtime.mu.Unlock()
		return ErrRuntimeFrozen
	case runtimeClosed:
		runtime.mu.Unlock()
		return ErrRuntimeClosed
	default:
		runtime.mu.Unlock()
		return ErrInvalidRuntime
	}
	builds, planErr := runtime.activationPlan(bindings)
	if planErr != nil {
		runtime.mu.Unlock()
		return planErr
	}
	runtime.state = runtimeActivating
	runtime.mu.Unlock()

	built := make([]providerBuildResult, 0, len(builds))
	for _, build := range builds {
		if cause := context.Cause(ctx); cause != nil {
			return runtime.failActivation(ctx, built, cause)
		}
		provider, closeProvider, factoryErr := callProviderFactory(
			ctx,
			build.factory,
		)
		if closeProvider != nil {
			built = append(built, providerBuildResult{
				key:      build.key,
				provider: provider,
				close:    closeProvider,
			})
		}
		if factoryErr != nil {
			return runtime.failActivation(
				ctx,
				built,
				fmt.Errorf(
					"%w: %s provider for %q: %w",
					ErrProviderFactory,
					build.key.mode,
					build.key.component,
					factoryErr,
				),
			)
		}
		if isNilProvider(provider) {
			return runtime.failActivation(
				ctx,
				built,
				fmt.Errorf(
					"%w: %s provider for %q is nil",
					ErrInvalidFactory,
					build.key.mode,
					build.key.component,
				),
			)
		}
		if closeProvider == nil {
			built = append(built, providerBuildResult{
				key:      build.key,
				provider: provider,
			})
		}
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.state != runtimeActivating {
		return ErrRuntimeActivating
	}
	for _, result := range built {
		providers := runtime.local
		if result.key.mode == topology.BindingRemote {
			providers = runtime.remote
		}
		providers[result.key.component] = result.provider
		if result.close != nil {
			runtime.closers = append(runtime.closers, providerCloser{
				key:   result.key,
				close: result.close,
			})
		}
	}
	runtime.snapshot = snapshot
	runtime.state = runtimeFrozen
	return nil
}

type providerBuildResult struct {
	key      providerKey
	provider any
	close    CloseFunc
}

func (runtime *Runtime) activationPlan(
	bindings []topology.Binding,
) ([]providerBuild, error) {
	selected := make(map[providerKey]struct{})
	for _, binding := range bindings {
		selected[providerKey{
			component: binding.Target,
			mode:      binding.Mode,
		}] = struct{}{}
	}
	keys := make([]providerKey, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(first, second int) bool {
		if keys[first].component != keys[second].component {
			return keys[first].component < keys[second].component
		}
		return keys[first].mode < keys[second].mode
	})
	builds := make([]providerBuild, 0, len(keys))
	for _, key := range keys {
		providers := runtime.local
		factories := runtime.localFactories
		if key.mode == topology.BindingRemote {
			providers = runtime.remote
			factories = runtime.remoteFactories
		}
		if _, exists := providers[key.component]; exists {
			continue
		}
		factory, exists := factories[key.component]
		if !exists {
			return nil, fmt.Errorf(
				"%w: %s provider for %q",
				ErrMissingBinding,
				key.mode,
				key.component,
			)
		}
		builds = append(builds, providerBuild{key: key, factory: factory})
	}
	return builds, nil
}

func (runtime *Runtime) failActivation(
	ctx context.Context,
	built []providerBuildResult,
	cause error,
) error {
	closers := make([]providerCloser, 0, len(built))
	for _, result := range built {
		if result.close != nil {
			closers = append(closers, providerCloser{
				key:   result.key,
				close: result.close,
			})
		}
	}
	rollbackCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		defaultFactoryRollbackTimeout,
	)
	closeErr := closeProviders(rollbackCtx, closers)
	cancel()
	runtime.mu.Lock()
	if runtime.state == runtimeActivating {
		runtime.state = runtimeMutable
	}
	runtime.mu.Unlock()
	return errors.Join(cause, closeErr)
}

// Close releases factory-owned providers in reverse construction order.
//
// Directly registered providers remain caller-owned. Repeated Close calls are
// idempotent.
func (runtime *Runtime) Close(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidRuntime)
	}
	runtime.mu.Lock()
	switch runtime.state {
	case runtimeActivating:
		runtime.mu.Unlock()
		return ErrRuntimeActivating
	case runtimeClosed:
		runtime.mu.Unlock()
		return nil
	case runtimeMutable, runtimeFrozen:
	default:
		runtime.mu.Unlock()
		return ErrInvalidRuntime
	}
	closers := append([]providerCloser(nil), runtime.closers...)
	runtime.closers = nil
	runtime.state = runtimeClosed
	runtime.mu.Unlock()
	return closeProviders(ctx, closers)
}

func callProviderFactory(
	ctx context.Context,
	factory ProviderFactory,
) (
	provider any,
	closeProvider CloseFunc,
	err error,
) {
	defer func() {
		if recover() != nil {
			provider = nil
			closeProvider = nil
			err = ErrProviderFactory
		}
	}()
	return factory(ctx)
}

func closeProviders(ctx context.Context, closers []providerCloser) error {
	failures := make([]error, 0)
	for index := len(closers) - 1; index >= 0; index-- {
		entry := closers[index]
		if err := callProviderClose(ctx, entry.close); err != nil {
			failures = append(failures, fmt.Errorf(
				"%w: %s provider for %q: %w",
				ErrProviderClose,
				entry.key.mode,
				entry.key.component,
				err,
			))
		}
	}
	return errors.Join(failures...)
}

func callProviderClose(
	ctx context.Context,
	closeProvider CloseFunc,
) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrProviderClose
		}
	}()
	return closeProvider(ctx)
}
