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
	r *Runtime,
	component topology.ComponentID,
	factory Factory[T],
) error {
	return registerTypedFactory(
		r,
		component,
		factory,
		topology.BindingLocal,
	)
}

// RegisterRemoteFactory registers one typed remote constructor before
// activation.
func RegisterRemoteFactory[T any](
	r *Runtime,
	component topology.ComponentID,
	factory Factory[T],
) error {
	return registerTypedFactory(
		r,
		component,
		factory,
		topology.BindingRemote,
	)
}

func registerTypedFactory[T any](
	r *Runtime,
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
		return r.RegisterRemoteFactory(component, adapted)
	}
	return r.RegisterLocalFactory(component, adapted)
}

// RegisterLocalFactory registers one type-erased local constructor.
func (r *Runtime) RegisterLocalFactory(
	component topology.ComponentID,
	factory ProviderFactory,
) error {
	return r.registerFactory(
		component,
		factory,
		topology.BindingLocal,
	)
}

// RegisterRemoteFactory registers one type-erased remote constructor.
func (r *Runtime) RegisterRemoteFactory(
	component topology.ComponentID,
	factory ProviderFactory,
) error {
	return r.registerFactory(
		component,
		factory,
		topology.BindingRemote,
	)
}

func (r *Runtime) registerFactory(
	component topology.ComponentID,
	factory ProviderFactory,
	mode topology.BindingMode,
) error {
	if r == nil {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
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
	providers := r.local
	factories := r.localFactories
	if mode == topology.BindingRemote {
		providers = r.remote
		factories = r.remoteFactories
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
func (r *Runtime) Activate(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if r == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidRuntime)
	}
	bindings, err := snapshot.Bindings()
	if err != nil {
		return fmt.Errorf("%w: topology snapshot: %w", ErrInvalidRuntime, err)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	r.mu.Lock()
	switch r.state {
	case runtimeMutable:
	case runtimeActivating:
		r.mu.Unlock()
		return ErrRuntimeActivating
	case runtimeFrozen:
		r.mu.Unlock()
		return ErrRuntimeFrozen
	case runtimeClosed:
		r.mu.Unlock()
		return ErrRuntimeClosed
	default:
		r.mu.Unlock()
		return ErrInvalidRuntime
	}
	builds, planErr := r.activationPlan(bindings)
	if planErr != nil {
		r.mu.Unlock()
		return planErr
	}
	r.state = runtimeActivating
	r.mu.Unlock()

	built := make([]providerBuildResult, 0, len(builds))
	for _, build := range builds {
		if cause := context.Cause(ctx); cause != nil {
			return r.failActivation(ctx, built, cause)
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
			return r.failActivation(
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
			return r.failActivation(
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

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != runtimeActivating {
		return ErrRuntimeActivating
	}
	for _, result := range built {
		providers := r.local
		if result.key.mode == topology.BindingRemote {
			providers = r.remote
		}
		providers[result.key.component] = result.provider
		if result.close != nil {
			r.closers = append(r.closers, providerCloser{
				key:   result.key,
				close: result.close,
			})
		}
	}
	r.snapshot = snapshot
	r.state = runtimeFrozen
	return nil
}

type providerBuildResult struct {
	key      providerKey
	provider any
	close    CloseFunc
}

func (r *Runtime) activationPlan(
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
		providers := r.local
		factories := r.localFactories
		if key.mode == topology.BindingRemote {
			providers = r.remote
			factories = r.remoteFactories
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

func (r *Runtime) failActivation(
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
	r.mu.Lock()
	if r.state == runtimeActivating {
		r.state = runtimeMutable
	}
	r.mu.Unlock()
	return errors.Join(cause, closeErr)
}

// Close releases factory-owned providers in reverse construction order.
//
// Directly registered providers remain caller-owned. Repeated Close calls are
// idempotent.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidRuntime)
	}
	r.mu.Lock()
	switch r.state {
	case runtimeActivating:
		r.mu.Unlock()
		return ErrRuntimeActivating
	case runtimeClosed:
		r.mu.Unlock()
		return nil
	case runtimeMutable, runtimeFrozen:
	default:
		r.mu.Unlock()
		return ErrInvalidRuntime
	}
	closers := append([]providerCloser(nil), r.closers...)
	r.closers = nil
	r.state = runtimeClosed
	r.mu.Unlock()
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
