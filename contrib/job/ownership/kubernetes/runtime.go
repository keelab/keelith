// Package kubernetes composes a precreated Kubernetes Lease coordinator and
// one local Cron scheduler into a lifecycle-owned distributed scheduler.
package kubernetes

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	coordinationkubernetes "github.com/keelab/contrib/coordination/kubernetes"
	keelithconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/coordination"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/worker"
	ownedworker "github.com/keelab/keelith/worker/owned"
)

const (
	minimumttl = 3 * time.Second
	maximumttl = 24 * time.Hour
)

// RuntimeConfig is the strict restart-bound declaration for one
// Kubernetes-backed Cron ownership boundary. Identity is supplied by the
// immutable application instance rather than duplicated in configuration.
type RuntimeConfig struct {
	Key       string        `config:"key" yaml:"key"`
	TTL       time.Duration `config:"ttl" yaml:"ttl"`
	Namespace string        `config:"namespace" yaml:"namespace"`
	LeaseName string        `config:"leaseName" yaml:"lease_name"`
}

// RuntimeDescription combines value-free Kubernetes coordination and owned
// scheduler state.
type RuntimeDescription struct {
	Coordination coordinationkubernetes.Description
	Scheduler    ownedworker.Description
}

// ValidateRuntimeConfig rejects ambiguous keys, unsafe Lease names, and
// ownership budgets without opening an in-cluster client.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	if config.Key == "" || strings.TrimSpace(config.Key) != config.Key ||
		config.Namespace == "" ||
		strings.TrimSpace(config.Namespace) != config.Namespace ||
		config.LeaseName == "" ||
		strings.TrimSpace(config.LeaseName) != config.LeaseName {
		return fmt.Errorf(
			"%w: ownership key, namespace, or Lease name",
			ownedworker.ErrInvalidOption,
		)
	}
	if config.TTL < minimumttl || config.TTL > maximumttl {
		return fmt.Errorf(
			"%w: ttl must be within %s..%s",
			ownedworker.ErrInvalidOption,
			minimumttl,
			maximumttl,
		)
	}
	if err := coordinationkubernetes.ValidateOptions(
		coordinationkubernetes.Options{
			Namespace: config.Namespace,
			Identity:  "keelith-runtime-validation",
			Leases: map[string]string{
				config.Key: config.LeaseName,
			},
		},
	); err != nil {
		return err
	}
	return nil
}

// NewRuntimeConfigBinding creates one strict restart-bound config binding.
func NewRuntimeConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[RuntimeConfig],
) (*keelithconfig.Component[RuntimeConfig], error) {
	all := make([]keelithconfig.ComponentOption[RuntimeConfig], 0, len(options)+1)
	all = append(all, keelithconfig.WithComponentValidator(ValidateRuntimeConfig))
	all = append(all, options...)
	return keelithconfig.NewComponent[RuntimeConfig](name, path, all...)
}

type lifecycleCoordinator interface {
	coordination.Coordinator
	Start(context.Context) error
	Shutdown(context.Context) error
	Description() coordinationkubernetes.Description
}

// Runtime owns the in-cluster Lease coordinator and exposes its owned
// Scheduler. The wrapped local Scheduler remains externally owned.
type Runtime struct {
	coordinator lifecycleCoordinator
	scheduler   *ownedworker.Scheduler
}

// NewRuntime opens one namespace-scoped in-cluster lease client and composes
// it with an existing local Scheduler.
func NewRuntime(
	config RuntimeConfig,
	identity string,
	scheduler worker.Scheduler,
) (*Runtime, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	if identity == "" || strings.TrimSpace(identity) != identity {
		return nil, fmt.Errorf(
			"%w: identity is invalid",
			ownedworker.ErrInvalidOption,
		)
	}
	coordinator, err := coordinationkubernetes.OpenInCluster(
		coordinationkubernetes.Options{
			Namespace: config.Namespace,
			Identity:  identity,
			Leases: map[string]string{
				config.Key: config.LeaseName,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	runtime, err := newRuntime(config, coordinator, scheduler)
	if err != nil {
		_ = coordinator.Shutdown(context.Background())
		return nil, err
	}
	return runtime, nil
}

func newRuntime(
	config RuntimeConfig,
	coordinator lifecycleCoordinator,
	scheduler worker.Scheduler,
) (*Runtime, error) {
	if isNil(coordinator) || isNil(scheduler) {
		return nil, fmt.Errorf(
			"%w: coordinator or scheduler is nil",
			ownedworker.ErrInvalidOption,
		)
	}
	owned, err := ownedworker.New(ownedworker.Config{
		Key:         config.Key,
		TTL:         config.TTL,
		Coordinator: coordinator,
		Scheduler:   scheduler,
	})
	if err != nil {
		return nil, err
	}
	return &Runtime{coordinator: coordinator, scheduler: owned}, nil
}

// Start verifies the precreated Lease and persistent fencing marker before App
// servers start.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || isNil(runtime.coordinator) {
		return fmt.Errorf("%w: runtime is nil", ownedworker.ErrInvalidOption)
	}
	return runtime.coordinator.Start(ctx)
}

// Shutdown releases active ownership and stops renewals.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || isNil(runtime.coordinator) {
		return nil
	}
	return runtime.coordinator.Shutdown(ctx)
}

// Scheduler returns the lease-owned scheduler used by worker.Job.
func (runtime *Runtime) Scheduler() *ownedworker.Scheduler {
	if runtime == nil {
		return nil
	}
	return runtime.scheduler
}

// Describe returns bounded aggregate state without keys, Lease names,
// identities, tokens, resource versions, or error text.
func (runtime *Runtime) Describe() RuntimeDescription {
	if runtime == nil || isNil(runtime.coordinator) {
		return RuntimeDescription{}
	}
	return RuntimeDescription{
		Coordination: runtime.coordinator.Description(),
		Scheduler:    runtime.scheduler.Description(),
	}
}

// CompositeRuntimeStatus adapts Kubernetes ownership to the low-sensitive Ops
// catalog.
func CompositeRuntimeStatus(runtime *Runtime) ops.RuntimeStatusProvider {
	if runtime == nil || isNil(runtime.coordinator) || runtime.scheduler == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ops.ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := runtime.Describe()
		state := "active"
		if description.Coordination.Closed {
			state = "stopped"
		}
		return ops.RuntimeStatus{
			State: state,
			Ready: !description.Coordination.Closed,
			Degraded: description.Coordination.BackendFailures > 0 ||
				description.Coordination.Lost > 0 ||
				description.Scheduler.AcquireFailures > 0 ||
				description.Scheduler.LeaseLosses > 0 ||
				description.Scheduler.ReleaseFailures > 0,
			Active: description.Scheduler.Active,
			Counters: []ops.RuntimeCounter{
				{Name: "acquire_failures", Value: description.Scheduler.AcquireFailures},
				{Name: "acquired", Value: description.Scheduler.Acquired},
				{Name: "backend_failures", Value: description.Coordination.BackendFailures},
				{Name: "contended", Value: description.Scheduler.Contended},
				{Name: "lease_losses", Value: description.Scheduler.LeaseLosses},
				{Name: "release_failures", Value: description.Scheduler.ReleaseFailures},
				{Name: "released", Value: description.Coordination.Released},
			},
			Capabilities: []string{
				"backend:kubernetes",
				"fencing",
				"precreated-leases",
				"renewable-ownership",
			},
		}, nil
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
