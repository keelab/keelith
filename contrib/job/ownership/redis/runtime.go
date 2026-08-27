// Package redis composes a declared Redis client, renewable coordination,
// and one local Cron scheduler into a lifecycle-owned distributed scheduler.
package redis

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	coordinationredis "github.com/keelab/contrib/coordination/redis"
	keelithconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/coordination"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/worker"
	ownedworker "github.com/keelab/keelith/worker/owned"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultPrefix  = "keelith:lease:"
	minimumttl     = 3 * time.Second
	maximumttl     = 24 * time.Hour
	maximumKeySize = 512
)

// RuntimeConfig is the strict restart-bound declaration for one Redis-backed
// Cron ownership boundary.
type RuntimeConfig struct {
	Key    string        `config:"key" yaml:"key"`
	TTL    time.Duration `config:"ttl" yaml:"ttl"`
	Prefix string        `config:"prefix" yaml:"prefix"`
}

// RuntimeDescription combines value-free coordination and scheduler state.
type RuntimeDescription struct {
	Coordination coordinationredis.Description
	Scheduler    ownedworker.Description
}

// ValidateRuntimeConfig rejects ambiguous keys and unsafe lease budgets.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	if !validText(config.Key, maximumKeySize) {
		return fmt.Errorf("%w: ownership key", ownedworker.ErrInvalidOption)
	}
	if config.TTL < minimumttl || config.TTL > maximumttl {
		return fmt.Errorf(
			"%w: ttl must be within %s..%s",
			ownedworker.ErrInvalidOption,
			minimumttl,
			maximumttl,
		)
	}
	prefix := config.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	if strings.TrimSpace(prefix) != prefix || !utf8.ValidString(prefix) {
		return fmt.Errorf(
			"%w: coordination prefix",
			ownedworker.ErrInvalidOption,
		)
	}
	if err := coordinationredis.ValidateConfig(
		coordinationredis.Config{Prefix: prefix},
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
	Description() coordinationredis.Description
}

// Runtime owns the Redis lease coordinator and exposes its owned Scheduler.
// The supplied Redis pool and wrapped local Scheduler remain externally owned.
type Runtime struct {
	coordinator lifecycleCoordinator
	scheduler   *ownedworker.Scheduler
}

// NewRuntime composes one borrowed Redis pool and one local Scheduler.
func NewRuntime(
	config RuntimeConfig,
	client goredis.UniversalClient,
	scheduler worker.Scheduler,
) (*Runtime, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	prefix := config.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	coordinator, err := coordinationredis.New(
		client,
		coordinationredis.Config{Prefix: prefix},
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

// Start verifies the borrowed Redis backend before App servers start.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || isNil(runtime.coordinator) {
		return fmt.Errorf("%w: runtime is nil", ownedworker.ErrInvalidOption)
	}
	return runtime.coordinator.Start(ctx)
}

// Shutdown releases active leases without closing the shared Redis pool.
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

// Describe returns bounded aggregate state without lease keys or errors.
func (runtime *Runtime) Describe() RuntimeDescription {
	if runtime == nil || isNil(runtime.coordinator) {
		return RuntimeDescription{}
	}
	return RuntimeDescription{
		Coordination: runtime.coordinator.Description(),
		Scheduler:    runtime.scheduler.Description(),
	}
}

// CompositeRuntimeStatus adapts ownership state to the low-sensitive Ops
// catalog without exposing logical job or backend keys.
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
				"backend:redis",
				"fencing",
				"renewable-ownership",
				"shared-client",
			},
		}, nil
	}
}

func validText(value string, maximum int) bool {
	if value == "" || len(value) > maximum ||
		strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
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
