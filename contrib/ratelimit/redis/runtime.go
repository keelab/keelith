package redis

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	keelithconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/governance/policy"
	core "github.com/keelab/keelith/governance/ratelimit"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/ops"
	goredis "github.com/redis/go-redis/v9"
)

const maximumRuntimeBackendTimeout = 5 * time.Second

// RuntimeConfig is the strict restart-bound declaration for a Redis-backed
// distributed request rate limiter.
type RuntimeConfig struct {
	Prefix         string        `config:"prefix" yaml:"prefix"`
	FailureMode    string        `config:"failureMode" yaml:"failureMode"`
	BackendTimeout time.Duration `config:"backendTimeout" yaml:"backendTimeout"`
}

// RuntimeDescription combines the value-free Redis and distributed limiter
// snapshots.
type RuntimeDescription struct {
	Backend     Description
	Limiter     core.DistributedDescription
	FailureMode string
}

// ValidateRuntimeConfig rejects unsafe namespaces, unknown failure modes, and
// unbounded backend work. Empty values use fail-closed production defaults.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	prefix := strings.TrimSpace(config.Prefix)
	if prefix != config.Prefix || len(prefix) > 128 || !utf8.ValidString(prefix) ||
		containsControl(prefix) {
		return fmt.Errorf("%w: prefix", ErrInvalidOption)
	}
	if _, _, err := normalizeFailureMode(config.FailureMode); err != nil {
		return err
	}
	if config.BackendTimeout < 0 ||
		config.BackendTimeout > maximumRuntimeBackendTimeout {
		return fmt.Errorf("%w: backend timeout", ErrInvalidOption)
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

// Runtime composes a borrowed Redis adapter with transport-neutral policy
// resolution and distributed rate-limit middleware. It never owns the shared
// Redis connection pool.
type Runtime struct {
	backend     *Client
	limiter     *core.DistributedLimiter
	middleware  middleware.Middleware
	failureMode string
}

// NewRuntime constructs a dormant runtime. Rate/burst/concurrency values stay
// in the existing Method Policy; this configuration only selects the shared
// backend and its failure behavior.
func NewRuntime(
	config RuntimeConfig,
	client goredis.UniversalClient,
	resolver policy.Resolver,
) (*Runtime, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	if isNil(resolver) {
		return nil, fmt.Errorf("%w: policy resolver is nil", ErrInvalidOption)
	}
	mode, modeName, err := normalizeFailureMode(config.FailureMode)
	if err != nil {
		return nil, err
	}
	backend, err := FromClient(client, Config{Prefix: config.Prefix})
	if err != nil {
		return nil, err
	}
	limiter, err := core.NewDistributedLimiter(core.DistributedConfig{
		Backend:        backend,
		Mode:           mode,
		BackendTimeout: config.BackendTimeout,
	})
	if err != nil {
		_ = backend.Shutdown(context.Background())
		return nil, fmt.Errorf("redis ratelimit: runtime: %w", err)
	}
	requestMiddleware, err := core.NewDistributedMiddleware(resolver, limiter)
	if err != nil {
		_ = backend.Shutdown(context.Background())
		return nil, fmt.Errorf("redis ratelimit: middleware: %w", err)
	}
	return &Runtime{
		backend:     backend,
		limiter:     limiter,
		middleware:  requestMiddleware,
		failureMode: modeName,
	}, nil
}

// Start verifies shared Redis connectivity during App startup rollback.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || runtime.backend == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	return runtime.backend.Start(ctx)
}

// Shutdown releases adapter state without closing the shared Redis pool.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || runtime.backend == nil {
		return nil
	}
	return runtime.backend.Shutdown(ctx)
}

// Middleware returns the policy-aware distributed rate-limit stage.
func (runtime *Runtime) Middleware() middleware.Middleware {
	if runtime == nil {
		return nil
	}
	return runtime.middleware
}

// Describe returns bounded counters without quota keys or request values.
func (runtime *Runtime) Describe() RuntimeDescription {
	if runtime == nil {
		return RuntimeDescription{}
	}
	return RuntimeDescription{
		Backend:     runtime.backend.Describe(),
		Limiter:     runtime.limiter.Describe(),
		FailureMode: runtime.failureMode,
	}
}

// CompositeRuntimeStatus adapts the shared backend and logical limiter to the
// low-sensitive Ops catalog.
func CompositeRuntimeStatus(runtime *Runtime) ops.RuntimeStatusProvider {
	if runtime == nil || runtime.backend == nil || runtime.limiter == nil {
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
		if description.Backend.Closed {
			state = "stopped"
		}
		return ops.RuntimeStatus{
			State: state,
			Ready: !description.Backend.Closed,
			Degraded: description.Backend.Errors > 0 ||
				description.Limiter.BackendErrors > 0,
			Counters: []ops.RuntimeCounter{
				{Name: "accepted", Value: description.Limiter.Accepted},
				{Name: "backend_allowed", Value: description.Backend.Allowed},
				{Name: "backend_errors", Value: description.Backend.Errors},
				{Name: "backend_rejected", Value: description.Backend.Rejected},
				{Name: "fail_open", Value: description.Limiter.FailOpen},
				{Name: "local_fallback", Value: description.Limiter.LocalFallback},
				{Name: "rejected", Value: description.Limiter.Rejected},
			},
			Capabilities: []string{
				"atomic-token-bucket",
				"backend:redis",
				"failure-mode:" + description.FailureMode,
				"local-concurrency",
				"shared-client",
			},
		}, nil
	}
}

func normalizeFailureMode(value string) (core.FailureMode, string, error) {
	normalized := strings.TrimSpace(value)
	if normalized != value {
		return 0, "", fmt.Errorf("%w: failure mode", ErrInvalidOption)
	}
	switch normalized {
	case "", "fail-closed":
		return core.FailClosed, "fail-closed", nil
	case "fail-open":
		return core.FailOpen, "fail-open", nil
	case "local-fallback":
		return core.LocalFallback, "local-fallback", nil
	default:
		return 0, "", fmt.Errorf("%w: failure mode %q", ErrInvalidOption, value)
	}
}
