package redis

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	keelithconfig "github.com/keelab/keelith/config"
	core "github.com/keelab/keelith/governance/idempotency"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/ops"
	goredis "github.com/redis/go-redis/v9"
)

const (
	maximumRuntimeBackendTimeout = 5 * time.Second
	maximumRuntimeResultBytes    = 1024 * 1024
)

// RuntimeConfig is the strict declaration for one shared Redis-backed
// idempotency runtime. All fields are restart-bound.
type RuntimeConfig struct {
	Prefix         string        `config:"prefix" yaml:"prefix"`
	BackendTimeout time.Duration `config:"backendTimeout" yaml:"backendTimeout"`
	MaxResultBytes int           `config:"maxResultBytes" yaml:"maxResultBytes"`
}

// ValidateRuntimeConfig rejects unsafe namespaces and unbounded backend work.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	prefix := strings.TrimSpace(config.Prefix)
	if prefix != config.Prefix || len(prefix) > 128 || !utf8.ValidString(prefix) ||
		containsControl(prefix) {
		return fmt.Errorf("%w: prefix", ErrInvalidOption)
	}
	if config.BackendTimeout < 0 ||
		config.BackendTimeout > maximumRuntimeBackendTimeout {
		return fmt.Errorf("%w: backend timeout", ErrInvalidOption)
	}
	if config.MaxResultBytes < 0 ||
		config.MaxResultBytes > maximumRuntimeResultBytes {
		return fmt.Errorf("%w: max result bytes", ErrInvalidOption)
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

// Runtime composes one shared Redis Store with the transport-neutral
// idempotency middleware. It does not own the shared Redis connection.
type Runtime struct {
	store *Client
	core  *core.Runtime
}

// NewRuntime constructs a dormant runtime from generated operation rules.
func NewRuntime(
	config RuntimeConfig,
	client goredis.UniversalClient,
	registrations []core.Registration,
) (*Runtime, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	if len(registrations) == 0 {
		return nil, fmt.Errorf("%w: no idempotent operations", ErrInvalidOption)
	}
	resolver, err := core.NewStaticResolver(registrations...)
	if err != nil {
		return nil, fmt.Errorf("redis idempotency: resolver: %w", err)
	}
	store, err := FromClient(client, Config{
		Prefix:         config.Prefix,
		MaxResultBytes: config.MaxResultBytes,
	})
	if err != nil {
		return nil, err
	}
	runtime, err := core.New(core.Config{
		Store:          store,
		Resolver:       resolver,
		BackendTimeout: config.BackendTimeout,
		MaxResultBytes: config.MaxResultBytes,
	})
	if err != nil {
		_ = store.Shutdown(context.Background())
		return nil, fmt.Errorf("redis idempotency: runtime: %w", err)
	}
	return &Runtime{store: store, core: runtime}, nil
}

// Start verifies shared Redis connectivity during App startup.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || runtime.store == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	return runtime.store.Start(ctx)
}

// Shutdown releases runtime state without closing the shared Redis pool.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || runtime.store == nil {
		return nil
	}
	return runtime.store.Shutdown(ctx)
}

// Middleware returns the generated operation-scoped unary middleware.
func (runtime *Runtime) Middleware() middleware.Middleware {
	if runtime == nil || runtime.core == nil {
		return nil
	}
	return runtime.core.Middleware()
}

// CompositeRuntimeStatus reports Store and middleware counters without
// exposing operation, request, owner, key, fingerprint, or result values.
func CompositeRuntimeStatus(runtime *Runtime) ops.RuntimeStatusProvider {
	if runtime == nil || runtime.store == nil || runtime.core == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ops.ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		backend := runtime.store.Describe()
		engine := runtime.core.Describe()
		state := "active"
		if backend.Closed {
			state = "stopped"
		}
		return ops.RuntimeStatus{
			State: state,
			Ready: !backend.Closed,
			Degraded: backend.Errors > 0 || backend.StaleOwners > 0 ||
				engine.BackendErrors > 0 || engine.CompletionErrors > 0 ||
				engine.AbandonErrors > 0 || engine.CodecErrors > 0,
			Counters: []ops.RuntimeCounter{
				{Name: "abandon_errors", Value: engine.AbandonErrors},
				{Name: "abandoned", Value: backend.Abandoned},
				{Name: "acquired", Value: engine.Acquired},
				{Name: "backend_errors", Value: engine.BackendErrors + backend.Errors},
				{Name: "codec_errors", Value: engine.CodecErrors},
				{Name: "completed", Value: backend.Completed},
				{Name: "completion_errors", Value: engine.CompletionErrors},
				{Name: "conflicts", Value: engine.Conflicts},
				{Name: "handler_failures", Value: engine.HandlerFailures},
				{Name: "in_progress", Value: engine.InProgress},
				{Name: "replayed", Value: engine.Replayed},
				{Name: "stale_owners", Value: backend.StaleOwners},
			},
			Capabilities: []string{
				"atomic-claim",
				"backend:redis",
				"bounded-result-replay",
				"fail-closed",
				"fenced-completion",
				"shared-client",
			},
		}, nil
	}
}
