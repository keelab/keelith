package saga

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	rediscoordination "github.com/keelab/contrib/coordination/redis"
	keelithconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/coordination"
	coresaga "github.com/keelab/keelith/saga"
	goredis "github.com/redis/go-redis/v9"
)

var (
	// ErrDefinitionRegistered reports a duplicate (name, version).
	ErrDefinitionRegistered = errors.New("postgres saga: definition registered")
	// ErrDefinitionsFrozen reports registration after the first Start attempt.
	ErrDefinitionsFrozen = errors.New("postgres saga: definitions frozen")
	// ErrDefinitionNotFound reports Run for an unregistered definition.
	ErrDefinitionNotFound = errors.New("postgres saga: definition not found")
	// ErrRuntimeNotStarted reports Run before successful Start.
	ErrRuntimeNotStarted = errors.New("postgres saga: runtime not started")
	// ErrRuntimeStarting reports a concurrent Start attempt.
	ErrRuntimeStarting = errors.New("postgres saga: runtime starting")
	// ErrRuntimeClosed reports use after Shutdown.
	ErrRuntimeClosed = errors.New("postgres saga: runtime closed")
)

// RuntimeConfig declares one PostgreSQL/Redis-backed Saga runtime.
type RuntimeConfig struct {
	Table                   string        `config:"table" yaml:"table"`
	CoordinationPrefix      string        `config:"coordinationPrefix" yaml:"coordinationPrefix"`
	LeaseTTL                time.Duration `config:"leasettl" yaml:"leasettl"`
	StepTimeout             time.Duration `config:"stepTimeout" yaml:"stepTimeout"`
	MaxCompensationAttempts int           `config:"maxCompensationAttempts" yaml:"maxCompensationAttempts"`
}

// ValidateRuntimeConfig rejects invalid persistence and lifecycle budgets.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	if _, err := Schema(config.Table); err != nil {
		return fmt.Errorf("%w: table", ErrInvalidOption)
	}
	if err := rediscoordination.ValidateConfig(rediscoordination.Config{
		Prefix: config.CoordinationPrefix,
		Owns:   false,
	}); err != nil {
		return err
	}
	if config.LeaseTTL < 100*time.Millisecond ||
		config.LeaseTTL > 10*time.Minute ||
		config.StepTimeout < 10*time.Millisecond ||
		config.StepTimeout > 10*time.Minute ||
		config.MaxCompensationAttempts < 1 ||
		config.MaxCompensationAttempts > 1_000 {
		return fmt.Errorf("%w: Saga lifecycle budgets", ErrInvalidOption)
	}
	return nil
}

// NewRuntimeConfigBinding creates a strict construction-time binding.
func NewRuntimeConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[RuntimeConfig],
) (*keelithconfig.Component[RuntimeConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[RuntimeConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateRuntimeConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[RuntimeConfig](name, path, all...)
}

// Runtime owns one Repository and a non-owning renewable Redis Coordinator.
// Business code supplies immutable Definition handlers explicitly.
type Runtime struct {
	config      RuntimeConfig
	repository  coresaga.Repository
	coordinator runtimeCoordinator

	mu       sync.RWMutex
	engines  map[definitionKey]*coresaga.Engine
	frozen   bool
	starting bool
	started  bool
	closed   bool
}

type runtimeCoordinator interface {
	coordination.Coordinator
	Start(context.Context) error
	Shutdown(context.Context) error
	Description() rediscoordination.Description
}

type definitionKey struct {
	name    string
	version string
}

// NewRuntime composes Saga infrastructure from existing SQL and Redis clients.
func NewRuntime(
	config RuntimeConfig,
	database *sql.DB,
	redis goredis.UniversalClient,
) (*Runtime, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	repository, err := New(database, Options{Table: config.Table})
	if err != nil {
		return nil, err
	}
	coordinator, err := rediscoordination.New(
		redis,
		rediscoordination.Config{
			Prefix: config.CoordinationPrefix,
			Owns:   false,
		},
	)
	if err != nil {
		return nil, err
	}
	return &Runtime{
		config:      config,
		repository:  repository,
		coordinator: coordinator,
		engines:     make(map[definitionKey]*coresaga.Engine),
	}, nil
}

func (runtime *Runtime) newEngine(
	definition coresaga.Definition,
) (*coresaga.Engine, error) {
	if runtime == nil ||
		runtime.repository == nil ||
		runtime.coordinator == nil {
		return nil, fmt.Errorf("%w: Saga runtime is nil", ErrInvalidOption)
	}
	return coresaga.New(coresaga.Config{
		Definition:              definition,
		Repository:              runtime.repository,
		Coordinator:             runtime.coordinator,
		LeaseTTL:                runtime.config.LeaseTTL,
		StepTimeout:             runtime.config.StepTimeout,
		MaxCompensationAttempts: runtime.config.MaxCompensationAttempts,
	})
}

// Register binds one immutable Definition before the first Start attempt.
func (runtime *Runtime) Register(definition coresaga.Definition) error {
	engine, err := runtime.newEngine(definition)
	if err != nil {
		return err
	}
	key := definitionKey{
		name:    definition.Name,
		version: definition.Version,
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return ErrRuntimeClosed
	}
	if runtime.frozen {
		return ErrDefinitionsFrozen
	}
	if _, exists := runtime.engines[key]; exists {
		return ErrDefinitionRegistered
	}
	runtime.engines[key] = engine
	return nil
}

// Run executes one registered Definition for an application-owned Saga id.
func (runtime *Runtime) Run(
	ctx context.Context,
	definition string,
	version string,
	id string,
) (coresaga.Result, error) {
	if runtime == nil {
		return coresaga.Result{}, fmt.Errorf(
			"%w: Saga runtime is nil",
			ErrInvalidOption,
		)
	}
	runtime.mu.RLock()
	if runtime.closed {
		runtime.mu.RUnlock()
		return coresaga.Result{}, ErrRuntimeClosed
	}
	if !runtime.started {
		runtime.mu.RUnlock()
		return coresaga.Result{}, ErrRuntimeNotStarted
	}
	engine := runtime.engines[definitionKey{
		name:    definition,
		version: version,
	}]
	runtime.mu.RUnlock()
	if engine == nil {
		return coresaga.Result{}, ErrDefinitionNotFound
	}
	return engine.Run(ctx, id)
}

// Start verifies the shared Redis coordination backend.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || runtime.coordinator == nil {
		return fmt.Errorf("%w: Saga runtime is nil", ErrInvalidOption)
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return ErrRuntimeClosed
	}
	if runtime.started {
		runtime.mu.Unlock()
		return nil
	}
	if runtime.starting {
		runtime.mu.Unlock()
		return ErrRuntimeStarting
	}
	runtime.frozen = true
	runtime.starting = true
	definitions := len(runtime.engines)
	runtime.mu.Unlock()
	if definitions == 0 {
		runtime.mu.Lock()
		runtime.starting = false
		runtime.mu.Unlock()
		return fmt.Errorf("%w: no definitions registered", ErrInvalidOption)
	}
	if err := runtime.coordinator.Start(ctx); err != nil {
		runtime.mu.Lock()
		runtime.starting = false
		runtime.mu.Unlock()
		return err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.starting = false
	if runtime.closed {
		return ErrRuntimeClosed
	}
	runtime.started = true
	return nil
}

// Shutdown releases active Saga leases without closing shared Redis.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || runtime.coordinator == nil {
		return nil
	}
	runtime.mu.Lock()
	runtime.closed = true
	runtime.starting = false
	runtime.started = false
	runtime.mu.Unlock()
	return runtime.coordinator.Shutdown(ctx)
}

// RuntimeDescription is a definition- and id-free aggregate snapshot.
type RuntimeDescription struct {
	Definitions          int
	Frozen               bool
	Starting             bool
	Started              bool
	Closed               bool
	Active               int64
	Runs                 uint64
	Completed            uint64
	Compensated          uint64
	TerminalFailures     uint64
	Contended            uint64
	ActionFailures       uint64
	CompensationFailures uint64
	LeaseLosses          uint64
	RepositoryFailures   uint64
	Coordination         rediscoordination.Description
}

// Description returns low-cardinality aggregate orchestration state.
func (runtime *Runtime) Description() RuntimeDescription {
	if runtime == nil {
		return RuntimeDescription{}
	}
	runtime.mu.RLock()
	engines := make([]*coresaga.Engine, 0, len(runtime.engines))
	for _, engine := range runtime.engines {
		engines = append(engines, engine)
	}
	description := RuntimeDescription{
		Definitions: len(engines),
		Frozen:      runtime.frozen,
		Starting:    runtime.starting,
		Started:     runtime.started,
		Closed:      runtime.closed,
	}
	coordinator := runtime.coordinator
	runtime.mu.RUnlock()
	for _, engine := range engines {
		current := engine.Description()
		description.Active += current.Active
		description.Runs += current.Started
		description.Completed += current.Completed
		description.Compensated += current.Compensated
		description.TerminalFailures += current.TerminalFailures
		description.Contended += current.Contended
		description.ActionFailures += current.ActionFailures
		description.CompensationFailures += current.CompensationFailures
		description.LeaseLosses += current.LeaseLosses
		description.RepositoryFailures += current.RepositoryFailures
	}
	if coordinator != nil {
		description.Coordination = coordinator.Description()
	}
	return description
}
