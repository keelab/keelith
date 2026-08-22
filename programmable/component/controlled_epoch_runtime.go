package component

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/programmable/topology"
	"github.com/keelab/keelith/programmable/topology/control"
)

const defaultControlledEpochRuntimeName = "keelith.topology.control"

var (
	// ErrInvalidControlledEpochRuntime reports incomplete lifecycle wiring.
	ErrInvalidControlledEpochRuntime = errors.New(
		"component: invalid controlled epoch runtime",
	)
	// ErrControlledEpochRuntimeStarted reports a repeated Start call.
	ErrControlledEpochRuntimeStarted = errors.New(
		"component: controlled epoch runtime already started",
	)
)

// ControlledEpochRuntimeConfig wires a revisioned Source to an EpochRuntime.
type ControlledEpochRuntimeConfig struct {
	Name          string
	Dependencies  []string
	Source        control.Source
	Runtime       *EpochRuntime
	Verifier      control.Verifier
	AllowUnsigned bool
	ApplyTimeout  time.Duration
	DrainTimeout  time.Duration
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	Sleep         control.Sleeper
	Observer      control.Observer
}

// ControlledEpochStatus is a bounded, payload-free lifecycle snapshot.
type ControlledEpochStatus struct {
	Control     control.Status
	ActiveEpoch uint64
	Running     bool
	Stopped     bool
}

// ControlledEpochRuntime bootstraps one last-good epoch, then watches updates.
// It implements the App Component contract and owns Runtime shutdown.
type ControlledEpochRuntime struct {
	name         string
	dependencies []string
	source       control.Source
	runtime      *EpochRuntime
	controller   *control.Controller

	mu      sync.Mutex
	cancel  context.CancelCauseFunc
	done    chan struct{}
	runErr  error
	started bool
	stopped bool
}

// NewControlledEpochRuntime validates and freezes control-plane wiring.
func NewControlledEpochRuntime(
	config ControlledEpochRuntimeConfig,
) (*ControlledEpochRuntime, error) {
	if config.Source == nil || config.Runtime == nil {
		return nil, ErrInvalidControlledEpochRuntime
	}
	name := strings.TrimSpace(config.Name)
	if name == "" {
		name = defaultControlledEpochRuntimeName
	}
	if config.Name != "" && config.Name != name {
		return nil, ErrInvalidControlledEpochRuntime
	}
	dependencies := append([]string(nil), config.Dependencies...)
	for _, dependency := range dependencies {
		if dependency == "" || strings.TrimSpace(dependency) != dependency {
			return nil, ErrInvalidControlledEpochRuntime
		}
	}
	target := epochControlTarget{runtime: config.Runtime}
	controller, err := control.NewController(control.ControllerConfig{
		Source:        config.Source,
		Target:        target,
		Verifier:      config.Verifier,
		AllowUnsigned: config.AllowUnsigned,
		ApplyTimeout:  config.ApplyTimeout,
		DrainTimeout:  config.DrainTimeout,
		MinBackoff:    config.MinBackoff,
		MaxBackoff:    config.MaxBackoff,
		Sleep:         config.Sleep,
		Observer:      config.Observer,
	})
	if err != nil {
		return nil, errors.Join(ErrInvalidControlledEpochRuntime, err)
	}
	return &ControlledEpochRuntime{
		name:         name,
		dependencies: dependencies,
		source:       config.Source,
		runtime:      config.Runtime,
		controller:   controller,
	}, nil
}

// Name returns the stable App component identity.
func (runtime *ControlledEpochRuntime) Name() string {
	if runtime == nil {
		return ""
	}
	return runtime.name
}

// Dependencies returns an independent App component dependency list.
func (runtime *ControlledEpochRuntime) Dependencies() []string {
	if runtime == nil {
		return nil
	}
	return append([]string(nil), runtime.dependencies...)
}

// Start synchronously applies the initial candidate before starting Watch.
func (runtime *ControlledEpochRuntime) Start(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidControlledEpochRuntime
	}
	runtime.mu.Lock()
	if runtime.started {
		runtime.mu.Unlock()
		return ErrControlledEpochRuntimeStarted
	}
	runtime.started = true
	runtime.mu.Unlock()
	if err := runtime.controller.Sync(ctx); err != nil {
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			defaultFactoryRollbackTimeout,
		)
		cleanupErr := runtime.runtime.Stop(cleanupCtx)
		cancel()
		runtime.mu.Lock()
		runtime.runErr = err
		runtime.stopped = true
		runtime.mu.Unlock()
		return errors.Join(
			err,
			cleanupErr,
			shutdownControlSource(ctx, runtime.source),
		)
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	runtime.mu.Lock()
	runtime.cancel = cancel
	runtime.done = done
	runtime.mu.Unlock()
	go func() {
		err := runtime.controller.Run(runCtx)
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) && context.Cause(runCtx) != nil {
			err = nil
		}
		runtime.mu.Lock()
		runtime.runErr = err
		runtime.mu.Unlock()
		close(done)
	}()
	return nil
}

// Stop ends Watch, drains every epoch and releases Source-owned resources.
func (runtime *ControlledEpochRuntime) Stop(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidControlledEpochRuntime
	}
	runtime.mu.Lock()
	cancel := runtime.cancel
	done := runtime.done
	started := runtime.started
	runtime.stopped = true
	runtime.mu.Unlock()
	if cancel != nil {
		cancel(context.Canceled)
	}
	var waitErr error
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			waitErr = context.Cause(ctx)
		}
	}
	runtime.mu.Lock()
	runErr := runtime.runErr
	runtime.mu.Unlock()
	if !started {
		runErr = nil
	}
	return errors.Join(
		waitErr,
		runErr,
		runtime.runtime.Stop(ctx),
		shutdownControlSource(ctx, runtime.source),
	)
}

// Acquire pins the active topology and component providers for one call.
func (runtime *ControlledEpochRuntime) Acquire(
	ctx context.Context,
) (*EpochLease, error) {
	if runtime == nil {
		return nil, ErrInvalidControlledEpochRuntime
	}
	return runtime.runtime.Acquire(ctx)
}

// AcquireKey pins the epoch selected for one stable weighted routing key.
func (runtime *ControlledEpochRuntime) AcquireKey(
	ctx context.Context,
	routingKey string,
) (*EpochLease, error) {
	if runtime == nil {
		return nil, ErrInvalidControlledEpochRuntime
	}
	return runtime.runtime.AcquireKey(ctx, routingKey)
}

// Status returns revision, degraded state and process-local active epoch.
func (runtime *ControlledEpochRuntime) Status() ControlledEpochStatus {
	if runtime == nil {
		return ControlledEpochStatus{
			Control: control.Status{
				Degraded: true, FailureClass: control.FailureSource,
			},
			Stopped: true,
		}
	}
	active, _ := runtime.runtime.Active()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return ControlledEpochStatus{
		Control:     runtime.controller.Status(),
		ActiveEpoch: active,
		Running:     runtime.started && !runtime.stopped && runtime.runErr == nil,
		Stopped:     runtime.stopped,
	}
}

// HealthCheck reports serving last-good as healthy while retaining degraded
// detail in Status and Ops.
func (runtime *ControlledEpochRuntime) HealthCheck(
	ctx context.Context,
) health.Result {
	if ctx == nil {
		return health.Unknown("invalid-context")
	}
	if cause := context.Cause(ctx); cause != nil {
		return health.Unknown("check-cancelled")
	}
	status := runtime.Status()
	if status.Stopped {
		return health.Fail("stopped")
	}
	if !status.Running || status.ActiveEpoch == 0 ||
		status.Control.AppliedRevision == 0 {
		return health.Fail("not-ready")
	}
	if status.Control.Degraded {
		return health.Pass("degraded-last-good")
	}
	return health.Pass("ready")
}

type epochControlTarget struct{ runtime *EpochRuntime }

func (target epochControlTarget) Stage(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	return target.runtime.Stage(ctx, snapshot)
}

func (target epochControlTarget) Ready(
	ctx context.Context,
	epoch uint64,
) (uint64, error) {
	return target.runtime.ReadyContext(ctx, epoch)
}

func (target epochControlTarget) Drain(
	ctx context.Context,
	epoch uint64,
) error {
	return target.runtime.Drain(ctx, epoch)
}

func (target epochControlTarget) Drainable(
	ctx context.Context,
) ([]uint64, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	return target.runtime.Drainable(), nil
}

type controlledSourceShutdown interface {
	Shutdown(context.Context) error
}

func shutdownControlSource(ctx context.Context, source control.Source) error {
	shutdown, ok := source.(controlledSourceShutdown)
	if !ok {
		return nil
	}
	return shutdown.Shutdown(ctx)
}
