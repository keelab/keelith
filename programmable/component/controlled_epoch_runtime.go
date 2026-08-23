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
func (cr *ControlledEpochRuntime) Name() string {
	if cr == nil {
		return ""
	}
	return cr.name
}

// Dependencies returns an independent App component dependency list.
func (cr *ControlledEpochRuntime) Dependencies() []string {
	if cr == nil {
		return nil
	}
	return append([]string(nil), cr.dependencies...)
}

// Start synchronously applies the initial candidate before starting Watch.
func (cr *ControlledEpochRuntime) Start(ctx context.Context) error {
	if cr == nil || ctx == nil {
		return ErrInvalidControlledEpochRuntime
	}
	cr.mu.Lock()
	if cr.started {
		cr.mu.Unlock()
		return ErrControlledEpochRuntimeStarted
	}
	cr.started = true
	cr.mu.Unlock()
	if err := cr.controller.Sync(ctx); err != nil {
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			defaultFactoryRollbackTimeout,
		)
		cleanupErr := cr.runtime.Stop(cleanupCtx)
		cancel()
		cr.mu.Lock()
		cr.runErr = err
		cr.stopped = true
		cr.mu.Unlock()
		return errors.Join(
			err,
			cleanupErr,
			shutdownControlSource(ctx, cr.source),
		)
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	cr.mu.Lock()
	cr.cancel = cancel
	cr.done = done
	cr.mu.Unlock()
	go func() {
		err := cr.controller.Run(runCtx)
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) && context.Cause(runCtx) != nil {
			err = nil
		}
		cr.mu.Lock()
		cr.runErr = err
		cr.mu.Unlock()
		close(done)
	}()
	return nil
}

// Stop ends Watch, drains every epoch and releases Source-owned resources.
func (cr *ControlledEpochRuntime) Stop(ctx context.Context) error {
	if cr == nil || ctx == nil {
		return ErrInvalidControlledEpochRuntime
	}
	cr.mu.Lock()
	cancel := cr.cancel
	done := cr.done
	started := cr.started
	cr.stopped = true
	cr.mu.Unlock()
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
	cr.mu.Lock()
	runErr := cr.runErr
	cr.mu.Unlock()
	if !started {
		runErr = nil
	}
	return errors.Join(
		waitErr,
		runErr,
		cr.runtime.Stop(ctx),
		shutdownControlSource(ctx, cr.source),
	)
}

// Acquire pins the active topology and component providers for one call.
func (cr *ControlledEpochRuntime) Acquire(
	ctx context.Context,
) (*EpochLease, error) {
	if cr == nil {
		return nil, ErrInvalidControlledEpochRuntime
	}
	return cr.runtime.Acquire(ctx)
}

// AcquireKey pins the epoch selected for one stable weighted routing key.
func (cr *ControlledEpochRuntime) AcquireKey(
	ctx context.Context,
	routingKey string,
) (*EpochLease, error) {
	if cr == nil {
		return nil, ErrInvalidControlledEpochRuntime
	}
	return cr.runtime.AcquireKey(ctx, routingKey)
}

// Status returns revision, degraded state and process-local active epoch.
func (cr *ControlledEpochRuntime) Status() ControlledEpochStatus {
	if cr == nil {
		return ControlledEpochStatus{
			Control: control.Status{
				Degraded: true, FailureClass: control.FailureSource,
			},
			Stopped: true,
		}
	}
	active, _ := cr.runtime.Active()
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return ControlledEpochStatus{
		Control:     cr.controller.Status(),
		ActiveEpoch: active,
		Running:     cr.started && !cr.stopped && cr.runErr == nil,
		Stopped:     cr.stopped,
	}
}

// HealthCheck reports serving last-good as healthy while retaining degraded
// detail in Status and Ops.
func (cr *ControlledEpochRuntime) HealthCheck(
	ctx context.Context,
) health.Result {
	if ctx == nil {
		return health.Unknown("invalid-context")
	}
	if cause := context.Cause(ctx); cause != nil {
		return health.Unknown("check-cancelled")
	}
	status := cr.Status()
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
