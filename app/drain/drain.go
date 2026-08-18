// Package drain coordinates service deregistration before server shutdown.
package drain

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/keelab/keelith/app"
	"github.com/keelab/keelith/registry"
)

var (
	// ErrInvalidOption reports an incomplete drain dependency or policy.
	ErrInvalidOption = errors.New("drain: invalid option")
)

// Clock supplies time for drain diagnostics and propagation delay.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// After returns a channel that will receive a time after the specified duration.
	After(time.Duration) <-chan time.Time
}

// State is the observable drain phase.
type State string

const (
	// StateReady means no drain has started.
	StateReady State = "ready"
	// StateDeregistering means the registry call is in progress.
	StateDeregistering State = "deregistering"
	// StatePropagating means the manager is waiting for discovery convergence.
	StatePropagating State = "propagating"
	// StateDrained means deregistration and propagation completed.
	StateDrained State = "drained"
	// StateFailed means the one drain attempt failed.
	StateFailed State = "failed"
)

// Config defines one instance drain boundary.
type Config struct {
	Registrar       registry.Registrar // The registrar to use for deregistration.
	Instance        registry.Instance  // The instance to deregister.
	PropagationWait time.Duration      // The duration to wait for discovery convergence.
	Clock           Clock              // The clock to use for time-based operations.
}

// Description is an immutable diagnostic snapshot.
type Description struct {
	State     State     // The current state of the drain manager.
	StartedAt time.Time // The time the drain manager started.
	EndedAt   time.Time // The time the drain manager ended.
	LastError string    // The last error encountered during the drain manager's operation.
}

// Manager performs exactly one deregister-and-propagate sequence.
type Manager struct {
	registrar registry.Registrar // The registrar to use for deregistration.
	instance  registry.Instance  // The instance to deregister.
	wait      time.Duration      // The duration to wait for discovery convergence.
	clock     Clock              // The clock to use for time-based operations.

	mu          sync.Mutex
	state       State     // The current state of the drain manager.
	startedAt   time.Time // The time the drain manager started.
	endedAt     time.Time // The time the drain manager ended.
	err         error     // The last error encountered during the drain manager's operation.
	done        chan struct{}
	started     bool
	completeOne sync.Once
}

// New validates and constructs a Manager.
func New(config Config) (*Manager, error) {
	if isNil(config.Registrar) {
		return nil, fmt.Errorf("%w: registrar is nil", ErrInvalidOption)
	}
	if err := config.Instance.Validate(); err != nil {
		return nil, fmt.Errorf("%w: instance: %w", ErrInvalidOption, err)
	}
	if config.PropagationWait < 0 {
		return nil, fmt.Errorf(
			"%w: propagation wait is negative",
			ErrInvalidOption,
		)
	}
	clock := config.Clock
	if isNil(clock) {
		clock = systemClock{}
	}

	return &Manager{
		registrar: config.Registrar,
		instance:  config.Instance.Clone(),
		wait:      config.PropagationWait,
		clock:     clock,
		state:     StateReady,
		done:      make(chan struct{}),
	}, nil
}

// Hook returns the App hook that runs after readiness is revoked and before
// servers begin draining.
func (m *Manager) Hook() app.Hook {
	return app.Hook{BeforeStop: m.Drain}
}

// Drain deregisters once, waits for discovery propagation, and is safe for
// concurrent/repeated calls.
func (m *Manager) Drain(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	m.mu.Lock()
	if m.started {
		done := m.done
		m.mu.Unlock()
		return m.waitFor(ctx, done)
	}
	m.started = true
	m.state = StateDeregistering
	m.startedAt = m.clock.Now()
	m.mu.Unlock()

	err := m.run(ctx)
	m.complete(err)

	return err
}

// Describe returns the current drain phase without registry-specific data.
func (m *Manager) Describe() Description {
	if m == nil {
		return Description{State: StateFailed, LastError: "manager is nil"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	description := Description{
		State:     m.state,
		StartedAt: m.startedAt,
		EndedAt:   m.endedAt,
	}
	if m.err != nil {
		description.LastError = m.err.Error()
	}
	return description
}

func (m *Manager) run(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := m.registrar.Deregister(ctx, m.instance); err != nil {
		return fmt.Errorf("drain: deregister: %w", err)
	}
	if m.wait == 0 {
		return nil
	}
	m.mu.Lock()
	m.state = StatePropagating
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-m.clock.After(m.wait):
		return nil
	}
}

func (m *Manager) complete(err error) {
	m.completeOne.Do(func() {
		m.mu.Lock()
		m.err = err
		m.endedAt = m.clock.Now()
		if err == nil {
			m.state = StateDrained
		} else {
			m.state = StateFailed
		}
		close(m.done)
		m.mu.Unlock()
	})
}

func (m *Manager) waitFor(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.err
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

type systemClock struct{}

// Now returns the current time.
func (systemClock) Now() time.Time {
	return time.Now()
}

// After returns a channel that will receive a time after the specified duration.
func (systemClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}
