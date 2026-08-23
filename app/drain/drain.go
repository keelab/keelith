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

// ErrInvalidOption reports an incomplete drain dependency or policy.
var ErrInvalidOption = errors.New("drain: invalid option")

// Clock supplies time for drain diagnostics and propagation delay.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

// State is the observable drain phase.
type State string

const (
	// StateReady means no drain has started.
	StateReady State = "ready"
	// StateDeregistering means the registry call is in progress.
	StateDeregistering State = "deregistering"
	// StatePropagating means discovery convergence is waiting.
	StatePropagating State = "propagating"
	// StateDrained means deregistration and propagation completed.
	StateDrained State = "drained"
	// StateFailed means the drain attempt failed.
	StateFailed State = "failed"
)

// Config defines one instance drain boundary.
type Config struct {
	Registrar       registry.Registrar
	Instance        registry.Instance
	PropagationWait time.Duration
	Clock           Clock
}

// Description is an immutable diagnostic snapshot.
type Description struct {
	State     State
	StartedAt time.Time
	EndedAt   time.Time
	LastError string
}

// Manager performs exactly one deregister-and-propagate sequence.
type Manager struct {
	registrar registry.Registrar
	instance  registry.Instance
	wait      time.Duration
	clock     Clock

	mu          sync.Mutex
	state       State
	startedAt   time.Time
	endedAt     time.Time
	err         error
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
		return nil, fmt.Errorf("%w: propagation wait is negative", ErrInvalidOption)
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

// Hook runs after readiness is revoked and before servers begin draining.
func (m *Manager) Hook() app.Hook {
	return app.Hook{BeforeStop: m.Drain}
}

// Drain deregisters once, waits for discovery propagation, and is safe for
// concurrent or repeated calls.
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
		defer m.mu.Unlock()

		m.err = err
		m.endedAt = m.clock.Now()
		if err == nil {
			m.state = StateDrained
		} else {
			m.state = StateFailed
		}
		close(m.done)
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

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func (systemClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
