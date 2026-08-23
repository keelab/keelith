package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/server"
	"github.com/keelab/keelith/service"
)

var (
	// ErrAlreadyRun is returned when Run is called more than once.
	ErrAlreadyRun = errors.New("app: app has already been run")
	// ErrInvalidOption is returned when an Option cannot produce a valid App.
	ErrInvalidOption = errors.New("app: invalid option")
	// ErrNilContext is returned when Run or Stop receives a nil context.
	ErrNilContext = errors.New("app: nil context")
	// ErrServerExited is returned when a Server Waiter exits without an error
	// while the App is still running.
	ErrServerExited = errors.New("app: server exited while app was running")

	// errStopRequested is returned when the App is requested to stop.
	errStopRequested = errors.New("app: stop requested")
)

// App coordinates hooks and servers without process-wide mutable state.
//
// An App can be run once. Multiple App instances can run independently in the
// same process.
type App struct {
	servers     []server.Server
	hooks       []Hook
	health      *health.Registry
	identity    *service.Identity
	stopTimeout time.Duration

	mu       sync.Mutex
	state    State
	cancel   context.CancelCauseFunc
	done     chan struct{}
	runErr   error
	closeOne sync.Once
}

// New constructs an App and validates every Option.
func New(opts ...Option) (*App, error) {
	s := defaultSettings()

	for index, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := opt.apply(&s); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}

	components, err := sortComponents(s.components)
	if err != nil {
		return nil, fmt.Errorf("%w: components: %w", ErrInvalidOption, err)
	}
	for _, component := range components {
		c := component
		s.hooks = append(s.hooks, Hook{
			BeforeStart: c.Start,
			AfterStop:   c.Stop,
		})
	}

	return &App{
		servers:     append([]server.Server(nil), s.servers...),
		hooks:       append([]Hook(nil), s.hooks...),
		health:      s.health,
		identity:    s.identity,
		stopTimeout: s.stopTimeout,
		state:       StateNew,
		done:        make(chan struct{}),
	}, nil
}

// Identity returns the immutable identity assigned to this App.
func (a *App) Identity() (service.Identity, bool) {
	if a == nil || a.identity == nil {
		return service.Identity{}, false
	}
	return *a.identity, true
}

// State returns the App's current lifecycle state.
func (a *App) State() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

// Description returns a bounded lifecycle snapshot suitable for diagnostics.
func (a *App) Description() Description {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Description{
		State:    a.state,
		Terminal: a.state == StateStopped || a.state == StateFailed,
		Failed:   a.state == StateFailed,
	}
}

// Err returns the terminal Run result after the App reaches Stopped or Failed.
//
// Err returns nil while the App is non-terminal. Callers that need to wait for
// completion should use Run or Stop instead of polling Err.
func (a *App) Err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != StateStopped && a.state != StateFailed {
		return nil
	}
	return a.runErr
}
