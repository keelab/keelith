// Package app coordinates the explicit lifecycle of a Keelith application.
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
func New(optionList ...Option) (*App, error) {
	settings := defaultOptions()

	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	components, err := sortComponents(settings.components)
	if err != nil {
		return nil, fmt.Errorf("%w: components: %w", ErrInvalidOption, err)
	}
	for _, component := range components {
		resource := component
		settings.hooks = append(settings.hooks, Hook{
			BeforeStart: resource.Start,
			AfterStop:   resource.Stop,
		})
	}

	return &App{
		servers:     append([]server.Server(nil), settings.servers...),
		hooks:       append([]Hook(nil), settings.hooks...),
		health:      settings.health,
		identity:    settings.identity,
		stopTimeout: settings.stopTimeout,
		state:       StateNew,
		done:        make(chan struct{}),
	}, nil
}

// Identity returns the immutable identity assigned to this App.
func (app *App) Identity() (service.Identity, bool) {
	if app == nil || app.identity == nil {
		return service.Identity{}, false
	}
	return *app.identity, true
}

// Run starts the App and blocks until it stops.
//
// Root context cancellation is returned to the caller. A successful explicit
// Stop makes Run return nil.
func (app *App) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	runContext, cancel := context.WithCancelCause(ctx)
	if err := app.begin(cancel); err != nil {
		cancel(nil)
		return err
	}
	defer cancel(nil)

	started, activeHooks, primaryErr := app.start(runContext)
	if primaryErr == nil {
		app.transition(StateReady)
		primaryErr = app.wait(runContext, started)
	}

	if errors.Is(primaryErr, errStopRequested) {
		primaryErr = nil
	}

	app.transition(StateDraining)
	stopContext, stopCancel := context.WithTimeout(context.WithoutCancel(runContext), app.stopTimeout)
	stopErr := app.shutdown(stopContext, started, activeHooks)
	stopCancel()

	result := errors.Join(primaryErr, stopErr)
	app.complete(result, terminalState(runContext, primaryErr, stopErr))
	return result
}

// Stop requests graceful shutdown and waits for it to finish or for ctx to
// expire. Stop is safe to call concurrently and multiple times.
func (app *App) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	app.mu.Lock()
	switch app.state {
	case StateNew:
		app.mu.Unlock()
		return nil
	case StateStopped, StateFailed:
		err := app.runErr
		app.mu.Unlock()
		return err
	default:
		cancel := app.cancel
		done := app.done
		app.mu.Unlock()

		cancel(errStopRequested)
		select {
		case <-done:
			app.mu.Lock()
			err := app.runErr
			app.mu.Unlock()
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// State returns the App's current lifecycle state.
func (app *App) State() State {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.state
}

// Description returns a bounded lifecycle snapshot suitable for diagnostics.
func (app *App) Description() Description {
	app.mu.Lock()
	defer app.mu.Unlock()
	return Description{
		State:    app.state,
		Terminal: app.state == StateStopped || app.state == StateFailed,
		Failed:   app.state == StateFailed,
	}
}

// Err returns the terminal Run result after the App reaches Stopped or Failed.
//
// Err returns nil while the App is non-terminal. Callers that need to wait for
// completion should use Run or Stop instead of polling Err.
func (app *App) Err() error {
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.state != StateStopped && app.state != StateFailed {
		return nil
	}
	return app.runErr
}

func (app *App) begin(cancel context.CancelCauseFunc) error {
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.state != StateNew {
		return ErrAlreadyRun
	}
	app.state = StateStarting
	app.cancel = cancel
	if app.health != nil {
		app.health.Starting()
	}
	return nil
}

func (app *App) start(ctx context.Context) ([]server.Server, int, error) {
	started := make([]server.Server, 0, len(app.servers))
	activeHooks := 0

	for index, hook := range app.hooks {
		if err := context.Cause(ctx); err != nil {
			return started, activeHooks, err
		}
		if hook.BeforeStart != nil {
			if err := hook.BeforeStart(ctx); err != nil {
				if cause := context.Cause(ctx); cause != nil {
					return started, activeHooks, cause
				}
				return started, activeHooks, fmt.Errorf("app: hook %d before start: %w", index, err)
			}
		}
		activeHooks++
	}

	for _, component := range app.servers {
		if err := context.Cause(ctx); err != nil {
			return started, activeHooks, err
		}
		if err := component.Start(ctx); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return started, activeHooks, cause
			}
			return started, activeHooks, fmt.Errorf("app: start server %q: %w", serverName(component), err)
		}
		started = append(started, component)
	}

	for index := range activeHooks {
		if err := context.Cause(ctx); err != nil {
			return started, activeHooks, err
		}
		hook := app.hooks[index]
		if hook.AfterStart != nil {
			if err := hook.AfterStart(ctx); err != nil {
				if cause := context.Cause(ctx); cause != nil {
					return started, activeHooks, cause
				}

				return started, activeHooks, fmt.Errorf("app: hook %d after start: %w", index, err)
			}
		}
	}

	if err := context.Cause(ctx); err != nil {
		return started, activeHooks, err
	}
	return started, activeHooks, nil
}

func (app *App) wait(ctx context.Context, started []server.Server) error {
	type waitResult struct {
		name string
		err  error
	}

	waiterCount := 0
	for _, component := range started {
		if _, ok := component.(server.Waiter); ok {
			waiterCount++
		}
	}

	results := make(chan waitResult, waiterCount)
	for _, component := range started {
		waiter, ok := component.(server.Waiter)
		if !ok {
			continue
		}
		name := serverName(component)
		go func() {
			results <- waitResult{name: name, err: waiter.Wait()}
		}()
	}

	if waiterCount == 0 {
		<-ctx.Done()
		return context.Cause(ctx)
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case result := <-results:
		if result.err == nil {
			return fmt.Errorf("%w: %s", ErrServerExited, result.name)
		}
		return fmt.Errorf("app: server %q runtime failure: %w", result.name, result.err)
	}
}

func (app *App) shutdown(ctx context.Context, started []server.Server, activeHooks int) error {
	stopErrors := make([]error, 0)

	for index := activeHooks - 1; index >= 0; index-- {
		hook := app.hooks[index]
		if hook.BeforeStop == nil {
			continue
		}
		if err := hook.BeforeStop(ctx); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("app: hook %d before stop: %w", index, err))
		}
	}

	for index := len(started) - 1; index >= 0; index-- {
		component := started[index]
		if err := component.Stop(ctx); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("app: stop server %q: %w", serverName(component), err))
		}
	}

	for index := activeHooks - 1; index >= 0; index-- {
		hook := app.hooks[index]
		if hook.AfterStop == nil {
			continue
		}
		if err := hook.AfterStop(ctx); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("app: hook %d after stop: %w", index, err))
		}
	}

	return errors.Join(stopErrors...)
}

func (app *App) transition(state State) {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.state = state
	if app.health == nil {
		return
	}
	switch state {
	case StateReady:
		app.health.Ready()
	case StateDraining:
		app.health.Draining()
	case StateStopped:
		app.health.Stopped()
	case StateFailed:
		app.health.Failed()
	case StateNew, StateStarting:
	}
}

func (app *App) complete(runErr error, terminal State) {
	app.mu.Lock()
	app.state = terminal
	app.runErr = runErr
	app.cancel = nil
	if app.health != nil {
		if terminal == StateFailed {
			app.health.Failed()
		} else {
			app.health.Stopped()
		}
	}
	app.closeOne.Do(func() {
		close(app.done)
	})
	app.mu.Unlock()
}

func terminalState(runContext context.Context, primaryErr error, stopErr error) State {
	if stopErr != nil {
		return StateFailed
	}
	if primaryErr == nil || expectedContextTermination(runContext, primaryErr) {
		return StateStopped
	}
	return StateFailed
}

func expectedContextTermination(runContext context.Context, primaryErr error) bool {
	cause := context.Cause(runContext)
	if !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
		return false
	}
	return errors.Is(primaryErr, cause)
}

func serverName(component server.Server) string {
	if named, ok := component.(server.Named); ok && named.Name() != "" {
		return named.Name()
	}
	return fmt.Sprintf("%T", component)
}
