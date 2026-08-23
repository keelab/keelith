package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/keelab/keelith/server"
)

// Run starts the App and blocks until it stops.
//
// Root context cancellation is returned to the caller. A successful explicit
// Stop makes Run return nil.
func (a *App) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	runContext, cancel := context.WithCancelCause(ctx)
	if err := a.begin(cancel); err != nil {
		cancel(nil)
		return err
	}
	defer cancel(nil)

	started, activeHooks, primaryErr := a.start(runContext)
	if primaryErr == nil {
		a.transition(StateReady)
		primaryErr = a.wait(runContext, started)
	}

	if errors.Is(primaryErr, errStopRequested) {
		primaryErr = nil
	}

	a.transition(StateDraining)
	stopContext, stopCancel := context.WithTimeout(context.WithoutCancel(runContext), a.stopTimeout)
	stopErr := a.shutdown(stopContext, started, activeHooks)
	stopCancel()

	result := errors.Join(primaryErr, stopErr)
	a.complete(result, terminalState(runContext, primaryErr, stopErr))
	return result
}

// Stop requests graceful shutdown and waits for it to finish or for ctx to
// expire. Stop is safe to call concurrently and multiple times.
func (a *App) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	a.mu.Lock()
	switch a.state {
	case StateNew:
		a.mu.Unlock()
		return nil
	case StateStopped, StateFailed:
		err := a.runErr
		a.mu.Unlock()
		return err
	default:
		cancel := a.cancel
		done := a.done
		a.mu.Unlock()

		cancel(errStopRequested)
		select {
		case <-done:
			a.mu.Lock()
			err := a.runErr
			a.mu.Unlock()
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func (a *App) begin(cancel context.CancelCauseFunc) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != StateNew {
		return ErrAlreadyRun
	}
	a.state = StateStarting
	a.cancel = cancel
	if a.health != nil {
		a.health.Starting()
	}
	return nil
}

func (a *App) start(ctx context.Context) ([]server.Server, int, error) {
	started := make([]server.Server, 0, len(a.servers))
	activeHooks := 0

	for index, hook := range a.hooks {
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

	for _, srv := range a.servers {
		if err := context.Cause(ctx); err != nil {
			return started, activeHooks, err
		}
		if err := srv.Start(ctx); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return started, activeHooks, cause
			}
			return started, activeHooks, fmt.Errorf("app: start server %q: %w", serverName(srv), err)
		}
		started = append(started, srv)
	}

	for index := range activeHooks {
		if err := context.Cause(ctx); err != nil {
			return started, activeHooks, err
		}
		hook := a.hooks[index]
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

func (a *App) wait(ctx context.Context, started []server.Server) error {
	type waitResult struct {
		name string
		err  error
	}

	waiterCount := 0
	for _, srv := range started {
		if _, ok := srv.(server.Waiter); ok {
			waiterCount++
		}
	}

	results := make(chan waitResult, waiterCount)
	for _, srv := range started {
		waiter, ok := srv.(server.Waiter)
		if !ok {
			continue
		}
		name := serverName(srv)
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

func (a *App) shutdown(ctx context.Context, started []server.Server, activeHooks int) error {
	stopErrors := make([]error, 0)

	for index := activeHooks - 1; index >= 0; index-- {
		hook := a.hooks[index]
		if hook.BeforeStop == nil {
			continue
		}
		if err := hook.BeforeStop(ctx); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("app: hook %d before stop: %w", index, err))
		}
	}

	for index := len(started) - 1; index >= 0; index-- {
		srv := started[index]
		if err := srv.Stop(ctx); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("app: stop server %q: %w", serverName(srv), err))
		}
	}

	for index := activeHooks - 1; index >= 0; index-- {
		hook := a.hooks[index]
		if hook.AfterStop == nil {
			continue
		}
		if err := hook.AfterStop(ctx); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("app: hook %d after stop: %w", index, err))
		}
	}

	return errors.Join(stopErrors...)
}

func (a *App) transition(state State) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state = state
	if a.health == nil {
		return
	}

	switch state {
	case StateReady:
		a.health.Ready()
	case StateDraining:
		a.health.Draining()
	case StateStopped:
		a.health.Stopped()
	case StateFailed:
		a.health.Failed()
	case StateNew, StateStarting:
	}
}

func (a *App) complete(runErr error, terminal State) {
	a.mu.Lock()
	a.state = terminal
	a.runErr = runErr
	a.cancel = nil
	if a.health != nil {
		if terminal == StateFailed {
			a.health.Failed()
		} else {
			a.health.Stopped()
		}
	}
	a.closeOne.Do(func() {
		close(a.done)
	})
	a.mu.Unlock()
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

func serverName(srv server.Server) string {
	if named, ok := srv.(server.Named); ok && named.Name() != "" {
		return named.Name()
	}
	return fmt.Sprintf("%T", srv)
}
