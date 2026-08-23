package config

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrRuntimeAlreadyStarted reports a repeated Runtime Start.
	ErrRuntimeAlreadyStarted = errors.New(
		"config: runtime has already been started",
	)
	// ErrRuntimeNotStarted reports Wait before a successful Start.
	ErrRuntimeNotStarted = errors.New("config: runtime has not been started")
	// ErrRuntimeExited reports a Watch loop that ended without a cause.
	ErrRuntimeExited = errors.New("config: runtime watch exited unexpectedly")
)

var errRuntimeStop = errors.New("config: runtime stop requested")

// RuntimeState is the value-free lifecycle state of a configuration Runtime.
type RuntimeState string

const (
	// RuntimeStateNew means the runtime has not started.
	RuntimeStateNew RuntimeState = "new"
	// RuntimeStateStarting means source watchers are being established.
	RuntimeStateStarting RuntimeState = "starting"
	// RuntimeStateRunning means configuration is actively watched.
	RuntimeStateRunning RuntimeState = "running"
	// RuntimeStateStopping means shutdown is in progress.
	RuntimeStateStopping RuntimeState = "stopping"
	// RuntimeStateStopped means the runtime has exited.
	RuntimeStateStopped RuntimeState = "stopped"
)

// RuntimeDescription is a bounded configuration watcher diagnostic.
type RuntimeDescription struct {
	State   RuntimeState
	Running bool
	Failed  bool
}

// Runtime exposes a Manager Watch loop as a server-compatible App resource.
//
// Start returns only after every Source watcher is established and the initial
// merged snapshot is valid and published. Wait reports runtime watcher failure
// to App, while Stop performs an idempotent bounded shutdown.
type Runtime struct {
	manager *Manager

	mu      sync.Mutex
	state   RuntimeState
	cancel  context.CancelCauseFunc
	done    chan struct{}
	waitErr error
}

// NewRuntime creates a watch runtime without starting any goroutines.
func NewRuntime(manager *Manager) (*Runtime, error) {
	if manager == nil {
		return nil, fmt.Errorf("%w: manager is nil", ErrInvalidOption)
	}
	return &Runtime{
		manager: manager,
		state:   RuntimeStateNew,
		done:    make(chan struct{}),
	}, nil
}

// Name returns the stable App server name.
func (*Runtime) Name() string {
	return "keelith.config"
}

// Start establishes Source watchers and publishes a valid initial snapshot.
func (r *Runtime) Start(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	r.mu.Lock()
	if r.state != RuntimeStateNew {
		r.mu.Unlock()
		return ErrRuntimeAlreadyStarted
	}
	watchContext, cancel := context.WithCancelCause(
		context.WithoutCancel(ctx),
	)
	ready := make(chan error, 1)
	r.state = RuntimeStateStarting
	r.cancel = cancel
	r.mu.Unlock()

	go r.run(watchContext, ready)

	select {
	case err := <-ready:
		if err != nil {
			cancel(err)
			<-r.done
			return err
		}
	case <-ctx.Done():
		cause := context.Cause(ctx)
		cancel(cause)
		<-r.done
		return cause
	}

	r.mu.Lock()
	if r.state == RuntimeStateStopped {
		err := r.waitErr
		r.mu.Unlock()
		if err == nil {
			return ErrRuntimeExited
		}
		return err
	}
	r.state = RuntimeStateRunning
	r.mu.Unlock()
	return nil
}

// Stop cancels the Watch loop and waits for all Source watchers to close.
func (r *Runtime) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	r.mu.Lock()
	switch r.state {
	case RuntimeStateNew:
		r.mu.Unlock()
		return nil
	case RuntimeStateStarting, RuntimeStateRunning:
		r.state = RuntimeStateStopping
		cancel := r.cancel
		done := r.done
		r.mu.Unlock()
		cancel(errRuntimeStop)
		return waitRuntime(ctx, done, r.waitError)
	case RuntimeStateStopping:
		done := r.done
		r.mu.Unlock()
		return waitRuntime(ctx, done, r.waitError)
	case RuntimeStateStopped:
		r.mu.Unlock()
		return nil
	default:
		r.mu.Unlock()
		return nil
	}
}

// Wait blocks until the Watch loop ends.
func (r *Runtime) Wait() error {
	if r == nil {
		return ErrRuntimeNotStarted
	}
	r.mu.Lock()
	if r.state == RuntimeStateNew {
		r.mu.Unlock()
		return ErrRuntimeNotStarted
	}
	done := r.done
	r.mu.Unlock()
	<-done
	return r.waitError()
}

// Description returns a value-free lifecycle snapshot.
func (r *Runtime) Description() RuntimeDescription {
	if r == nil {
		return RuntimeDescription{
			State:  RuntimeStateStopped,
			Failed: true,
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return RuntimeDescription{
		State:   r.state,
		Running: r.state == RuntimeStateRunning,
		Failed:  r.state == RuntimeStateStopped && r.waitErr != nil,
	}
}

func (r *Runtime) run(
	ctx context.Context,
	ready chan<- error,
) {
	err := r.manager.watch(ctx, ready)
	r.mu.Lock()
	stopping := r.state == RuntimeStateStopping
	if stopping && errors.Is(err, errRuntimeStop) {
		err = nil
	}
	if err == nil && !stopping {
		err = ErrRuntimeExited
	}
	r.waitErr = err
	r.state = RuntimeStateStopped
	r.cancel = nil
	close(r.done)
	r.mu.Unlock()
}

func (r *Runtime) waitError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.waitErr
}

func waitRuntime(
	ctx context.Context,
	done <-chan struct{},
	result func() error,
) error {
	select {
	case <-done:
		return result()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
