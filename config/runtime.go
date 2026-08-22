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
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	runtime.mu.Lock()
	if runtime.state != RuntimeStateNew {
		runtime.mu.Unlock()
		return ErrRuntimeAlreadyStarted
	}
	watchContext, cancel := context.WithCancelCause(
		context.WithoutCancel(ctx),
	)
	ready := make(chan error, 1)
	runtime.state = RuntimeStateStarting
	runtime.cancel = cancel
	runtime.mu.Unlock()

	go runtime.run(watchContext, ready)

	select {
	case err := <-ready:
		if err != nil {
			cancel(err)
			<-runtime.done
			return err
		}
	case <-ctx.Done():
		cause := context.Cause(ctx)
		cancel(cause)
		<-runtime.done
		return cause
	}

	runtime.mu.Lock()
	if runtime.state == RuntimeStateStopped {
		err := runtime.waitErr
		runtime.mu.Unlock()
		if err == nil {
			return ErrRuntimeExited
		}
		return err
	}
	runtime.state = RuntimeStateRunning
	runtime.mu.Unlock()
	return nil
}

// Stop cancels the Watch loop and waits for all Source watchers to close.
func (runtime *Runtime) Stop(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	runtime.mu.Lock()
	switch runtime.state {
	case RuntimeStateNew:
		runtime.mu.Unlock()
		return nil
	case RuntimeStateStarting, RuntimeStateRunning:
		runtime.state = RuntimeStateStopping
		cancel := runtime.cancel
		done := runtime.done
		runtime.mu.Unlock()
		cancel(errRuntimeStop)
		return waitRuntime(ctx, done, runtime.waitError)
	case RuntimeStateStopping:
		done := runtime.done
		runtime.mu.Unlock()
		return waitRuntime(ctx, done, runtime.waitError)
	case RuntimeStateStopped:
		runtime.mu.Unlock()
		return nil
	default:
		runtime.mu.Unlock()
		return nil
	}
}

// Wait blocks until the Watch loop ends.
func (runtime *Runtime) Wait() error {
	if runtime == nil {
		return ErrRuntimeNotStarted
	}
	runtime.mu.Lock()
	if runtime.state == RuntimeStateNew {
		runtime.mu.Unlock()
		return ErrRuntimeNotStarted
	}
	done := runtime.done
	runtime.mu.Unlock()
	<-done
	return runtime.waitError()
}

// Description returns a value-free lifecycle snapshot.
func (runtime *Runtime) Description() RuntimeDescription {
	if runtime == nil {
		return RuntimeDescription{
			State:  RuntimeStateStopped,
			Failed: true,
		}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return RuntimeDescription{
		State:   runtime.state,
		Running: runtime.state == RuntimeStateRunning,
		Failed:  runtime.state == RuntimeStateStopped && runtime.waitErr != nil,
	}
}

func (runtime *Runtime) run(
	ctx context.Context,
	ready chan<- error,
) {
	err := runtime.manager.watch(ctx, ready)
	runtime.mu.Lock()
	stopping := runtime.state == RuntimeStateStopping
	if stopping && errors.Is(err, errRuntimeStop) {
		err = nil
	}
	if err == nil && !stopping {
		err = ErrRuntimeExited
	}
	runtime.waitErr = err
	runtime.state = RuntimeStateStopped
	runtime.cancel = nil
	close(runtime.done)
	runtime.mu.Unlock()
}

func (runtime *Runtime) waitError() error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.waitErr
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
