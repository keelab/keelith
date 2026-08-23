// Package termination coordinates executable-owned two-stage shutdown.
//
// The package does not install process signal handlers and never calls
// os.Exit. Executables own both decisions: they pass a signal channel to Run
// and decide how ErrForcedShutdown terminates the process.
package termination

import (
	"context"
	"errors"
	"os"
)

var (
	// ErrInvalidOption reports a nil parent, signal channel, or Runner.
	ErrInvalidOption = errors.New("termination: invalid option")

	// ErrForcedShutdown reports a second signal after shutdown has begun.
	//
	// Run may return this error while Runner is still blocked. The executable
	// must terminate the process rather than continue normal execution.
	ErrForcedShutdown = errors.New("termination: forced shutdown requested")
)

// Runner owns one complete executable lifecycle, including construction and
// App.Run. It must propagate ctx to every blocking construction and runtime
// operation.
type Runner func(context.Context) error

// Run executes runner with a child of parent and coordinates two-stage
// termination.
//
// The first received signal cancels the Runner context. Parent cancellation
// has the same first-stage effect. A signal received after either event returns
// ErrForcedShutdown immediately. Run otherwise returns the Runner result.
func Run(parent context.Context, signals <-chan os.Signal, runner Runner) error {
	if parent == nil || signals == nil || runner == nil {
		return ErrInvalidOption
	}

	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)

	done := make(chan error, 1)
	go func() {
		done <- runner(ctx)
	}()

	terminating := context.Cause(ctx) != nil
	for {
		if terminating {
			select {
			case err := <-done:
				return err
			case _, open := <-signals:
				if !open {
					signals = nil
					continue
				}
				return ErrForcedShutdown
			}
		}

		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			terminating = true
		case _, open := <-signals:
			if !open {
				signals = nil
				continue
			}
			cancel(context.Canceled)
			terminating = true
		}
	}
}
