package continuation

import (
	"context"
	"errors"
)

// RetryableError asks the runtime to durably suspend this execution attempt.
type RetryableError struct {
	Cause error
}

// Error implements error.
func (err *RetryableError) Error() string {
	if err == nil || err.Cause == nil {
		return "continuation: retryable machine error"
	}
	return "continuation: retryable machine error: " + err.Cause.Error()
}

// Unwrap exposes the application cause.
func (err *RetryableError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// TerminalError asks the runtime to durably fail this call.
type TerminalError struct {
	Cause error
}

// Error implements error.
func (err *TerminalError) Error() string {
	if err == nil || err.Cause == nil {
		return "continuation: terminal machine error"
	}
	return "continuation: terminal machine error: " + err.Cause.Error()
}

// Unwrap exposes the application cause.
func (err *TerminalError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// Retryable marks a Machine error for automatic durable suspension.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Cause: err}
}

// Terminal marks a Machine error for automatic durable failure.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &TerminalError{Cause: err}
}

func machineErrorClass(err error) (ErrorClass, bool) {
	var retryable *RetryableError
	if errors.As(err, &retryable) {
		return ErrorClassRetryable, true
	}
	var terminal *TerminalError
	if errors.As(err, &terminal) {
		return ErrorClassTerminal, true
	}
	return ErrorClassInternal, false
}

func (r *Runtime) commitMachineError(
	ctx context.Context,
	current Snapshot,
	err error,
) (Snapshot, bool, error) {
	class, classified := machineErrorClass(err)
	r.observe(ctx, Event{
		Kind:       EventMachineError,
		Status:     current.Status(),
		ErrorClass: class,
	})
	if !classified {
		return Snapshot{}, false, nil
	}
	status := StatusSuspended
	frameKind := FrameSuspended
	if class == ErrorClassTerminal {
		status = StatusFailed
		frameKind = FrameFailed
	} else if current.Status() == StatusCancelRequested {
		status = StatusCancelRequested
	}
	frame, frameErr := NewFrame(frameKind, nil)
	if frameErr != nil {
		return Snapshot{}, true, frameErr
	}
	next, applyErr := Apply(
		current,
		Move(status, current.Fence(), frame),
	)
	if applyErr != nil {
		return Snapshot{}, true, applyErr
	}
	committed, commitErr := r.store.Transition(
		ctx,
		r.commitRequest(current, next),
	)
	if commitErr != nil {
		return Snapshot{}, true, commitErr
	}
	r.observe(ctx, Event{
		Kind:   EventTransition,
		Status: committed.Status(),
	})
	return committed, true, nil
}
