// Package worker provides broker- and scheduler-neutral background runtimes.
package worker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

const forcedShutdownTimeout = 100 * time.Millisecond

var (
	// ErrInvalidOption reports an invalid Worker dependency or operation.
	ErrInvalidOption = errors.New("worker: invalid option")
	// ErrAlreadyStarted reports a second Start call.
	ErrAlreadyStarted = errors.New("worker: already started")
	// ErrNilContext reports a nil lifecycle or delivery context.
	ErrNilContext = errors.New("worker: nil context")
	// ErrNotAccepting reports a delivery after draining began.
	ErrNotAccepting = errors.New("worker: not accepting work")
	// ErrNotStarted reports Wait before Start.
	ErrNotStarted = errors.New("worker: not started")
)

type workerState uint8

const (
	workerStateNew workerState = iota
	workerStateStarting
	workerStateRunning
	workerStateStopping
	workerStateStopped
)

type runtimeSource struct {
	start       func(context.Context, middleware.Handler) error
	stopPulling func(context.Context) error
	drain       func(context.Context) error
	close       func(context.Context) error
	wait        func() error
}

// Worker is a Server-compatible owner of one Consumer or Scheduler.
type Worker struct {
	name      string
	target    operation.Operation
	source    runtimeSource
	invoke    middleware.Handler
	inflight  *inflightGroup
	readiness *health.Registry

	mu            sync.Mutex
	state         workerState
	runtimeCtx    context.Context
	runtimeCancel context.CancelCauseFunc
	startErr      error
	stopErr       error

	startDone chan struct{}
	stopDone  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
}

func newWorker(name string, target operation.Operation, wantKind operation.Kind, source runtimeSource, final middleware.Handler, bundle *middleware.Bundle, registry *health.Registry) (*Worker, error) {
	normalizedName := strings.TrimSpace(name)
	if normalizedName == "" {
		return nil, invalidOption("name is empty")
	}
	if target.Transport() == "" || target.Kind() != wantKind {
		return nil, invalidOption(fmt.Sprintf("operation kind is %q, want %q", target.Kind(), wantKind))
	}
	if final == nil || source.start == nil || source.stopPulling == nil || source.drain == nil || source.close == nil || source.wait == nil {
		return nil, invalidOption("runtime source is incomplete")
	}
	invoke := final
	if bundle != nil {
		invoke = bundle.Chain()(final)
	}
	result := &Worker{
		name:      normalizedName,
		target:    target,
		source:    source,
		invoke:    invoke,
		inflight:  newInflightGroup(),
		readiness: registry,
		state:     workerStateNew,
		startDone: make(chan struct{}),
		stopDone:  make(chan struct{}),
	}
	if registry != nil {
		if err := registry.Register(health.KindReadiness, normalizedName, result.healthCheck); err != nil {
			return nil, fmt.Errorf("%w: register health: %w", ErrInvalidOption, err)
		}
	}
	return result, nil
}

// Name returns the stable diagnostic and health contributor name.
func (w *Worker) Name() string {
	return w.name
}

// Ready reports whether subscription or scheduling is accepting work.
func (w *Worker) Ready() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state == workerStateRunning
}

// Start subscribes or schedules and returns only after the source is ready.
func (w *Worker) Start(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("%w: worker is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return ErrNilContext
	}
	w.mu.Lock()
	if w.state != workerStateNew {
		w.mu.Unlock()
		return ErrAlreadyStarted
	}
	w.state = workerStateStarting
	w.runtimeCtx, w.runtimeCancel = context.WithCancelCause(
		context.WithoutCancel(ctx),
	)
	w.mu.Unlock()
	defer w.startOnce.Do(func() { close(w.startDone) })

	w.inflight.open()
	err := w.source.start(ctx, w.dispatch)
	if err == nil {
		err = context.Cause(ctx)
	}
	if err != nil {
		cleanupErr := w.rollback(err)
		result := errors.Join(err, cleanupErr)
		w.mu.Lock()
		w.startErr = result
		w.state = workerStateStopped
		w.mu.Unlock()
		w.stopOnce.Do(func() { close(w.stopDone) })
		return result
	}

	w.mu.Lock()
	w.state = workerStateRunning
	w.mu.Unlock()
	return nil
}

// Stop stops new work, drains handlers and adapter commits, then closes the
// source. Stop is safe to call repeatedly and concurrently.
func (w *Worker) Stop(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		return ErrNilContext
	}

	for {
		w.mu.Lock()
		switch w.state {
		case workerStateNew:
			w.state = workerStateStopped
			w.startOnce.Do(func() { close(w.startDone) })
			w.stopOnce.Do(func() { close(w.stopDone) })
			w.mu.Unlock()
			return nil
		case workerStateStarting:
			startDone := w.startDone
			w.mu.Unlock()
			select {
			case <-startDone:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case workerStateRunning:
			w.state = workerStateStopping
			w.mu.Unlock()
			err := w.shutdown(ctx)
			w.complete(err)
			return err
		case workerStateStopping:
			stopDone := w.stopDone
			w.mu.Unlock()
			return w.waitStop(ctx, stopDone)
		case workerStateStopped:
			err := errors.Join(w.startErr, w.stopErr)
			w.mu.Unlock()
			return err
		default:
			w.mu.Unlock()
			return fmt.Errorf("%w: unknown state", ErrInvalidOption)
		}
	}
}

// Wait reports source termination and returns after Close during normal Stop.
func (w *Worker) Wait() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	state := w.state
	startErr := w.startErr
	w.mu.Unlock()
	switch state {
	case workerStateNew, workerStateStarting:
		return ErrNotStarted
	case workerStateStopped:
		w.mu.Lock()
		stopErr := w.stopErr
		w.mu.Unlock()
		return errors.Join(startErr, stopErr)
	}

	return w.source.wait()
}

func (w *Worker) dispatch(ctx context.Context, request any) (any, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if !w.inflight.begin() {
		return nil, ErrNotAccepting
	}
	defer w.inflight.done()

	w.mu.Lock()
	runtimeCtx := w.runtimeCtx
	w.mu.Unlock()
	if runtimeCtx == nil {
		return nil, ErrNotAccepting
	}
	invocationCtx, cancel := mergeContexts(ctx, runtimeCtx)
	defer cancel()
	requestInfo, err := operation.NewRequestInfo(w.target)
	if err != nil {
		return nil, fmt.Errorf("%w: request info: %w", ErrInvalidOption, err)
	}
	invocationCtx = operation.WithRequestInfo(invocationCtx, requestInfo)

	return w.invoke(invocationCtx, request)
}

func (w *Worker) shutdown(ctx context.Context) error {
	stopPullingErr := w.source.stopPulling(ctx)
	drainHandlersErr := w.inflight.stopAndWait(ctx)
	if drainHandlersErr != nil {
		w.cancelRuntime(drainHandlersErr)
		forced, cancel := context.WithTimeout(context.Background(), forcedShutdownTimeout)
		drainHandlersErr = errors.Join(drainHandlersErr, w.inflight.stopAndWait(forced))
		cancel()
	}
	w.cancelRuntime(context.Canceled)

	cleanupCtx, cleanupCancel := cleanupContext(ctx)
	defer cleanupCancel()
	adapterDrainErr := w.source.drain(cleanupCtx)
	closeErr := w.source.close(cleanupCtx)

	return errors.Join(stopPullingErr, drainHandlersErr, adapterDrainErr, closeErr)
}

func (w *Worker) rollback(primary error) error {
	w.cancelRuntime(primary)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), forcedShutdownTimeout)
	defer cancel()

	return errors.Join(w.source.stopPulling(cleanupCtx), w.inflight.stopAndWait(cleanupCtx), w.source.drain(cleanupCtx), w.source.close(cleanupCtx))
}

func (w *Worker) complete(err error) {
	w.mu.Lock()
	w.stopErr = err
	w.state = workerStateStopped
	w.mu.Unlock()
	w.stopOnce.Do(func() { close(w.stopDone) })
}

func (w *Worker) waitStop(
	ctx context.Context,
	stopDone <-chan struct{},
) error {
	select {
	case <-stopDone:
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.stopErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (w *Worker) cancelRuntime(cause error) {
	w.mu.Lock()
	cancel := w.runtimeCancel
	w.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

func (w *Worker) healthCheck(context.Context) health.Result {
	if w.Ready() {
		return health.Pass("worker subscription is accepting work")
	}
	return health.Fail("worker subscription is not accepting work")
}

func mergeContexts(message context.Context, runtime context.Context) (context.Context, func()) {
	merged, cancel := context.WithCancelCause(message)

	stop := context.AfterFunc(runtime, func() {
		cancel(context.Cause(runtime))
	})

	return merged, func() {
		stop()
		cancel(nil)
	}
}

func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if context.Cause(ctx) == nil {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(context.Background(), forcedShutdownTimeout)
}

func invalidOption(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidOption, message)
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
