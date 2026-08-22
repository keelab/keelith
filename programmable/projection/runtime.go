package projection

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/server"
)

const defaultReconnectDelay = 100 * time.Millisecond

var (
	// ErrInvalidRuntime reports incomplete projection synchronization wiring.
	ErrInvalidRuntime = errors.New("projection: invalid runtime")
	// ErrRuntimeNotStarted reports Wait before Start.
	ErrRuntimeNotStarted = errors.New("projection: runtime not started")
)

var (
	_ server.Server = (*Runtime)(nil)
	_ server.Waiter = (*Runtime)(nil)
	_ server.Named  = (*Runtime)(nil)
)

// RuntimeConfig wires one resumable projection synchronization loop.
type RuntimeConfig struct {
	Name     string
	Schema   Schema
	Store    Store
	Source   Source
	Notifier CheckpointNotifier
	Backoff  Backoff
	Observer Observer
	Clock    func() time.Time
}

// Runtime bootstraps, resumes, and rebuilds one projection replica.
type Runtime struct {
	config RuntimeConfig

	mu            sync.Mutex
	started       bool
	stopRequested bool
	finished      bool
	cancel        context.CancelCauseFunc
	active        Session
	activeToken   uint64
	runErr        error
	done          chan struct{}
}

// NewRuntime validates and constructs a stopped Runtime.
func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	if strings.TrimSpace(config.Name) == "" ||
		config.Name != strings.TrimSpace(config.Name) ||
		isNilInterface(config.Store) ||
		isNilInterface(config.Source) ||
		isNilObserver(config.Observer) {
		return nil, fmt.Errorf("%w: name, store, or source", ErrInvalidRuntime)
	}
	if err := config.Schema.Validate(); err != nil {
		return nil, err
	}
	if !config.Backoff.configured {
		config.Backoff = defaultBackoff(deterministicBackoffSeed(config.Name + "\x00" + string(config.Schema.ID)))
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &Runtime{
		config: config,
		done:   make(chan struct{}),
	}, nil
}

// Name returns the stable App server identity.
func (r *Runtime) Name() string {
	if r == nil {
		return ""
	}
	return r.config.Name
}

// Start launches synchronization and waits until existing state is connected
// or an initial snapshot is atomically committed.
func (r *Runtime) Start(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidRuntime)
	}
	if err := validateRuntimeContext(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return fmt.Errorf("%w: already started", ErrInvalidRuntime)
	}
	runCtx, cancel := context.WithCancelCause(context.Background())
	r.cancel = cancel
	r.started = true
	ready := make(chan error, 1)
	r.mu.Unlock()

	go r.run(runCtx, ready)
	select {
	case err := <-ready:
		return err
	case <-ctx.Done():
		cancel(context.Cause(ctx))
		r.closeActive()
		return context.Cause(ctx)
	}
}

// Stop interrupts the active session and waits for the synchronization loop.
func (r *Runtime) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if err := validateRuntimeContext(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return nil
	}
	r.stopRequested = true
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel(context.Canceled)
	}
	r.closeActive()
	select {
	case <-r.done:
		return r.waitError()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Wait reports terminal protocol or Store failures.
func (r *Runtime) Wait() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return ErrRuntimeNotStarted
	}
	<-r.done
	return r.waitError()
}

func (r *Runtime) run(ctx context.Context, ready chan<- error) {
	var readyOnce sync.Once
	signalReady := func(err error) {
		readyOnce.Do(func() {
			ready <- err
		})
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err := fmt.Errorf("projection: runtime panic")
			r.setRunError(err)
			signalReady(err)
		}
		r.mu.Lock()
		r.finished = true
		r.active = nil
		r.mu.Unlock()
		close(r.done)
	}()

	forceSnapshot := false
	reconnectAttempt := uint32(0)
	for {
		if context.Cause(ctx) != nil {
			signalReady(context.Cause(ctx))
			return
		}
		checkpoint, exists, err := r.config.Store.Checkpoint(
			ctx,
			r.config.Schema.ID,
		)
		if err != nil {
			r.observe(ctx, Event{
				Kind:       EventError,
				ErrorClass: ErrorStore,
				Attempt:    reconnectAttempt,
			})
			r.fail(err, signalReady)
			return
		}
		if !exists {
			forceSnapshot = true
		}
		request := SubscribeRequest{
			Schema:        r.config.Schema,
			ForceSnapshot: forceSnapshot,
		}
		if exists && !forceSnapshot {
			request.After = checkpoint.Cursor
		}
		session, err := r.config.Source.Open(ctx, request)
		if err != nil {
			if context.Cause(ctx) != nil {
				signalReady(context.Cause(ctx))
				return
			}
			r.observe(ctx, Event{
				Kind:       EventError,
				ErrorClass: ErrorSource,
				Attempt:    reconnectAttempt,
			})
			if !r.waitReconnect(ctx, reconnectAttempt) {
				signalReady(context.Cause(ctx))
				return
			}
			reconnectAttempt = nextReconnectAttempt(reconnectAttempt)
			continue
		}
		if isNilInterface(session) {
			r.fail(
				fmt.Errorf("%w: source returned nil session", ErrInvalidRuntime),
				signalReady,
			)
			return
		}
		activeToken := r.setActive(session)
		r.observe(ctx, Event{
			Kind:    EventConnect,
			Attempt: reconnectAttempt,
		})
		if exists && !forceSnapshot {
			signalReady(nil)
		}
		snapshotCommitted := false
		gap, progressed, consumeErr := r.consume(
			ctx,
			session,
			signalReady,
			&snapshotCommitted,
		)
		r.clearActive(activeToken)
		_ = session.Close()
		if context.Cause(ctx) != nil {
			signalReady(context.Cause(ctx))
			return
		}
		if snapshotCommitted {
			forceSnapshot = false
		}
		if progressed {
			reconnectAttempt = 0
		}
		if gap {
			forceSnapshot = true
			continue
		}
		if errors.Is(consumeErr, ErrCursorMismatch) ||
			errors.Is(consumeErr, ErrProjectionNotFound) ||
			errors.Is(consumeErr, ErrSnapshotConflict) {
			forceSnapshot = true
		}
		if consumeErr != nil && isFatalRuntimeError(consumeErr) {
			r.observe(ctx, Event{
				Kind:       EventError,
				ErrorClass: errorClassOf(consumeErr),
				Attempt:    reconnectAttempt,
			})
			r.fail(consumeErr, signalReady)
			return
		}
		if consumeErr != nil {
			r.observe(ctx, Event{
				Kind:       EventError,
				ErrorClass: errorClassOf(consumeErr),
				Attempt:    reconnectAttempt,
			})
		}
		if !r.waitReconnect(ctx, reconnectAttempt) {
			signalReady(context.Cause(ctx))
			return
		}
		reconnectAttempt = nextReconnectAttempt(reconnectAttempt)
	}
}

func (r *Runtime) consume(ctx context.Context, session Session, signalReady func(error), snapshotCommitted *bool) (gap bool, progressed bool, resultErr error) {
	var snapshot SnapshotTxn
	defer func() {
		if snapshot != nil {
			_ = snapshot.Abort()
		}
	}()
	for {
		frame, err := session.Next(ctx)
		if err != nil {
			return false, progressed, classifiedError(ErrorSource, err)
		}
		if frame == nil {
			return false, progressed, classifiedError(ErrorProtocol, fmt.Errorf("%w: nil frame", ErrInvalidFrame))
		}
		switch current := frame.cloneFrame().(type) {
		case SnapshotBeginFrame:
			if snapshot != nil {
				return false, progressed, classifiedError(ErrorProtocol, fmt.Errorf("%w: nested snapshot begin", ErrInvalidFrame))
			}
			if err := requireMatchingSchema(
				r.config.Schema,
				current.Schema,
			); err != nil {
				return false, progressed, classifiedError(ErrorProtocol, err)
			}
			snapshot, err = r.config.Store.BeginSnapshot(ctx, current.Schema)
			if err != nil {
				return false, progressed, classifiedError(ErrorStore, err)
			}
			progressed = true
		case SnapshotChunkFrame:
			if snapshot == nil || len(current.Mutations) == 0 {
				return false, progressed, classifiedError(ErrorProtocol, fmt.Errorf("%w: snapshot chunk outside snapshot", ErrInvalidFrame))
			}
			for _, mutation := range current.Mutations {
				if err := snapshot.Stage(mutation); err != nil {
					return false, progressed, classifiedError(ErrorStore, err)
				}
			}
			progressed = true
		case SnapshotEndFrame:
			if snapshot == nil {
				return false, progressed, classifiedError(
					ErrorProtocol,
					fmt.Errorf("%w: snapshot end outside snapshot", ErrInvalidFrame),
				)
			}
			if err := snapshot.Commit(ctx, current.Cursor, current.SourceTime); err != nil {
				return false, progressed, classifiedError(ErrorStore, err)
			}
			snapshot = nil
			*snapshotCommitted = true
			r.notify(ctx)
			r.observe(ctx, Event{Kind: EventSnapshot})
			r.observeLag(ctx, current.SourceTime)
			signalReady(nil)
			progressed = true
		case DeltaFrame:
			if snapshot != nil {
				return false, progressed, classifiedError(
					ErrorProtocol,
					fmt.Errorf("%w: delta inside snapshot",
						ErrInvalidFrame))
			}
			if err := r.config.Store.ApplyDelta(
				ctx,
				current.Batch,
			); err != nil {
				return false, progressed, classifiedError(ErrorStore, err)
			}
			r.notify(ctx)
			r.observe(ctx, Event{Kind: EventDelta})
			r.observeLag(ctx, current.Batch.SourceTime)
			progressed = true
		case HeartbeatFrame:
			if err := current.Cursor.Validate(); err != nil ||
				current.SourceTime.IsZero() {
				return false, progressed, classifiedError(ErrorProtocol, fmt.Errorf("%w: malformed heartbeat", ErrInvalidFrame))
			}
			r.observeLag(ctx, current.SourceTime)
			progressed = true
		case GapFrame:
			if err := current.Requested.Validate(); err != nil {
				return false, progressed, classifiedError(ErrorProtocol, fmt.Errorf("%w: malformed gap request", ErrInvalidFrame))
			}
			if err := current.Floor.Validate(); err != nil {
				return false, progressed, classifiedError(ErrorProtocol, fmt.Errorf("%w: malformed gap floor", ErrInvalidFrame))
			}
			r.observe(ctx, Event{Kind: EventGap})
			return true, true, nil
		default:
			return false, progressed, classifiedError(ErrorProtocol, fmt.Errorf("%w: unsupported frame %T", ErrInvalidFrame, frame))
		}
	}
}

func (r *Runtime) notify(ctx context.Context) {
	if r.config.Notifier == nil {
		return
	}
	checkpoint, exists, err := r.config.Store.Checkpoint(
		ctx,
		r.config.Schema.ID,
	)
	if err != nil {
		r.observe(ctx, Event{
			Kind:       EventError,
			ErrorClass: ErrorStore,
		})
		return
	}
	if exists {
		r.config.Notifier.NotifyCheckpoint(checkpoint)
	}
}

func (r *Runtime) waitReconnect(ctx context.Context, attempt uint32) bool {
	delay := r.config.Backoff.Delay(attempt)
	r.observe(ctx, Event{
		Kind:    EventReconnect,
		Attempt: attempt,
		Delay:   delay,
	})
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Runtime) observe(ctx context.Context, event Event) {
	event.Projection = r.config.Schema.ID
	observeProjection(ctx, r.config.Observer, event)
}

func (r *Runtime) observeLag(ctx context.Context, sourceTime time.Time) {
	lag := max(r.config.Clock().UTC().Sub(sourceTime), 0)
	r.observe(ctx, Event{
		Kind: EventLag,
		Lag:  lag,
	})
}

type runtimeClassifiedError struct {
	class ErrorClass
	err   error
}

func (classified *runtimeClassifiedError) Error() string {
	return classified.err.Error()
}

func (classified *runtimeClassifiedError) Unwrap() error {
	return classified.err
}

func classifiedError(class ErrorClass, err error) error {
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		class = ErrorContext
	}
	return &runtimeClassifiedError{class: class, err: err}
}

func errorClassOf(err error) ErrorClass {
	if classified, ok := errors.AsType[*runtimeClassifiedError](err); ok {
		return classified.class
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return ErrorContext
	}
	return ErrorSource
}

func nextReconnectAttempt(attempt uint32) uint32 {
	if attempt >= maximumBackoffAttempt {
		return maximumBackoffAttempt
	}
	return attempt + 1
}

func (r *Runtime) setActive(session Session) uint64 {
	r.mu.Lock()
	r.activeToken++
	token := r.activeToken
	r.active = session
	stopRequested := r.stopRequested
	r.mu.Unlock()
	if stopRequested {
		_ = session.Close()
	}
	return token
}

func (r *Runtime) clearActive(token uint64) {
	r.mu.Lock()
	if r.activeToken == token {
		r.active = nil
	}
	r.mu.Unlock()
}

func (r *Runtime) closeActive() {
	r.mu.Lock()
	active := r.active
	r.mu.Unlock()
	if active != nil {
		_ = active.Close()
	}
}

func (r *Runtime) fail(err error, signalReady func(error)) {
	r.setRunError(err)
	signalReady(err)
}

func (r *Runtime) setRunError(err error) {
	r.mu.Lock()
	if r.runErr == nil {
		r.runErr = err
	}
	r.mu.Unlock()
}

func (r *Runtime) waitError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runErr
}

func isFatalRuntimeError(err error) bool {
	return errors.Is(err, ErrInvalidFrame) ||
		errors.Is(err, ErrInvalidSchema) ||
		errors.Is(err, ErrSchemaMismatch) ||
		errors.Is(err, ErrInvalidMutation) ||
		errors.Is(err, ErrInvalidDelta) ||
		errors.Is(err, ErrReplayConflict)
}

func validateRuntimeContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidRuntime)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}
