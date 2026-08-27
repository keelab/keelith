package cron

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/keelab/keelith/worker"
	robfig "github.com/robfig/cron/v3"
)

// State is the observable local scheduler lifecycle.
type State string

const (
	// StateNew means Schedule has not started the engine.
	StateNew State = "new"
	// StateRunning means ticks and manual triggers are accepted.
	StateRunning State = "running"
	// StateStopping means new triggers are disabled while executions drain.
	StateStopping State = "stopping"
	// StateClosed means resources are closed and Wait can return.
	StateClosed State = "closed"
)

// Description is an immutable operational snapshot.
type Description struct {
	Name            string
	Spec            string
	Location        string
	State           State
	Overlap         OverlapPolicy
	Misfire         MisfirePolicy
	Accepting       bool
	Active          int
	Triggered       uint64
	Attempts        uint64
	Retries         uint64
	Completed       uint64
	Skipped         uint64
	LastScheduledAt time.Time
	LastStartedAt   time.Time
	LastCompletedAt time.Time
	LastAction      worker.Action
	LastFailed      bool
	Failures        uint64
	Capabilities    worker.SchedulerCapabilities
}

// Scheduler owns one local cron expression and implements worker.Scheduler.
type Scheduler struct {
	config normalizedConfig
	engine *robfig.Cron
	entry  robfig.EntryID

	mu            sync.Mutex
	state         State
	handler       worker.JobHandler
	accepting     bool
	runtimeCtx    context.Context
	cancelRuntime context.CancelCauseFunc
	pullingCtx    context.Context
	cancelPulling context.CancelFunc
	active        int
	sequence      uint64
	description   Description
	drained       chan struct{}

	pullingStopped chan struct{}
	done           chan struct{}
	stopOnce       sync.Once
	doneOnce       sync.Once
}

// New validates a local schedule without starting goroutines.
func New(config Config) (*Scheduler, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	scheduler := &Scheduler{
		config:         normalized,
		state:          StateNew,
		pullingStopped: make(chan struct{}),
		done:           make(chan struct{}),
		drained:        closedSignal(),
		description: Description{
			Name:     normalized.name,
			Spec:     normalized.spec,
			Location: normalized.location.String(),
			State:    StateNew,
			Overlap:  normalized.overlap,
			Misfire:  normalized.misfire,
			Capabilities: worker.SchedulerCapabilities{
				TriggerAuthority: worker.TriggerAuthorityLocal,
				Ownership:        worker.OwnershipPerReplica,
			},
		},
	}
	engine, entry, err := newEngine(normalized, scheduler.fire)
	if err != nil {
		return nil, err
	}
	scheduler.engine = engine
	scheduler.entry = entry
	return scheduler, nil
}

// SchedulerCapabilities declares that Cron is local and runs per replica.
func (*Scheduler) SchedulerCapabilities() worker.SchedulerCapabilities {
	return worker.SchedulerCapabilities{
		TriggerAuthority: worker.TriggerAuthorityLocal,
		Ownership:        worker.OwnershipPerReplica,
	}
}

// Schedule starts accepting ticks and returns after the cron engine is ready.
func (scheduler *Scheduler) Schedule(
	ctx context.Context,
	handler worker.JobHandler,
) error {
	if scheduler == nil || handler == nil {
		return fmt.Errorf(
			"%w: scheduler or handler is nil",
			ErrInvalidOption,
		)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	scheduler.mu.Lock()
	if scheduler.state != StateNew {
		scheduler.mu.Unlock()
		return ErrAlreadyScheduled
	}
	scheduler.runtimeCtx, scheduler.cancelRuntime =
		context.WithCancelCause(context.WithoutCancel(ctx))
	scheduler.pullingCtx, scheduler.cancelPulling =
		context.WithCancel(context.Background())
	scheduler.handler = handler
	scheduler.accepting = true
	scheduler.state = StateRunning
	scheduler.description.State = StateRunning
	scheduler.description.Accepting = true
	scheduler.mu.Unlock()

	scheduler.engine.Start()
	return nil
}

// RunNow runs one ad-hoc execution through the same overlap, retry, Worker,
// Middleware and drain semantics as a cron tick.
func (scheduler *Scheduler) RunNow(
	ctx context.Context,
) (worker.Result, error) {
	if scheduler == nil {
		return worker.Result{}, fmt.Errorf(
			"%w: scheduler is nil",
			ErrInvalidOption,
		)
	}
	if ctx == nil {
		return worker.Result{}, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidOption,
		)
	}
	results, err := scheduler.launch(
		ctx,
		time.Now().In(scheduler.config.location),
	)
	if err != nil {
		return worker.Result{}, err
	}
	select {
	case result := <-results:
		return result, nil
	case <-ctx.Done():
		return worker.Result{}, context.Cause(ctx)
	}
}

// StopPulling prevents new ticks and retries. It does not cancel a handler
// already running.
func (scheduler *Scheduler) StopPulling(ctx context.Context) error {
	if scheduler == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	scheduler.mu.Lock()
	switch scheduler.state {
	case StateNew, StateClosed:
		scheduler.mu.Unlock()
		return nil
	case StateRunning:
		scheduler.accepting = false
		scheduler.state = StateStopping
		scheduler.description.Accepting = false
		scheduler.description.State = StateStopping
		cancelPulling := scheduler.cancelPulling
		scheduler.mu.Unlock()
		if cancelPulling != nil {
			cancelPulling()
		}
	case StateStopping:
		scheduler.mu.Unlock()
	default:
		scheduler.mu.Unlock()
	}

	scheduler.stopOnce.Do(func() {
		stopped := scheduler.engine.Stop()
		<-stopped.Done()
		close(scheduler.pullingStopped)
	})
	select {
	case <-scheduler.pullingStopped:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Drain waits for active executions and retry delays to finish.
func (scheduler *Scheduler) Drain(ctx context.Context) error {
	if scheduler == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	scheduler.mu.Lock()
	drained := scheduler.drained
	scheduler.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Close stops scheduling, cancels remaining executions and releases Wait.
func (scheduler *Scheduler) Close(ctx context.Context) error {
	if scheduler == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	stopErr := scheduler.StopPulling(ctx)

	scheduler.mu.Lock()
	if scheduler.state == StateNew {
		scheduler.state = StateClosed
		scheduler.description.State = StateClosed
		scheduler.description.Accepting = false
		scheduler.doneOnce.Do(func() { close(scheduler.done) })
		scheduler.mu.Unlock()
		return stopErr
	}
	scheduler.state = StateClosed
	scheduler.description.State = StateClosed
	scheduler.description.Accepting = false
	cancelRuntime := scheduler.cancelRuntime
	active := scheduler.active
	if active == 0 {
		scheduler.doneOnce.Do(func() { close(scheduler.done) })
	}
	scheduler.mu.Unlock()
	if cancelRuntime != nil {
		cancelRuntime(context.Canceled)
	}

	select {
	case <-scheduler.done:
		return stopErr
	case <-ctx.Done():
		return errors.Join(stopErr, context.Cause(ctx))
	}
}

// Wait blocks until Close has released scheduler resources.
func (scheduler *Scheduler) Wait() error {
	if scheduler == nil {
		return nil
	}
	scheduler.mu.Lock()
	state := scheduler.state
	done := scheduler.done
	scheduler.mu.Unlock()
	if state == StateNew {
		return ErrNotRunning
	}
	<-done
	return nil
}

// Describe returns lifecycle, policy and bounded execution diagnostics.
func (scheduler *Scheduler) Describe() Description {
	if scheduler == nil {
		return Description{State: StateClosed, LastFailed: true}
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	description := scheduler.description
	description.Active = scheduler.active
	return description
}

func (scheduler *Scheduler) fire() {
	scheduledAt := scheduler.engine.Entry(scheduler.entry).Prev
	if scheduledAt.IsZero() {
		scheduledAt = time.Now().In(scheduler.config.location)
	}
	_, _ = scheduler.launch(
		context.Background(),
		scheduledAt,
	)
}

func (scheduler *Scheduler) launch(
	ctx context.Context,
	scheduledAt time.Time,
) (<-chan worker.Result, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	scheduler.mu.Lock()
	if scheduler.state != StateRunning || !scheduler.accepting {
		scheduler.mu.Unlock()
		return nil, ErrNotRunning
	}
	if scheduler.config.overlap == OverlapForbid && scheduler.active > 0 {
		scheduler.description.Skipped++
		scheduler.mu.Unlock()
		return nil, ErrOverlap
	}
	if scheduler.active == 0 {
		scheduler.drained = make(chan struct{})
	}
	scheduler.active++
	scheduler.sequence++
	sequence := scheduler.sequence
	scheduler.description.Triggered++
	scheduler.description.LastScheduledAt = scheduledAt
	handler := scheduler.handler
	runtimeCtx := scheduler.runtimeCtx
	pullingCtx := scheduler.pullingCtx
	scheduler.mu.Unlock()

	invocationCtx, cancel := mergeContexts(ctx, runtimeCtx)
	results := make(chan worker.Result, 1)
	task := executionTask{
		ID: scheduler.config.name + "-" +
			strconv.FormatInt(scheduledAt.UnixNano(), 10) + "-" +
			strconv.FormatUint(sequence, 10),
		scheduledAt: scheduledAt,
		handler:     handler,
		context:     invocationCtx,
		cancel:      cancel,
		pulling:     pullingCtx,
		results:     results,
	}
	go scheduler.execute(task)
	return results, nil
}

func closedSignal() chan struct{} {
	signal := make(chan struct{})
	close(signal)
	return signal
}
