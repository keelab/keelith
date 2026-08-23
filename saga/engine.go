package saga

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/coordination"
	"github.com/keelab/keelith/governance/failure"
)

const (
	defaultLeaseTTL                = 30 * time.Second
	defaultStepTimeout             = 30 * time.Second
	defaultMaxCompensationAttempts = 20
)

// FailureClassifier maps an internal error to a bounded durable reason.
type FailureClassifier func(error) string

// Config constructs one Engine.
type Config struct {
	Definition              Definition
	Repository              Repository
	Coordinator             coordination.Coordinator
	LeaseTTL                time.Duration
	StepTimeout             time.Duration
	MaxCompensationAttempts int
	ClassifyFailure         FailureClassifier
	Clock                   func() time.Time
}

// Description is an ID- and payload-free Engine snapshot.
type Description struct {
	Active               int64
	Started              uint64
	Completed            uint64
	Compensated          uint64
	TerminalFailures     uint64
	Contended            uint64
	ActionFailures       uint64
	CompensationFailures uint64
	LeaseLosses          uint64
	RepositoryFailures   uint64
}

// Engine executes one immutable saga Definition.
type Engine struct {
	definition              Definition
	repository              Repository
	coordinator             coordination.Coordinator
	leaseTTL                time.Duration
	stepTimeout             time.Duration
	maxCompensationAttempts int
	classify                FailureClassifier
	clock                   func() time.Time

	active               atomic.Int64
	started              atomic.Uint64
	completed            atomic.Uint64
	compensated          atomic.Uint64
	terminalFailures     atomic.Uint64
	contended            atomic.Uint64
	actionFailures       atomic.Uint64
	compensationFailures atomic.Uint64
	leaseLosses          atomic.Uint64
	repositoryFailures   atomic.Uint64
}

// New constructs an Engine without acquiring a lease or touching Repository.
func New(config Config) (*Engine, error) {
	if err := config.Definition.Validate(); err != nil {
		return nil, err
	}
	if isNil(config.Repository) || isNil(config.Coordinator) {
		return nil, fmt.Errorf(
			"%w: repository and coordinator are required",
			ErrInvalidOption,
		)
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = defaultLeaseTTL
	}
	if config.StepTimeout == 0 {
		config.StepTimeout = defaultStepTimeout
	}
	if config.MaxCompensationAttempts == 0 {
		config.MaxCompensationAttempts = defaultMaxCompensationAttempts
	}
	if config.LeaseTTL < 100*time.Millisecond ||
		config.LeaseTTL > 10*time.Minute ||
		config.StepTimeout < 10*time.Millisecond ||
		config.StepTimeout > 10*time.Minute ||
		config.MaxCompensationAttempts < 1 ||
		config.MaxCompensationAttempts > 1_000 {
		return nil, fmt.Errorf("%w: lifecycle budgets", ErrInvalidOption)
	}
	if config.ClassifyFailure == nil {
		config.ClassifyFailure = func(err error) string {
			return string(failure.Classify(err))
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &Engine{
		definition:              cloneDefinition(config.Definition),
		repository:              config.Repository,
		coordinator:             config.Coordinator,
		leaseTTL:                config.LeaseTTL,
		stepTimeout:             config.StepTimeout,
		maxCompensationAttempts: config.MaxCompensationAttempts,
		classify:                config.ClassifyFailure,
		clock:                   config.Clock,
	}, nil
}

// Run creates or resumes one saga instance under a renewable lease.
func (e *Engine) Run(
	ctx context.Context,
	id string,
) (result Result, resultErr error) {
	if e == nil || ctx == nil || !validIdentity(id, maxIdentityBytes) {
		return Result{}, fmt.Errorf(
			"%w: engine, context, or instance ID",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return Result{}, cause
	}
	key := "saga/" + e.definition.Name + "/" + id
	lease, acquired, err := e.coordinator.TryAcquire(
		ctx,
		key,
		e.leaseTTL,
	)
	if err != nil {
		return Result{}, fmt.Errorf("saga: acquire lease: %w", err)
	}
	if !acquired {
		e.contended.Add(1)
		return Result{}, ErrContended
	}
	e.active.Add(1)
	e.started.Add(1)
	defer e.active.Add(-1)
	defer func() {
		releaseContext, cancel := context.WithTimeout(
			context.Background(),
			e.leaseTTL,
		)
		defer cancel()
		resultErr = errors.Join(resultErr, lease.Release(releaseContext))
	}()

	runContext, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	stopMonitor := make(chan struct{})
	defer close(stopMonitor)
	go func() {
		select {
		case <-stopMonitor:
		case <-lease.Done():
			cause := lease.Err()
			if cause == nil {
				cause = coordination.ErrLeaseLost
			}
			cancelRun(cause)
		}
	}()

	record, err := e.loadOrCreate(runContext, id, lease.Fence())
	if err != nil {
		return Result{}, err
	}
	result.Record = record
	for {
		if cause := context.Cause(runContext); cause != nil {
			if errors.Is(cause, coordination.ErrLeaseLost) {
				e.leaseLosses.Add(1)
			}
			return result, cause
		}
		switch record.Status {
		case StatusCompleted:
			result.Record = record
			return result, nil
		case StatusCompensated:
			result.Record = record
			return result, ErrCompensated
		case StatusFailed:
			result.Record = record
			return result, ErrFailed
		case StatusRunning:
			if record.NextStep >= len(e.definition.Steps) {
				record.Status = StatusCompleted
				record.Attempt = 0
				record, err = e.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				e.completed.Add(1)
				continue
			}
			step := e.definition.Steps[record.NextStep]
			record.Attempt++
			record, err = e.save(runContext, record, lease.Fence())
			if err != nil {
				return result, err
			}
			invocation := e.invocation(
				record,
				step.Name,
				record.NextStep,
				PhaseAction,
				lease.Fence(),
			)
			if err := e.invoke(runContext, step.Action, invocation); err != nil {
				if cause := context.Cause(runContext); cause != nil {
					return result, cause
				}
				e.actionFailures.Add(1)
				record.Status = StatusCompensating
				record.CompensationIndex = record.NextStep - 1
				record.Attempt = 0
				record.FailureReason = e.failureReason(err)
				record.CauseReason = record.FailureReason
				record, err = e.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				e.compensated.Add(1)
				continue
			}
			result.Actions++
			record.NextStep++
			record.Attempt = 0
			record.FailureReason = ""
			record, err = e.save(runContext, record, lease.Fence())
			if err != nil {
				return result, err
			}
		case StatusCompensating:
			if record.CompensationIndex < 0 {
				record.Status = StatusCompensated
				record.Attempt = 0
				record, err = e.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				continue
			}
			step := e.definition.Steps[record.CompensationIndex]
			if step.Compensate == nil {
				record.CompensationIndex--
				record.Attempt = 0
				record, err = e.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				continue
			}
			record.Attempt++
			record, err = e.save(runContext, record, lease.Fence())
			if err != nil {
				return result, err
			}
			invocation := e.invocation(
				record,
				step.Name,
				record.CompensationIndex,
				PhaseCompensation,
				lease.Fence(),
			)
			if err := e.invoke(
				runContext,
				step.Compensate,
				invocation,
			); err != nil {
				if cause := context.Cause(runContext); cause != nil {
					return result, cause
				}
				e.compensationFailures.Add(1)
				record.FailureReason = e.failureReason(err)
				if record.Attempt >= e.maxCompensationAttempts {
					record.Status = StatusFailed
				}
				record, saveErr := e.save(
					runContext,
					record,
					lease.Fence(),
				)
				result.Record = record
				if saveErr != nil {
					return result, saveErr
				}
				if record.Status == StatusFailed {
					e.terminalFailures.Add(1)
					return result, errors.Join(ErrFailed, err)
				}
				return result, fmt.Errorf(
					"saga: compensate step %q: %w",
					step.Name,
					err,
				)
			}
			result.Compensated++
			record.CompensationIndex--
			record.Attempt = 0
			record.FailureReason = record.CauseReason
			record, err = e.save(runContext, record, lease.Fence())
			if err != nil {
				return result, err
			}
		default:
			return result, fmt.Errorf("%w: record status", ErrInvalidOption)
		}
		result.Record = record
	}
}

// Description returns aggregate orchestration status without instance IDs.
func (e *Engine) Description() Description {
	if e == nil {
		return Description{}
	}
	return Description{
		Active:               e.active.Load(),
		Started:              e.started.Load(),
		Completed:            e.completed.Load(),
		Compensated:          e.compensated.Load(),
		TerminalFailures:     e.terminalFailures.Load(),
		Contended:            e.contended.Load(),
		ActionFailures:       e.actionFailures.Load(),
		CompensationFailures: e.compensationFailures.Load(),
		LeaseLosses:          e.leaseLosses.Load(),
		RepositoryFailures:   e.repositoryFailures.Load(),
	}
}

func (e *Engine) loadOrCreate(
	ctx context.Context,
	id string,
	fence uint64,
) (Record, error) {
	record, err := e.repository.Load(ctx, id)
	if errors.Is(err, ErrNotFound) {
		record = Record{
			ID:                id,
			Definition:        e.definition.Name,
			Version:           e.definition.Version,
			Status:            StatusRunning,
			CompensationIndex: -1,
			UpdatedAt:         e.clock().UTC(),
		}
		record, err = e.repository.Create(ctx, record, fence)
	}
	if err != nil {
		e.repositoryFailures.Add(1)
		return Record{}, fmt.Errorf("saga: load or create: %w", err)
	}
	if record.Definition != e.definition.Name ||
		record.Version != e.definition.Version {
		return Record{}, ErrDefinitionMismatch
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	if record.NextStep > len(e.definition.Steps) ||
		record.CompensationIndex >= len(e.definition.Steps) {
		return Record{}, fmt.Errorf("%w: step position", ErrInvalidOption)
	}
	return record, nil
}

func (e *Engine) save(
	ctx context.Context,
	record Record,
	fence uint64,
) (Record, error) {
	expected := record.Revision
	record.UpdatedAt = e.clock().UTC()
	saved, err := e.repository.Save(ctx, record, expected, fence)
	if err != nil {
		e.repositoryFailures.Add(1)
		return Record{}, fmt.Errorf("saga: save: %w", err)
	}
	return saved, nil
}

func (e *Engine) invoke(
	ctx context.Context,
	handler Handler,
	invocation Invocation,
) (err error) {
	stepContext, cancel := context.WithTimeout(ctx, e.stepTimeout)
	defer cancel()
	stepContext = coordination.WithFence(stepContext, invocation.Fence)
	defer func() {
		if recover() != nil {
			err = errHandlerPanic
		}
	}()
	err = handler(stepContext, invocation)
	if err == nil {
		err = context.Cause(stepContext)
	}
	return err
}

func (e *Engine) invocation(
	record Record,
	step string,
	index int,
	phase Phase,
	fence uint64,
) Invocation {
	return Invocation{
		SagaID:     record.ID,
		Definition: record.Definition,
		Version:    record.Version,
		Step:       step,
		StepIndex:  index,
		Phase:      phase,
		Attempt:    record.Attempt,
		Fence:      fence,
		IdempotencyKey: idempotencyKey(
			record.ID,
			record.Definition,
			record.Version,
			step,
			phase,
		),
	}
}

func (e *Engine) failureReason(err error) string {
	reason := e.classify(err)
	if !validIdentity(reason, maxIdentityBytes) {
		return "internal"
	}
	return reason
}

func cloneDefinition(definition Definition) Definition {
	return Definition{
		Name:    definition.Name,
		Version: definition.Version,
		Steps:   append([]Step(nil), definition.Steps...),
	}
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
