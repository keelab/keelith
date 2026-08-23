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
func (engine *Engine) Run(
	ctx context.Context,
	id string,
) (result Result, resultErr error) {
	if engine == nil || ctx == nil || !validIdentity(id, maxIdentityBytes) {
		return Result{}, fmt.Errorf(
			"%w: engine, context, or instance ID",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return Result{}, cause
	}
	key := "saga/" + engine.definition.Name + "/" + id
	lease, acquired, err := engine.coordinator.TryAcquire(
		ctx,
		key,
		engine.leaseTTL,
	)
	if err != nil {
		return Result{}, fmt.Errorf("saga: acquire lease: %w", err)
	}
	if !acquired {
		engine.contended.Add(1)
		return Result{}, ErrContended
	}
	engine.active.Add(1)
	engine.started.Add(1)
	defer engine.active.Add(-1)
	defer func() {
		releaseContext, cancel := context.WithTimeout(
			context.Background(),
			engine.leaseTTL,
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

	record, err := engine.loadOrCreate(runContext, id, lease.Fence())
	if err != nil {
		return Result{}, err
	}
	result.Record = record
	for {
		if cause := context.Cause(runContext); cause != nil {
			if errors.Is(cause, coordination.ErrLeaseLost) {
				engine.leaseLosses.Add(1)
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
			if record.NextStep >= len(engine.definition.Steps) {
				record.Status = StatusCompleted
				record.Attempt = 0
				record, err = engine.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				engine.completed.Add(1)
				continue
			}
			step := engine.definition.Steps[record.NextStep]
			record.Attempt++
			record, err = engine.save(runContext, record, lease.Fence())
			if err != nil {
				return result, err
			}
			invocation := engine.invocation(
				record,
				step.Name,
				record.NextStep,
				PhaseAction,
				lease.Fence(),
			)
			if err := engine.invoke(runContext, step.Action, invocation); err != nil {
				if cause := context.Cause(runContext); cause != nil {
					return result, cause
				}
				engine.actionFailures.Add(1)
				record.Status = StatusCompensating
				record.CompensationIndex = record.NextStep - 1
				record.Attempt = 0
				record.FailureReason = engine.failureReason(err)
				record.CauseReason = record.FailureReason
				record, err = engine.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				engine.compensated.Add(1)
				continue
			}
			result.Actions++
			record.NextStep++
			record.Attempt = 0
			record.FailureReason = ""
			record, err = engine.save(runContext, record, lease.Fence())
			if err != nil {
				return result, err
			}
		case StatusCompensating:
			if record.CompensationIndex < 0 {
				record.Status = StatusCompensated
				record.Attempt = 0
				record, err = engine.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				continue
			}
			step := engine.definition.Steps[record.CompensationIndex]
			if step.Compensate == nil {
				record.CompensationIndex--
				record.Attempt = 0
				record, err = engine.save(runContext, record, lease.Fence())
				if err != nil {
					return result, err
				}
				continue
			}
			record.Attempt++
			record, err = engine.save(runContext, record, lease.Fence())
			if err != nil {
				return result, err
			}
			invocation := engine.invocation(
				record,
				step.Name,
				record.CompensationIndex,
				PhaseCompensation,
				lease.Fence(),
			)
			if err := engine.invoke(
				runContext,
				step.Compensate,
				invocation,
			); err != nil {
				if cause := context.Cause(runContext); cause != nil {
					return result, cause
				}
				engine.compensationFailures.Add(1)
				record.FailureReason = engine.failureReason(err)
				if record.Attempt >= engine.maxCompensationAttempts {
					record.Status = StatusFailed
				}
				record, saveErr := engine.save(
					runContext,
					record,
					lease.Fence(),
				)
				result.Record = record
				if saveErr != nil {
					return result, saveErr
				}
				if record.Status == StatusFailed {
					engine.terminalFailures.Add(1)
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
			record, err = engine.save(runContext, record, lease.Fence())
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
func (engine *Engine) Description() Description {
	if engine == nil {
		return Description{}
	}
	return Description{
		Active:               engine.active.Load(),
		Started:              engine.started.Load(),
		Completed:            engine.completed.Load(),
		Compensated:          engine.compensated.Load(),
		TerminalFailures:     engine.terminalFailures.Load(),
		Contended:            engine.contended.Load(),
		ActionFailures:       engine.actionFailures.Load(),
		CompensationFailures: engine.compensationFailures.Load(),
		LeaseLosses:          engine.leaseLosses.Load(),
		RepositoryFailures:   engine.repositoryFailures.Load(),
	}
}

func (engine *Engine) loadOrCreate(
	ctx context.Context,
	id string,
	fence uint64,
) (Record, error) {
	record, err := engine.repository.Load(ctx, id)
	if errors.Is(err, ErrNotFound) {
		record = Record{
			ID:                id,
			Definition:        engine.definition.Name,
			Version:           engine.definition.Version,
			Status:            StatusRunning,
			CompensationIndex: -1,
			UpdatedAt:         engine.clock().UTC(),
		}
		record, err = engine.repository.Create(ctx, record, fence)
	}
	if err != nil {
		engine.repositoryFailures.Add(1)
		return Record{}, fmt.Errorf("saga: load or create: %w", err)
	}
	if record.Definition != engine.definition.Name ||
		record.Version != engine.definition.Version {
		return Record{}, ErrDefinitionMismatch
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	if record.NextStep > len(engine.definition.Steps) ||
		record.CompensationIndex >= len(engine.definition.Steps) {
		return Record{}, fmt.Errorf("%w: step position", ErrInvalidOption)
	}
	return record, nil
}

func (engine *Engine) save(
	ctx context.Context,
	record Record,
	fence uint64,
) (Record, error) {
	expected := record.Revision
	record.UpdatedAt = engine.clock().UTC()
	saved, err := engine.repository.Save(ctx, record, expected, fence)
	if err != nil {
		engine.repositoryFailures.Add(1)
		return Record{}, fmt.Errorf("saga: save: %w", err)
	}
	return saved, nil
}

func (engine *Engine) invoke(
	ctx context.Context,
	handler Handler,
	invocation Invocation,
) (err error) {
	stepContext, cancel := context.WithTimeout(ctx, engine.stepTimeout)
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

func (engine *Engine) invocation(
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

func (engine *Engine) failureReason(err error) string {
	reason := engine.classify(err)
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
