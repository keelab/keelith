package continuation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	defaultPollInterval   = 100 * time.Millisecond
	defaultBatchSize      = 100
	defaultMaxTransitions = 64
	defaultLeaseDuration  = 30 * time.Second
)

var (
	// ErrInvalidRuntime reports missing dependencies or unsafe budgets.
	ErrInvalidRuntime = errors.New("continuation: invalid runtime")
	// ErrTransitionBudget reports a Machine that remained Running too long.
	ErrTransitionBudget = errors.New("continuation: transition budget exhausted")
)

// RuntimeConfig constructs one Runtime.
type RuntimeConfig struct {
	Store             Store
	Registry          *Registry
	PollInterval      time.Duration
	BatchSize         int
	MaxTransitions    int
	ExecutorID        string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	Observer          Observer
	TerminalRetention time.Duration
	Clock             func() time.Time
}

// Runtime executes registered Machines against durable Store snapshots.
type Runtime struct {
	store             Store
	leases            LeaseStore
	registry          *Registry
	pollInterval      time.Duration
	batchSize         int
	maxTransitions    int
	executorID        string
	leaseDuration     time.Duration
	heartbeat         time.Duration
	observer          Observer
	terminalRetention time.Duration
	now               func() time.Time
}

// NewRuntime validates and constructs a Runtime.
func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	if isNilStore(config.Store) ||
		config.Registry == nil ||
		!config.Registry.Frozen() {
		return nil, fmt.Errorf(
			"%w: store and frozen registry are required",
			ErrInvalidRuntime,
		)
	}
	leaseStore, ok := config.Store.(LeaseStore)
	if !ok || isNilLeaseStore(leaseStore) {
		return nil, ErrLeaseUnsupported
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.MaxTransitions == 0 {
		config.MaxTransitions = defaultMaxTransitions
	}
	if config.ExecutorID == "" {
		var err error
		config.ExecutorID, err = randomExecutorID()
		if err != nil {
			return nil, fmt.Errorf(
				"%w: executor identity: %w",
				ErrInvalidRuntime,
				err,
			)
		}
	}
	if config.LeaseDuration == 0 {
		config.LeaseDuration = defaultLeaseDuration
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = config.LeaseDuration / 3
	}
	if config.TerminalRetention == 0 {
		config.TerminalRetention = defaultTerminalRetention
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	if config.PollInterval <= 0 ||
		config.PollInterval > time.Minute ||
		config.BatchSize <= 0 ||
		config.BatchSize > 10_000 ||
		config.MaxTransitions <= 0 ||
		config.MaxTransitions > 10_000 ||
		!validLeaseOwner(config.ExecutorID) ||
		config.LeaseDuration <= 0 ||
		config.LeaseDuration > maxLeaseDuration ||
		config.HeartbeatInterval <= 0 ||
		config.HeartbeatInterval >= config.LeaseDuration ||
		config.TerminalRetention <= 0 ||
		config.TerminalRetention > maxTerminalRetention {
		return nil, fmt.Errorf("%w: runtime budgets", ErrInvalidRuntime)
	}
	return &Runtime{
		store:             config.Store,
		leases:            leaseStore,
		registry:          config.Registry,
		pollInterval:      config.PollInterval,
		batchSize:         config.BatchSize,
		maxTransitions:    config.MaxTransitions,
		executorID:        config.ExecutorID,
		leaseDuration:     config.LeaseDuration,
		heartbeat:         config.HeartbeatInterval,
		observer:          config.Observer,
		terminalRetention: config.TerminalRetention,
		now:               config.Clock,
	}, nil
}

// Create stores one new accepted call.
func (runtime *Runtime) Create(
	ctx context.Context,
	callID CallID,
	operation Operation,
) (Snapshot, error) {
	if runtime == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	snapshot, err := NewSnapshot(callID, operation)
	if err != nil {
		return Snapshot{}, err
	}
	if _, exists := runtime.registry.Resolve(operation); !exists {
		return Snapshot{}, ErrMachineNotFound
	}
	return runtime.store.Create(ctx, snapshot)
}

// SubmitSignal atomically wakes one waiting call.
func (runtime *Runtime) SubmitSignal(
	ctx context.Context,
	callID CallID,
	commandID string,
	payload []byte,
) (Snapshot, error) {
	if runtime == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	current, err := runtime.store.Load(ctx, callID)
	if err != nil {
		return Snapshot{}, err
	}
	return runtime.store.SubmitSignal(ctx, CommandRequest{
		CallID:           callID,
		ExpectedRevision: current.Revision(),
		CommandID:        commandID,
		Payload:          append([]byte(nil), payload...),
	})
}

// RequestCancel atomically requests cooperative cancellation.
func (runtime *Runtime) RequestCancel(
	ctx context.Context,
	callID CallID,
	commandID string,
) (Snapshot, error) {
	if runtime == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	current, err := runtime.store.Load(ctx, callID)
	if err != nil {
		return Snapshot{}, err
	}
	return runtime.store.RequestCancel(ctx, CommandRequest{
		CallID:           callID,
		ExpectedRevision: current.Revision(),
		CommandID:        commandID,
	})
}

// RunOnce acquires and advances one bounded ready batch.
//
// A Machine can execute successfully while its Store commit fails. Another
// executor will then run the same revision again. This is intentionally
// at-least-once and does not make application side effects exactly-once.
func (runtime *Runtime) RunOnce(ctx context.Context) (int, error) {
	if runtime == nil || ctx == nil {
		return 0, ErrInvalidRuntime
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	ready, err := runtime.store.ListReady(ctx, runtime.batchSize)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, candidate := range ready {
		if candidate.workflow != nil {
			continue
		}
		if cause := context.Cause(ctx); cause != nil {
			return processed, cause
		}
		claim, acquireErr := runtime.leases.Claim(ctx, ClaimRequest{
			CallID:           candidate.CallID(),
			ExpectedRevision: candidate.Revision(),
			OwnerID:          runtime.executorID,
			LeaseDuration:    runtime.leaseDuration,
		})
		if errors.Is(acquireErr, ErrConflict) ||
			errors.Is(acquireErr, ErrNotReady) ||
			errors.Is(acquireErr, ErrLeaseHeld) {
			continue
		}
		if acquireErr != nil {
			return processed, acquireErr
		}
		processed++
		runtime.observe(ctx, Event{
			Kind:   EventClaim,
			Status: claim.Snapshot.Status(),
		})
		machine, exists := runtime.registry.Resolve(claim.Snapshot.Operation())
		if !exists {
			runtime.release(ctx, claim.Snapshot)
			return processed, ErrMachineNotFound
		}
		current := claim.Snapshot
		for step := 0; step < runtime.maxTransitions; step++ {
			transition, advanceErr := runtime.advance(ctx, machine, current)
			if advanceErr != nil {
				if cause := context.Cause(ctx); cause != nil {
					runtime.release(ctx, current)
					return processed, cause
				}
				_, handled, handleErr :=
					runtime.commitMachineError(
						ctx,
						current,
						advanceErr,
					)
				if handled {
					if handleErr != nil {
						runtime.release(ctx, current)
						return processed, handleErr
					}
					break
				}
				runtime.release(ctx, current)
				return processed, fmt.Errorf(
					"continuation: advance machine: %w",
					advanceErr,
				)
			}
			next, applyErr := Apply(current, transition)
			if applyErr != nil {
				runtime.release(ctx, current)
				return processed, applyErr
			}
			next, commitErr := runtime.store.Transition(
				ctx,
				runtime.commitRequest(current, next),
			)
			if commitErr != nil {
				runtime.release(ctx, current)
				return processed, commitErr
			}
			current = next
			runtime.observe(ctx, Event{
				Kind:   EventTransition,
				Status: current.Status(),
			})
			if current.Status() != StatusRunning {
				break
			}
			if step == runtime.maxTransitions-1 {
				runtime.release(ctx, current)
				return processed, ErrTransitionBudget
			}
		}
	}
	return processed, nil
}

func (runtime *Runtime) advance(
	ctx context.Context,
	machine Machine,
	current Snapshot,
) (Transition, error) {
	advanceCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	done := make(chan struct{})
	heartbeatResult := make(chan error, 1)
	go func() {
		timer := time.NewTicker(runtime.heartbeat)
		defer timer.Stop()
		for {
			select {
			case <-done:
				heartbeatResult <- nil
				return
			case <-advanceCtx.Done():
				heartbeatResult <- context.Cause(advanceCtx)
				return
			case <-timer.C:
				_, err := runtime.leases.Renew(
					advanceCtx,
					LeaseRequest{
						CallID:        current.CallID(),
						Revision:      current.Revision(),
						Fence:         current.Fence(),
						OwnerID:       runtime.executorID,
						LeaseDuration: runtime.leaseDuration,
					},
				)
				if err != nil {
					runtime.observe(advanceCtx, Event{
						Kind:       EventRenew,
						Status:     current.Status(),
						ErrorClass: ErrorClassInternal,
					})
					cancel(err)
					heartbeatResult <- err
					return
				}
				runtime.observe(advanceCtx, Event{
					Kind:   EventRenew,
					Status: current.Status(),
				})
			}
		}
	}()
	transition, err := machine.Advance(advanceCtx, current)
	close(done)
	heartbeatErr := <-heartbeatResult
	if heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
		return Transition{}, heartbeatErr
	}
	return transition, err
}

func (runtime *Runtime) release(ctx context.Context, snapshot Snapshot) {
	if runtime == nil || runtime.leases == nil {
		return
	}
	_ = runtime.leases.Release(ctx, LeaseRequest{
		CallID:   snapshot.CallID(),
		Revision: snapshot.Revision(),
		Fence:    snapshot.Fence(),
		OwnerID:  runtime.executorID,
	})
}

// Run polls and advances ready calls until ctx ends or a runtime error occurs.
func (runtime *Runtime) Run(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidRuntime
	}
	for {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if _, err := runtime.RunOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(runtime.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}

func isNilStore(store Store) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilLeaseStore(store LeaseStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func randomExecutorID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "executor-" + hex.EncodeToString(value[:]), nil
}
