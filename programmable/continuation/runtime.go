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
func (r *Runtime) Create(
	ctx context.Context,
	callID CallID,
	operation Operation,
) (Snapshot, error) {
	if r == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	snapshot, err := NewSnapshot(callID, operation)
	if err != nil {
		return Snapshot{}, err
	}
	if _, exists := r.registry.Resolve(operation); !exists {
		return Snapshot{}, ErrMachineNotFound
	}
	return r.store.Create(ctx, snapshot)
}

// SubmitSignal atomically wakes one waiting call.
func (r *Runtime) SubmitSignal(
	ctx context.Context,
	callID CallID,
	commandID string,
	payload []byte,
) (Snapshot, error) {
	if r == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	current, err := r.store.Load(ctx, callID)
	if err != nil {
		return Snapshot{}, err
	}
	return r.store.SubmitSignal(ctx, CommandRequest{
		CallID:           callID,
		ExpectedRevision: current.Revision(),
		CommandID:        commandID,
		Payload:          append([]byte(nil), payload...),
	})
}

// RequestCancel atomically requests cooperative cancellation.
func (r *Runtime) RequestCancel(
	ctx context.Context,
	callID CallID,
	commandID string,
) (Snapshot, error) {
	if r == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	current, err := r.store.Load(ctx, callID)
	if err != nil {
		return Snapshot{}, err
	}
	return r.store.RequestCancel(ctx, CommandRequest{
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
func (r *Runtime) RunOnce(ctx context.Context) (int, error) {
	if r == nil || ctx == nil {
		return 0, ErrInvalidRuntime
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	ready, err := r.store.ListReady(ctx, r.batchSize)
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
		claim, acquireErr := r.leases.Claim(ctx, ClaimRequest{
			CallID:           candidate.CallID(),
			ExpectedRevision: candidate.Revision(),
			OwnerID:          r.executorID,
			LeaseDuration:    r.leaseDuration,
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
		r.observe(ctx, Event{
			Kind:   EventClaim,
			Status: claim.Snapshot.Status(),
		})
		machine, exists := r.registry.Resolve(claim.Snapshot.Operation())
		if !exists {
			r.release(ctx, claim.Snapshot)
			return processed, ErrMachineNotFound
		}
		current := claim.Snapshot
		for step := 0; step < r.maxTransitions; step++ {
			transition, advanceErr := r.advance(ctx, machine, current)
			if advanceErr != nil {
				if cause := context.Cause(ctx); cause != nil {
					r.release(ctx, current)
					return processed, cause
				}
				_, handled, handleErr :=
					r.commitMachineError(
						ctx,
						current,
						advanceErr,
					)
				if handled {
					if handleErr != nil {
						r.release(ctx, current)
						return processed, handleErr
					}
					break
				}
				r.release(ctx, current)
				return processed, fmt.Errorf(
					"continuation: advance machine: %w",
					advanceErr,
				)
			}
			next, applyErr := Apply(current, transition)
			if applyErr != nil {
				r.release(ctx, current)
				return processed, applyErr
			}
			next, commitErr := r.store.Transition(
				ctx,
				r.commitRequest(current, next),
			)
			if commitErr != nil {
				r.release(ctx, current)
				return processed, commitErr
			}
			current = next
			r.observe(ctx, Event{
				Kind:   EventTransition,
				Status: current.Status(),
			})
			if current.Status() != StatusRunning {
				break
			}
			if step == r.maxTransitions-1 {
				r.release(ctx, current)
				return processed, ErrTransitionBudget
			}
		}
	}
	return processed, nil
}

func (r *Runtime) advance(
	ctx context.Context,
	machine Machine,
	current Snapshot,
) (Transition, error) {
	advanceCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	done := make(chan struct{})
	heartbeatResult := make(chan error, 1)
	go func() {
		timer := time.NewTicker(r.heartbeat)
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
				_, err := r.leases.Renew(
					advanceCtx,
					LeaseRequest{
						CallID:        current.CallID(),
						Revision:      current.Revision(),
						Fence:         current.Fence(),
						OwnerID:       r.executorID,
						LeaseDuration: r.leaseDuration,
					},
				)
				if err != nil {
					r.observe(advanceCtx, Event{
						Kind:       EventRenew,
						Status:     current.Status(),
						ErrorClass: ErrorClassInternal,
					})
					cancel(err)
					heartbeatResult <- err
					return
				}
				r.observe(advanceCtx, Event{
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

func (r *Runtime) release(ctx context.Context, snapshot Snapshot) {
	if r == nil || r.leases == nil {
		return
	}
	_ = r.leases.Release(ctx, LeaseRequest{
		CallID:   snapshot.CallID(),
		Revision: snapshot.Revision(),
		Fence:    snapshot.Fence(),
		OwnerID:  r.executorID,
	})
}

// Run polls and advances ready calls until ctx ends or a runtime error occurs.
func (r *Runtime) Run(ctx context.Context) error {
	if r == nil || ctx == nil {
		return ErrInvalidRuntime
	}
	for {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if _, err := r.RunOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(r.pollInterval)
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
