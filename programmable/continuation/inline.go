package continuation

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	maxInlineDuration    = 5 * time.Second
	inlineCleanupTimeout = 250 * time.Millisecond
)

var (
	// ErrInvalidInlineBudget reports an unsafe inline execution budget.
	ErrInvalidInlineBudget = errors.New(
		"continuation: invalid inline execution budget",
	)
	// ErrInlineBudget is the internal cancellation cause used when an inline
	// attempt must fall back to durable asynchronous execution.
	ErrInlineBudget = errors.New("continuation: inline execution budget exhausted")
)

// InlineBudget bounds synchronous execution after a durable call is created.
//
// Duration bounds wall time and MaxTransitions bounds committed Machine
// transitions. Exhausting either budget is not a call failure: the latest
// durable non-terminal Snapshot is returned so another executor can resume it.
type InlineBudget struct {
	Duration       time.Duration
	MaxTransitions int
}

// StartCallInline durably creates a call and attempts to finish it within a
// bounded synchronous execution window.
//
// Machine implementations must honor ctx cancellation. A Machine that ignores
// cancellation cannot be safely abandoned because it may still perform
// externally visible side effects.
func (r *Runtime) StartCallInline(
	ctx context.Context,
	callID CallID,
	operation Operation,
	input []byte,
	budget InlineBudget,
) (Snapshot, error) {
	if r == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	if budget.Duration <= 0 ||
		budget.Duration > maxInlineDuration ||
		budget.MaxTransitions <= 0 ||
		budget.MaxTransitions > 10_000 {
		return Snapshot{}, ErrInvalidInlineBudget
	}

	created, err := r.StartCall(ctx, callID, operation, input)
	if err != nil {
		return Snapshot{}, err
	}
	inlineCtx, cancel := context.WithTimeoutCause(
		ctx,
		budget.Duration,
		ErrInlineBudget,
	)
	defer cancel()

	claim, err := r.leases.Claim(inlineCtx, ClaimRequest{
		CallID:           created.CallID(),
		ExpectedRevision: created.Revision(),
		OwnerID:          r.executorID,
		LeaseDuration:    r.leaseDuration,
	})
	if err != nil {
		if inlineBudgetExhausted(inlineCtx) {
			return created, nil
		}
		return Snapshot{}, err
	}
	current := claim.Snapshot
	r.observe(ctx, Event{
		Kind:   EventClaim,
		Status: current.Status(),
	})
	leased := true
	defer func() {
		if leased {
			r.releaseInline(current)
		}
	}()

	machine, exists := r.registry.Resolve(current.Operation())
	if !exists {
		return Snapshot{}, ErrMachineNotFound
	}
	for step := 0; step < budget.MaxTransitions; step++ {
		if inlineBudgetExhausted(inlineCtx) {
			current = r.loadInlineSnapshot(current)
			return current, nil
		}
		transition, advanceErr := r.advance(
			inlineCtx,
			machine,
			current,
		)
		if advanceErr != nil {
			if inlineBudgetExhausted(inlineCtx) {
				current = r.loadInlineSnapshot(current)
				return current, nil
			}
			if cause := context.Cause(inlineCtx); cause != nil {
				return Snapshot{}, cause
			}
			committed, handled, handleErr := r.commitMachineError(
				inlineCtx,
				current,
				advanceErr,
			)
			if handled {
				if handleErr != nil {
					return Snapshot{}, handleErr
				}
				current = committed
				leased = current.Status() == StatusRunning
				return current, nil
			}
			return Snapshot{}, fmt.Errorf(
				"continuation: advance machine inline: %w",
				advanceErr,
			)
		}
		next, applyErr := Apply(current, transition)
		if applyErr != nil {
			return Snapshot{}, applyErr
		}
		committed, commitErr := r.store.Transition(
			inlineCtx,
			r.commitRequest(current, next),
		)
		if commitErr != nil {
			if inlineBudgetExhausted(inlineCtx) {
				current = r.loadInlineSnapshot(current)
				return current, nil
			}
			return Snapshot{}, commitErr
		}
		current = committed
		r.observe(inlineCtx, Event{
			Kind:   EventTransition,
			Status: current.Status(),
		})
		if current.Status() != StatusRunning {
			leased = false
			return current, nil
		}
	}
	return current, nil
}

func inlineBudgetExhausted(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrInlineBudget)
}

func (r *Runtime) loadInlineSnapshot(fallback Snapshot) Snapshot {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		inlineCleanupTimeout,
	)
	defer cancel()
	current, err := r.store.Load(ctx, fallback.CallID())
	if err != nil {
		return fallback
	}
	return current
}

func (r *Runtime) releaseInline(snapshot Snapshot) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		inlineCleanupTimeout,
	)
	defer cancel()
	r.release(ctx, snapshot)
}
