// Package retry provides deadline-aware, budgeted full-jitter retries.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"

	"github.com/keelab/keelith/governance/attempt"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

var (
	// ErrInvalidOption reports an invalid retry dependency or option.
	ErrInvalidOption = errors.New("retry: invalid option")
	// ErrMissingOperation reports a call without a stable Operation.
	ErrMissingOperation = errors.New("retry: operation is missing")
)

// Clock supplies cancelable backoff timer channels.
type Clock interface {
	After(time.Duration) <-chan time.Time
}

// Random supplies a value in [0, limit) for full jitter.
type Random interface {
	Uint64N(limit uint64) uint64
}

// Option configures retry Middleware.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (f optionFunc) apply(options *options) error {
	return f(options)
}

type options struct {
	clock  Clock
	random Random
	budget *Budget
}

// WithClock replaces the production timer source.
func WithClock(clock Clock) Option {
	return optionFunc(func(options *options) error {
		if isNil(clock) {
			return fmt.Errorf("clock is nil")
		}
		options.clock = clock
		return nil
	})
}

// WithRandom replaces the production full-jitter source.
func WithRandom(random Random) Option {
	return optionFunc(func(options *options) error {
		if isNil(random) {
			return fmt.Errorf("random source is nil")
		}
		options.random = random
		return nil
	})
}

// WithBudget replaces the shared ratio budget.
func WithBudget(budget *Budget) Option {
	return optionFunc(func(options *options) error {
		if budget == nil {
			return fmt.Errorf("budget is nil")
		}
		options.budget = budget
		return nil
	})
}

// New creates policy-resolved retry Middleware.
func New(
	resolver policy.Resolver,
	optionList ...Option,
) (middleware.Middleware, error) {
	if isNil(resolver) {
		return nil, fmt.Errorf("%w: resolver is nil", ErrInvalidOption)
	}
	settings := options{
		clock:  realClock{},
		random: packageRandom{},
		budget: NewBudget(1),
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	return middlewareFor(resolver, settings), nil
}

func middlewareFor(resolver policy.Resolver, settings options) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		if next == nil {
			return func(context.Context, any) (any, error) {
				return nil, fmt.Errorf("%w: next handler is nil", ErrInvalidOption)
			}
		}
		return func(ctx context.Context, request any) (any, error) {
			if ctx == nil {
				return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
			}
			target, ok := operation.FromContext(ctx)
			if !ok {
				return nil, ErrMissingOperation
			}
			if isStreaming(target.Kind()) {
				return next(attempt.WithContext(ctx, 1), request)
			}
			resolved := policy.Resolve(ctx, resolver, target).Retry
			if attempt.IsProbe(ctx) {
				return next(attempt.WithContext(ctx, 1), request)
			}
			if !resolved.Enabled ||
				!resolved.Idempotent ||
				resolved.MaxAttempts < 2 {
				return next(attempt.WithContext(ctx, 1), request)
			}
			if err := validate(resolved); err != nil {
				return nil, err
			}

			key := target.PolicyKey()
			settings.budget.begin(key)
			var lastErr error
			for number := 1; number <= resolved.MaxAttempts; number++ {
				if cause := context.Cause(ctx); cause != nil {
					return nil, cause
				}
				response, err := next(attempt.WithContext(ctx, number), request)
				if err == nil {
					return response, nil
				}
				lastErr = err
				if !failure.Retryable(err) || number == resolved.MaxAttempts {
					return response, err
				}
				if cause := context.Cause(ctx); cause != nil {
					return nil, cause
				}
				if !settings.budget.take(key, resolved.BudgetRatio) {
					return response, err
				}
				delay := fullJitter(
					resolved,
					number+1,
					settings.random,
				)
				select {
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				case <-settings.clock.After(delay):
				}
			}
			return nil, lastErr
		}
	}
}

func isStreaming(kind operation.Kind) bool {
	switch kind {
	case operation.KindClientStream,
		operation.KindServerStream,
		operation.KindBidiStream:
		return true
	default:
		return false
	}
}

func validate(value policy.RetryPolicy) error {
	if value.MaxAttempts < 2 ||
		value.BackoffMin <= 0 ||
		value.BackoffMax < value.BackoffMin ||
		value.BudgetRatio <= 0 ||
		value.BudgetRatio > 1 {
		return fmt.Errorf("%w: resolved retry policy is invalid", policy.ErrInvalidPolicy)
	}
	return nil
}

func fullJitter(value policy.RetryPolicy, nextAttempt int, random Random) time.Duration {
	limit := value.BackoffMin
	for exponent := 2; exponent < nextAttempt; exponent++ {
		if limit >= value.BackoffMax/2 {
			limit = value.BackoffMax
			break
		}
		limit *= 2
	}
	if limit > value.BackoffMax {
		limit = value.BackoffMax
	}
	return time.Duration(random.Uint64N(uint64(limit) + 1))
}

type realClock struct{}

func (realClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

type packageRandom struct{}

func (packageRandom) Uint64N(limit uint64) uint64 {
	return rand.Uint64N(limit)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
