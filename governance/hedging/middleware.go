// Package hedging provides bounded parallel attempts for idempotent methods.
package hedging

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/keelab/keelith/governance/attempt"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

var (
	// ErrInvalidOption reports an invalid hedging dependency or option.
	ErrInvalidOption = errors.New("hedging: invalid option")
	// ErrMissingOperation reports a call without a stable Operation.
	ErrMissingOperation = errors.New("hedging: operation is missing")
)

// Clock supplies hedge-delay timer channels.
type Clock interface {
	After(time.Duration) <-chan time.Time
}

// Option configures hedging Middleware.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (f optionFunc) apply(options *options) error {
	return f(options)
}

type options struct {
	clock Clock
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

// New creates policy-resolved hedging Middleware.
func New(resolver policy.Resolver, optionList ...Option) (middleware.Middleware, error) {
	if isNil(resolver) {
		return nil, fmt.Errorf("%w: resolver is nil", ErrInvalidOption)
	}
	settings := options{clock: realClock{}}

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
			resolved := policy.Resolve(ctx, resolver, target).Hedging
			if attempt.IsProbe(ctx) {
				return next(attempt.WithContext(ctx, 1), request)
			}
			if !resolved.Enabled ||
				!resolved.Idempotent ||
				resolved.MaxAttempts < 2 {
				return next(attempt.WithContext(ctx, 1), request)
			}
			if resolved.Delay <= 0 || resolved.MaxAttempts > 5 {
				return nil, fmt.Errorf("%w: resolved hedging policy is invalid", policy.ErrInvalidPolicy)
			}
			return invoke(ctx, request, next, resolved, settings.clock)
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

type result struct {
	response any
	err      error
}

func invoke(ctx context.Context, request any, next middleware.Handler, resolved policy.HedgingPolicy, clock Clock) (any, error) {
	callContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan result, resolved.MaxAttempts)
	launched := 0
	active := 0
	launch := func() {
		launched++
		active++
		number := launched
		go func() {
			response, err := next(
				attempt.WithContext(callContext, number),
				request,
			)
			results <- result{response: response, err: err}
		}()
	}
	launch()
	timer := clock.After(resolved.Delay)
	failures := make([]error, 0, resolved.MaxAttempts)

	for active > 0 {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case completed := <-results:
			active--
			if completed.err == nil {
				cancel()
				return completed.response, nil
			}
			if !failure.Retryable(completed.err) {
				cancel()
				return completed.response, completed.err
			}
			failures = append(failures, completed.err)
			if active == 0 && launched < resolved.MaxAttempts {
				launch()
			}
			if active == 0 && launched == resolved.MaxAttempts {
				return nil, errors.Join(failures...)
			}
		case <-timer:
			if launched < resolved.MaxAttempts {
				launch()
			}
			if launched < resolved.MaxAttempts {
				timer = clock.After(resolved.Delay)
			} else {
				timer = nil
			}
		}
	}
	return nil, errors.Join(failures...)
}

type realClock struct{}

func (realClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
