// Package fallback provides explicit, transport-neutral degraded responses.
package fallback

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

var (
	// ErrInvalidOption reports an invalid fallback dependency or option.
	ErrInvalidOption = errors.New("fallback: invalid option")
	// ErrMissingOperation reports a failed call without an operation identity.
	ErrMissingOperation = errors.New("fallback: operation is missing")
)

// Replacement is an explicitly resolved degraded value.
type Replacement struct {
	Value any
	Found bool
	Stale bool
}

// Response makes degradation visible to application and transport adapters.
//
// Cause is intentionally exposed through a method instead of an exported
// field so generic encoders cannot accidentally serialize internal errors.
type Response struct {
	Value    any
	Degraded bool
	Stale    bool

	cause error
}

// Cause returns the original invocation error.
func (response Response) Cause() error {
	return response.cause
}

// Resolver obtains an operation-specific replacement after a failed call.
type Resolver interface {
	Resolve(context.Context, operation.Operation, any, error) (Replacement, error)
}

// ResolverFunc adapts a function to Resolver.
type ResolverFunc func(context.Context, operation.Operation, any, error) (Replacement, error)

// Resolve implements Resolver.
func (f ResolverFunc) Resolve(ctx context.Context, target operation.Operation, request any, cause error) (Replacement, error) {
	return f(ctx, target, request, cause)
}

// Classifier decides which invocation failures may use a fallback.
type Classifier func(error) bool

// Event is a low-cardinality fallback decision for instrumentation.
type Event struct {
	Operation operation.Operation
	Stale     bool
	Cause     error
}

// Recorder observes successful fallback decisions.
type Recorder interface {
	Record(Event)
}

// Option configures fallback Middleware.
type Option interface {
	apply(*settings) error
}

type optionFunc func(*settings) error

func (function optionFunc) apply(settings *settings) error {
	return function(settings)
}

type settings struct {
	classify Classifier
	recorder Recorder
}

// WithClassifier replaces the default transport/timeout classifier.
func WithClassifier(classifier Classifier) Option {
	return optionFunc(func(settings *settings) error {
		if classifier == nil {
			return fmt.Errorf("classifier is nil")
		}
		settings.classify = classifier
		return nil
	})
}

// WithRecorder observes successful degraded responses.
func WithRecorder(recorder Recorder) Option {
	return optionFunc(func(settings *settings) error {
		if isNil(recorder) {
			return fmt.Errorf("recorder is nil")
		}
		settings.recorder = recorder
		return nil
	})
}

// New creates explicit fallback Middleware.
func New(resolver Resolver, options ...Option) (middleware.Middleware, error) {
	if isNil(resolver) {
		return nil, fmt.Errorf("%w: resolver is nil", ErrInvalidOption)
	}
	settings := settings{classify: failure.Retryable}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	return middlewareFor(resolver, settings), nil
}

func middlewareFor(resolver Resolver, settings settings) middleware.Middleware {
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
			response, err := next(ctx, request)
			if err == nil || !settings.classify(err) {
				return response, err
			}
			if errors.Is(err, context.Canceled) {
				return response, err
			}
			target, ok := operation.FromContext(ctx)
			if !ok {
				return response, errors.Join(err, ErrMissingOperation)
			}
			replacement, resolveErr := resolver.Resolve(
				ctx,
				target,
				request,
				err,
			)
			if resolveErr != nil {
				return response, errors.Join(err, fmt.Errorf("fallback resolver: %w", resolveErr))
			}
			if !replacement.Found {
				return response, err
			}
			if settings.recorder != nil {
				settings.recorder.Record(Event{
					Operation: target,
					Stale:     replacement.Stale,
					Cause:     err,
				})
			}
			return Response{
				Value:    replacement.Value,
				Degraded: true,
				Stale:    replacement.Stale,
				cause:    err,
			}, nil
		}
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
