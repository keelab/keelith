// Package timeout applies Method Policy deadlines.
package timeout

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

var (
	// ErrInvalidOption reports an invalid timeout dependency.
	ErrInvalidOption = errors.New("timeout: invalid option")
	// ErrMissingOperation reports a call without a stable Operation.
	ErrMissingOperation = errors.New("timeout: operation is missing")
)

// New creates policy-resolved timeout Middleware.
func New(resolver policy.Resolver) (middleware.Middleware, error) {
	if isNilResolver(resolver) {
		return nil, fmt.Errorf("%w: resolver is nil", ErrInvalidOption)
	}
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
			duration := policy.Resolve(ctx, resolver, target).Timeout
			if duration <= 0 {
				return nil, fmt.Errorf("%w: resolved timeout must be positive", policy.ErrInvalidPolicy)
			}

			callContext, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			response, err := next(callContext, request)
			if err != nil {
				return response, err
			}
			select {
			case <-callContext.Done():
				return nil, context.Cause(callContext)
			default:
				return response, nil
			}
		}
	}, nil
}

func isNilResolver(resolver policy.Resolver) bool {
	if resolver == nil {
		return true
	}
	value := reflect.ValueOf(resolver)
	return value.Kind() == reflect.Pointer && value.IsNil()
}
