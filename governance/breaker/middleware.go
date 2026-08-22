package breaker

import (
	"context"
	"errors"
	"fmt"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/attempt"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// ErrMissingInstance reports an instance breaker without a selected instance.
var ErrMissingInstance = kerrors.New(
	500,
	"BREAKER_INSTANCE_MISSING",
	"instance breaker requires a selected instance",
)

// NewService creates the service-level dependency breaker.
//
// Place it outside retry or hedging middleware so one logical dependency call
// contributes exactly one final outcome to aggregate service health.
func NewService(resolver policy.Resolver, pool *Pool) (middleware.Middleware, error) {
	return New(resolver, pool, ScopeService)
}

// NewInstance creates an instance-scoped breaker for advanced adapters that
// attach a selected instance with WithInstance before entering middleware.
//
// Selector-based clients should normally use outlier.Detector instead because
// it observes every selected attempt and can exclude unhealthy nodes before
// transport dispatch.
func NewInstance(resolver policy.Resolver, pool *Pool) (middleware.Middleware, error) {
	return New(resolver, pool, ScopeInstance)
}

// New creates service- or instance-scoped breaker Middleware.
func New(resolver policy.Resolver, pool *Pool, scope Scope) (middleware.Middleware, error) {
	if isNil(resolver) || pool == nil {
		return nil, fmt.Errorf("%w: resolver or pool is nil", ErrInvalidOption)
	}
	if scope != ScopeService && scope != ScopeInstance {
		return nil, fmt.Errorf("%w: scope %q", ErrInvalidOption, scope)
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
				return nil, errors.New("breaker: operation is missing")
			}
			config := policy.Resolve(ctx, resolver, target).Breaker
			if !config.Enabled {
				return next(ctx, request)
			}
			key := target.Transport() + "/" + target.Service()
			if scope == ScopeInstance {
				var exists bool
				key, exists = instanceFromContext(ctx)
				if !exists {
					return nil, ErrMissingInstance
				}
			}
			permit, err := pool.Get(scope, key).Allow(config)
			if err != nil {
				return nil, err
			}
			invokeContext := ctx
			if permit.Probe() {
				invokeContext = attempt.WithProbe(ctx)
			}
			var response any
			var resultErr error

			defer func() {
				recovered := recover()
				if recovered != nil {
					permit.Done(failure.MarkTransport(
						errors.New("breaker: downstream invocation panic"),
					))
					panic(recovered)
				}
				permit.Done(resultErr)
			}()
			response, resultErr = next(invokeContext, request)
			return response, resultErr
		}
	}, nil
}
