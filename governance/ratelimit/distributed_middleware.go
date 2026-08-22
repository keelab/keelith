package ratelimit

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// NewDistributedMiddleware creates transport-neutral shared quota
// Middleware.
func NewDistributedMiddleware(resolver policy.Resolver, limiter *DistributedLimiter) (middleware.Middleware, error) {
	if isNil(resolver) || limiter == nil {
		return nil, fmt.Errorf(
			"%w: resolver or distributed limiter is nil",
			ErrInvalidOption,
		)
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
				return nil, fmt.Errorf("%w: operation is missing", ErrInvalidOption)
			}
			config := policy.Resolve(ctx, resolver, target).RateLimit
			if !config.Enabled {
				return next(ctx, request)
			}
			permit, err := limiter.Acquire(ctx, target, request, config)
			if err != nil {
				return nil, err
			}
			defer permit.Release()
			return next(ctx, request)
		}
	}, nil
}
