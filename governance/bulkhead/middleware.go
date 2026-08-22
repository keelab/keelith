package bulkhead

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// New creates dependency-scoped logical-call concurrency middleware.
func New(resolver policy.Resolver, pool *Pool) (middleware.Middleware, error) {
	if isNil(resolver) || pool == nil {
		return nil, fmt.Errorf("%w: resolver or pool is nil", ErrInvalidOption)
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
			config := policy.Resolve(ctx, resolver, target).Bulkhead
			if !config.Enabled {
				return next(ctx, request)
			}
			key := target.Transport() + "/" + target.Service()
			permit, err := pool.Get(key).Acquire(ctx, config)
			if err != nil {
				return nil, err
			}
			defer permit.Release()
			return next(ctx, request)
		}
	}, nil
}
