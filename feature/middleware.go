package feature

import (
	"context"

	"github.com/keelab/keelith/middleware"
)

type storeContextKey struct{}

// WithStore attaches an instance-scoped evaluator to an invocation context.
// It is intended for framework composition; application code normally uses
// StoreFromContext or the typed FromContext helpers.
func WithStore(ctx context.Context, store *Store) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, storeContextKey{}, store)
}

// StoreFromContext returns the instance-scoped evaluator, when configured.
func StoreFromContext(ctx context.Context) (*Store, bool) {
	if ctx == nil {
		return nil, false
	}
	store, ok := ctx.Value(storeContextKey{}).(*Store)
	return store, ok && store != nil
}

// ContextMiddleware makes one Store available to HTTP, gRPC, and Worker
// handlers without a process-global evaluator.
func ContextMiddleware(store *Store) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			return next(WithStore(ctx, store), request)
		}
	}
}

// BooleanFromContext evaluates one Boolean flag or returns fallback when no
// evaluator is attached.
func BooleanFromContext(ctx context.Context, key string, fallback bool) (bool, Details) {
	store, _ := StoreFromContext(ctx)
	return store.Boolean(ctx, key, fallback)
}

// StringFromContext evaluates one String flag or returns fallback when no
// evaluator is attached.
func StringFromContext(ctx context.Context, key string, fallback string) (string, Details) {
	store, _ := StoreFromContext(ctx)
	return store.String(ctx, key, fallback)
}

// IntegerFromContext evaluates one Integer flag or returns fallback when no
// evaluator is attached.
func IntegerFromContext(ctx context.Context, key string, fallback int64) (int64, Details) {
	store, _ := StoreFromContext(ctx)
	return store.Integer(ctx, key, fallback)
}

// FloatFromContext evaluates one Float flag or returns fallback when no
// evaluator is attached.
func FloatFromContext(ctx context.Context, key string, fallback float64) (float64, Details) {
	store, _ := StoreFromContext(ctx)
	return store.Float(ctx, key, fallback)
}
