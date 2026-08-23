package di

import (
	"context"
	"fmt"
)

type requestKey[T any] struct{ name string }

// RequestKey identifies one explicitly context-bound request dependency.
type RequestKey[T any] struct{ key requestKey[T] }

// NewRequestKey creates a request key. It is not a container binding and can
// only be transported through context.Context by protocol middleware.
func NewRequestKey[T any](name string) (RequestKey[T], error) {
	if name == "" {
		return RequestKey[T]{}, fmt.Errorf("%w: request key is empty", ErrInvalidModule)
	}
	return RequestKey[T]{key: requestKey[T]{name: name}}, nil
}

// WithRequestValue attaches one typed request dependency.
func WithRequestValue[T any](ctx context.Context, key RequestKey[T], value T) context.Context {
	return context.WithValue(ctx, key.key, value)
}

// RequestValue returns one typed request dependency.
func RequestValue[T any](ctx context.Context, key RequestKey[T]) (T, bool) {
	var zero T
	if ctx == nil {
		return zero, false
	}
	value, ok := ctx.Value(key.key).(T)
	return value, ok
}
