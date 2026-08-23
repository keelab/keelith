package fallback

import (
	"context"
	"errors"
	"fmt"

	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

var (
	// ErrInvalidInvocation reports an invalid typed fallback invocation.
	ErrInvalidInvocation = errors.New("fallback: invalid invocation")
	// ErrInvalidReplacement reports a resolver value that cannot safely be
	// returned as the invocation's declared response type.
	ErrInvalidReplacement = errors.New("fallback: invalid replacement")
)

// Invocation is one already-governed unary dependency call.
//
// The supplied context carries target as its operation identity. Implementations
// should pass it to the underlying HTTP, gRPC, Hertz, or Kitex client unchanged.
type Invocation[T any] func(context.Context) (T, error)

// Result makes a normal or degraded typed response explicit to application
// code. Cause is deliberately private so transport encoders cannot serialize an
// internal dependency error by accident.
type Result[T any] struct {
	Value    T
	Degraded bool
	Stale    bool

	cause error
}

// Cause returns the dependency failure replaced by a degraded value. It is nil
// for a normal response.
func (result Result[T]) Cause() error {
	return result.cause
}

// Runtime owns one concurrency-safe fallback policy. The configured Resolver
// and Recorder must be safe for concurrent use by dependency calls.
//
// Runtime intentionally stays outside client.Outbound: fallback is a typed,
// application-visible boundary after the final governed attempt, not a
// transparent transport middleware that can manufacture an empty gRPC reply.
type Runtime struct {
	middleware middleware.Middleware
}

// NewRuntime validates and compiles a reusable typed fallback runtime.
func NewRuntime(resolver Resolver, options ...Option) (*Runtime, error) {
	fallbackMiddleware, err := New(resolver, options...)
	if err != nil {
		return nil, err
	}
	return &Runtime{middleware: fallbackMiddleware}, nil
}

// Invoke runs one unary call and returns a type-safe, degradation-aware result.
//
// The invocation should already use Keelith's managed client so timeout,
// retry/hedging, breaker, bulkhead, and routing complete before fallback is
// considered. Business failures and cancellation are preserved. Resolver type
// mismatches and nil reference replacements fail closed while retaining the
// original dependency error in the returned error chain.
func Invoke[T any](
	ctx context.Context,
	runtime *Runtime,
	target operation.Operation,
	request any,
	invocation Invocation[T],
) (Result[T], error) {
	if ctx == nil {
		return Result[T]{}, fmt.Errorf("%w: context is nil", ErrInvalidInvocation)
	}
	if runtime == nil || runtime.middleware == nil {
		return Result[T]{}, fmt.Errorf("%w: runtime is nil", ErrInvalidInvocation)
	}
	if target.Transport() == "" || target.Service() == "" ||
		target.Method() == "" || target.Kind() != operation.KindUnary {
		return Result[T]{}, fmt.Errorf("%w: target must be a valid unary operation", ErrInvalidInvocation)
	}
	if invocation == nil {
		return Result[T]{}, fmt.Errorf("%w: invocation is nil", ErrInvalidInvocation)
	}

	handler := runtime.middleware(func(callContext context.Context, _ any) (any, error) {
		return invocation(callContext)
	})
	response, invocationErr := handler(
		operation.WithContext(ctx, target),
		request,
	)
	result, decodeErr := decodeResult[T](response)
	if decodeErr != nil {
		if invocationErr != nil {
			return result, errors.Join(invocationErr, decodeErr)
		}
		return result, decodeErr
	}
	return result, invocationErr
}

func decodeResult[T any](response any) (Result[T], error) {
	if response == nil {
		return Result[T]{}, nil
	}
	degraded, ok := response.(Response)
	if !ok {
		value, valid := response.(T)
		if !valid {
			return Result[T]{}, fmt.Errorf("%w: invocation returned %T", ErrInvalidInvocation, response)
		}
		return Result[T]{Value: value}, nil
	}

	result := Result[T]{
		Degraded: degraded.Degraded,
		Stale:    degraded.Stale,
		cause:    degraded.Cause(),
	}
	if !degraded.Degraded || degraded.Cause() == nil {
		return result, fmt.Errorf("%w: degraded response is incomplete", ErrInvalidReplacement)
	}
	if isNil(degraded.Value) {
		return result, errors.Join(
			degraded.Cause(),
			fmt.Errorf("%w: replacement is nil", ErrInvalidReplacement),
		)
	}
	value, valid := degraded.Value.(T)
	if !valid {
		return result, errors.Join(
			degraded.Cause(),
			fmt.Errorf("%w: replacement type %T does not match invocation result", ErrInvalidReplacement, degraded.Value),
		)
	}
	result.Value = value
	return result, nil
}
