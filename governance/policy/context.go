package policy

import (
	"context"

	"github.com/keelab/keelith/operation"
)

type resolvedContextKey struct{}

type resolvedContextValue struct {
	key   string
	value Policy
}

// WithResolved records one immutable policy snapshot for an invocation.
//
// The operation key is stored with the policy so a nested call cannot
// accidentally reuse its parent's resolved policy.
func WithResolved(ctx context.Context, target operation.Operation, value Policy) context.Context {
	return context.WithValue(ctx, resolvedContextKey{}, resolvedContextValue{
		key:   target.PolicyKey(),
		value: value,
	})
}

// FromContext returns a matching invocation policy snapshot.
func FromContext(ctx context.Context, target operation.Operation) (Policy, bool) {
	if ctx == nil {
		return Policy{}, false
	}
	resolved, ok := ctx.Value(resolvedContextKey{}).(resolvedContextValue)
	if !ok || resolved.key != target.PolicyKey() {
		return Policy{}, false
	}
	return resolved.value, true
}

// Resolve returns the invocation snapshot when present, otherwise it consults
// resolver. Middleware that participates in one governance chain therefore
// observes one coherent policy revision.
func Resolve(
	ctx context.Context,
	resolver Resolver,
	target operation.Operation,
) Policy {
	if resolved, ok := FromContext(ctx, target); ok {
		return resolved
	}
	return resolver.Resolve(target)
}
