// Package attempt carries low-cardinality governance attempt metadata.
package attempt

import (
	"context"

	"github.com/keelab/keelith/operation"
)

type probeContextKey struct{}

// WithContext records a one-based invocation attempt.
func WithContext(ctx context.Context, number int) context.Context {
	return operation.WithAttempt(ctx, number)
}

// FromContext returns a one-based invocation attempt. Calls that have not
// entered retry or hedging middleware are reported as attempt one.
func FromContext(ctx context.Context) int {
	return operation.AttemptFromContext(ctx)
}

// WithProbe marks a circuit-breaker recovery probe.
//
// Attempt controllers must not amplify a probe through retries or hedging:
// one admitted half-open probe represents exactly one transport invocation.
func WithProbe(ctx context.Context) context.Context {
	return context.WithValue(ctx, probeContextKey{}, true)
}

// IsProbe reports whether the invocation is a circuit-breaker recovery probe.
func IsProbe(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	probe, _ := ctx.Value(probeContextKey{}).(bool)
	return probe
}
