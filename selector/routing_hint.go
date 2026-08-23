package selector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/operation"
)

const maxRoutingHintBytes = 1024

var (
	// ErrRoutingHint means an affinity hint is missing or malformed.
	ErrRoutingHint = errors.New("selector: invalid routing hint")
)

// RoutingHint is an immutable, request-scoped best-effort affinity key.
//
// A hint can have high cardinality and may contain tenant-derived data. It
// must never be used as a metric label or emitted by framework diagnostics.
type RoutingHint struct {
	key string
}

// NewRoutingHint validates and constructs an explicit affinity hint.
func NewRoutingHint(key string) (RoutingHint, error) {
	if key == "" ||
		len(key) > maxRoutingHintBytes ||
		!utf8.ValidString(key) ||
		strings.TrimSpace(key) != key {
		return RoutingHint{}, fmt.Errorf(
			"%w: key is empty, oversized, or malformed",
			ErrRoutingHint,
		)
	}
	for _, character := range key {
		if unicode.IsControl(character) {
			return RoutingHint{}, fmt.Errorf(
				"%w: key contains a control character",
				ErrRoutingHint,
			)
		}
	}
	return RoutingHint{key: key}, nil
}

// Key returns the opaque affinity key.
//
// Callers must not copy the value into logs, traces, metrics, or diagnostics.
func (hint RoutingHint) Key() string {
	return hint.key
}

type routingHintContextKey struct{}

// WithRoutingHint attaches a validated affinity hint to ctx.
func WithRoutingHint(
	ctx context.Context,
	hint RoutingHint,
) context.Context {
	return context.WithValue(ctx, routingHintContextKey{}, hint)
}

// RoutingHintFromContext returns a valid request-scoped affinity hint.
func RoutingHintFromContext(ctx context.Context) (RoutingHint, bool) {
	if ctx == nil {
		return RoutingHint{}, false
	}
	hint, ok := ctx.Value(routingHintContextKey{}).(RoutingHint)
	return hint, ok && hint.key != ""
}

// RoutingHintHashKey adapts RoutingHint to Rendezvous HashKey.
func RoutingHintHashKey(
	ctx context.Context,
	_ operation.Operation,
) (string, error) {
	hint, ok := RoutingHintFromContext(ctx)
	if !ok {
		return "", fmt.Errorf("%w: hint is missing", ErrRoutingHint)
	}
	return hint.key, nil
}
