// Package correlation defines validated, transport-neutral request correlation
// identities. It never exposes arbitrary metadata to observability sinks.
package correlation

import (
	"context"
	"errors"
	"fmt"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
)

const (
	// RequestIDMetadataKey is the one explicitly supported inbound correlation
	// key. Other metadata remains unavailable to completion logging.
	RequestIDMetadataKey = "x-request-id"

	// MaxRequestIDBytes bounds log and trace storage for one request identity.
	MaxRequestIDBytes = 128
)

// ErrInvalidRequestID reports an empty, oversized, ambiguous, or malformed ID.
var ErrInvalidRequestID = errors.New("correlation: invalid request id")

type requestIDContextKey struct{}

// ParseRequestID validates one stable ASCII request identity. Padding and
// control characters are rejected instead of normalized to avoid ambiguity.
func ParseRequestID(value string) (string, error) {
	if value == "" || len(value) > MaxRequestIDBytes {
		return "", fmt.Errorf("%w: value is empty or oversized", ErrInvalidRequestID)
	}
	for _, r := range value {
		valid := r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '-' ||
			r == '_' ||
			r == '.' ||
			r == ':'
		if !valid {
			return "", fmt.Errorf("%w: value contains an unsupported character", ErrInvalidRequestID)
		}
	}
	return value, nil
}

// RequestID returns the validated typed identity when middleware published it.
// Before policy middleware runs, it may read only the explicitly supported
// inbound metadata key and applies the same validation contract.
func RequestID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	if value, ok := ctx.Value(requestIDContextKey{}).(string); ok {
		return value, value != ""
	}
	inbound, ok := metadata.Inbound(ctx)
	if !ok {
		return "", false
	}
	values := inbound.Values(RequestIDMetadataKey)
	if len(values) != 1 {
		return "", false
	}
	value, err := ParseRequestID(values[0])
	return value, err == nil
}

// RequireRequestID rejects calls without exactly one validated request ID and
// publishes the typed value to downstream handlers.
func RequireRequestID() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			inbound, ok := metadata.Inbound(ctx)
			if !ok {
				return nil, requiredRequestIDError()
			}
			values := inbound.Values(RequestIDMetadataKey)
			if len(values) == 0 {
				return nil, requiredRequestIDError()
			}
			if len(values) != 1 {
				return nil, invalidRequestIDError()
			}
			requestID, err := ParseRequestID(values[0])
			if err != nil {
				return nil, invalidRequestIDError()
			}
			return next(
				context.WithValue(ctx, requestIDContextKey{}, requestID),
				request,
			)
		}
	}
}

func requiredRequestIDError() error {
	return kerrors.New(
		400,
		"REQUEST_ID_REQUIRED",
		"x-request-id metadata is required",
	)
}

func invalidRequestIDError() error {
	return kerrors.New(
		400,
		"REQUEST_ID_INVALID",
		"x-request-id metadata is invalid",
	)
}
