// Package failure classifies transport-neutral invocation failures.
package failure

import (
	"context"
	"errors"
	"net"

	kerrors "github.com/keelab/keelith/errors"
)

// Kind is a stable, low-cardinality failure class.
type Kind string

const (
	// None represents a successful invocation.
	None Kind = "none"
	// Transport represents a network or adapter failure.
	Transport Kind = "transport"
	// Timeout represents a deadline or timeout failure.
	Timeout Kind = "timeout"
	// Business represents a framework application error.
	Business Kind = "business"
	// Canceled represents explicit context cancellation.
	Canceled Kind = "canceled"
	// Unknown represents a failure without a more specific classification.
	Unknown Kind = "unknown"
)

type transportError struct {
	cause error
}

func (failure *transportError) Error() string {
	return failure.cause.Error()
}

func (failure *transportError) Unwrap() error {
	return failure.cause
}

// MarkTransport marks an adapter/client failure as transport-level without
// changing its errors.Is/errors.As chain.
func MarkTransport(err error) error {
	if err == nil {
		return nil
	}
	var marked *transportError
	if errors.As(err, &marked) {
		return err
	}
	return &transportError{cause: err}
}

// Classify maps err to a transport-neutral failure class.
func Classify(err error) Kind {
	if err == nil {
		return None
	}
	var marked *transportError
	if errors.As(err, &marked) {
		return Transport
	}
	if errors.Is(err, context.Canceled) {
		return Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Timeout
	}
	var application *kerrors.Error
	if errors.As(err, &application) {
		return Business
	}
	var network net.Error
	if errors.As(err, &network) {
		if network.Timeout() {
			return Timeout
		}
		return Transport
	}
	return Unknown
}

// Retryable reports whether a failure may be repeated when the root context
// is still active and the method policy permits it.
func Retryable(err error) bool {
	switch Classify(err) {
	case Transport, Timeout:
		return true
	default:
		return false
	}
}
