// Package completion defines the shared, low-cardinality result contract used
// by request logs, metrics, and traces.
package completion

import (
	"errors"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/failure"
)

// Direction identifies the instrumented side of an invocation.
type Direction string

const (
	// DirectionServer identifies inbound server work.
	DirectionServer Direction = "server"
	// DirectionClient identifies outbound client work.
	DirectionClient Direction = "client"
)

// Outcome is the stable completion result shared by all observability signals.
type Outcome string

const (
	// OutcomeOK indicates successful completion.
	OutcomeOK Outcome = "ok"
	// OutcomeSlow indicates successful but slow completion.
	OutcomeSlow Outcome = "slow"
	// OutcomeRejected indicates rejected work.
	OutcomeRejected Outcome = "rejected"
	// OutcomeRateLimited indicates rate-limited work.
	OutcomeRateLimited Outcome = "rate_limited"
	// OutcomeCanceled indicates canceled work.
	OutcomeCanceled Outcome = "canceled"
	// OutcomeTimeout indicates timed-out work.
	OutcomeTimeout Outcome = "timeout"
	// OutcomeError indicates failed work.
	OutcomeError Outcome = "error"
)

// ErrorClass is a stable, bounded diagnostic category. It is suitable for
// logs and traces; request identifiers and error reasons remain unsuitable as
// metric labels.
type ErrorClass string

const (
	// ErrorClassClient identifies a client-side failure.
	ErrorClassClient ErrorClass = "client"
	// ErrorClassRateLimited identifies a rate limit rejection.
	ErrorClassRateLimited ErrorClass = "rate_limited"
	// ErrorClassCanceled identifies cancellation.
	ErrorClassCanceled ErrorClass = "canceled"
	// ErrorClassTimeout identifies a timeout.
	ErrorClassTimeout ErrorClass = "timeout"
	// ErrorClassServer identifies a server-side failure.
	ErrorClassServer ErrorClass = "server"
	// ErrorClassTransport identifies a transport failure.
	ErrorClassTransport ErrorClass = "transport"
	// ErrorClassInternal identifies an internal failure.
	ErrorClassInternal ErrorClass = "internal"
)

// Severity is the signal-neutral log severity recommendation.
type Severity string

const (
	// SeverityInfo is used for expected or successful outcomes.
	SeverityInfo Severity = "info"
	// SeverityWarn is used for recoverable or expected failures.
	SeverityWarn Severity = "warn"
	// SeverityError is used for unexpected failures.
	SeverityError Severity = "error"
)

// Result is an immutable classification of one terminal invocation result.
type Result struct {
	outcome     Outcome
	errorClass  ErrorClass
	severity    Severity
	failureKind failure.Kind
	serverError bool
	clientError bool
}

// Classify maps err into one shared completion result without inspecting error
// messages or metadata.
func Classify(err error) Result {
	kind := failure.Classify(err)
	if err == nil {
		return result(OutcomeOK, "", SeverityInfo, kind, false, false)
	}

	var applicationError *kerrors.Error
	if errors.As(err, &applicationError) {
		switch code := applicationError.Code(); {
		case code == 408:
			return result(OutcomeTimeout, ErrorClassTimeout, SeverityWarn, kind, false, true)
		case code == 429:
			return result(
				OutcomeRateLimited,
				ErrorClassRateLimited,
				SeverityWarn,
				kind,
				false,
				true,
			)
		case code == 499:
			return result(OutcomeCanceled, ErrorClassCanceled, SeverityInfo, kind, false, false)
		case code >= 400 && code < 500:
			return result(OutcomeRejected, ErrorClassClient, SeverityInfo, kind, false, true)
		case code == 504:
			return result(OutcomeTimeout, ErrorClassTimeout, SeverityWarn, kind, true, true)
		default:
			return result(OutcomeError, ErrorClassServer, SeverityError, kind, true, true)
		}
	}

	switch kind {
	case failure.Canceled:
		return result(OutcomeCanceled, ErrorClassCanceled, SeverityInfo, kind, false, false)
	case failure.Timeout:
		return result(OutcomeTimeout, ErrorClassTimeout, SeverityWarn, kind, true, true)
	case failure.Transport:
		return result(OutcomeError, ErrorClassTransport, SeverityError, kind, true, true)
	default:
		return result(OutcomeError, ErrorClassInternal, SeverityError, kind, true, true)
	}
}

func result(
	outcome Outcome,
	errorClass ErrorClass,
	severity Severity,
	kind failure.Kind,
	serverError bool,
	clientError bool,
) Result {
	return Result{
		outcome:     outcome,
		errorClass:  errorClass,
		severity:    severity,
		failureKind: kind,
		serverError: serverError,
		clientError: clientError,
	}
}

// Outcome returns the bounded terminal result.
func (result Result) Outcome() Outcome {
	return result.outcome
}

// ErrorClass returns the bounded diagnostic class or an empty value on success.
func (result Result) ErrorClass() ErrorClass {
	return result.errorClass
}

// Severity returns the recommended completion log severity.
func (result Result) Severity() Severity {
	return result.severity
}

// FailureKind returns the existing governance-oriented failure category.
func (result Result) FailureKind() failure.Kind {
	return result.failureKind
}

// CountsAsError reports whether the result contributes to the instrumented
// side's availability error signal. Expected inbound client rejections are not
// server errors, while the same response is an outbound client-call failure.
func (result Result) CountsAsError(direction Direction) bool {
	switch direction {
	case DirectionServer:
		return result.serverError
	case DirectionClient:
		return result.clientError
	default:
		return false
	}
}

// IsCancellation reports a terminal cancellation that should not be recorded
// as a span error on either side.
func (result Result) IsCancellation() bool {
	return result.outcome == OutcomeCanceled
}
