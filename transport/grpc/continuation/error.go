package continuationgrpc

import (
	"context"
	"errors"
	"time"

	continuationv1 "github.com/keelab/keelith/api/continuation/v1"
	"github.com/keelab/keelith/programmable/continuation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FailureFromError returns the structured continuation detail attached to a
// gRPC status without exposing its human-readable message.
func FailureFromError(err error) (*continuationv1.FailureDetail, bool) {
	if err == nil {
		return nil, false
	}
	for _, detail := range status.Convert(err).Details() {
		if failure, ok := detail.(*continuationv1.FailureDetail); ok {
			return failure, true
		}
	}
	return nil, false
}

func continuationStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	var gap *continuation.GapError
	if errors.As(err, &gap) {
		return statusWithFailure(
			codes.OutOfRange,
			"continuation history is no longer retained",
			&continuationv1.FailureDetail{
				Kind:              continuationv1.FailureKind_FAILURE_KIND_GAP,
				RequestedSequence: gap.After,
				FloorSequence:     gap.Floor,
				StableReason:      "RETENTION_GAP",
			},
		)
	}
	var cursor *continuation.CursorError
	if errors.As(err, &cursor) {
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation cursor is ahead of durable state",
			&continuationv1.FailureDetail{
				Kind:              continuationv1.FailureKind_FAILURE_KIND_CURSOR,
				RequestedSequence: cursor.After,
				CurrentSequence:   cursor.Current,
				StableReason:      "CURSOR_AHEAD",
			},
		)
	}
	var timer *continuation.TimerNotReadyError
	if errors.As(err, &timer) {
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation timer is not ready",
			&continuationv1.FailureDetail{
				Kind:         continuationv1.FailureKind_FAILURE_KIND_TIMER,
				Retryable:    true,
				StableReason: "TIMER_NOT_READY",
				ReadyAt:      timer.ReadyAt.UTC().Format(time.RFC3339Nano),
			},
		)
	}
	switch {
	case errors.Is(err, continuation.ErrTimerNotReady):
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation timer is not ready",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_TIMER,
				true,
				"TIMER_NOT_READY",
			),
		)
	case errors.Is(err, continuation.ErrHistoryBudget):
		return messageBudgetStatus()
	case errors.Is(err, continuation.ErrAuthenticationRequired):
		return statusWithFailure(
			codes.Unauthenticated,
			"continuation authentication is required",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_AUTHORIZATION,
				false,
				"AUTHENTICATION_REQUIRED",
			),
		)
	case errors.Is(err, continuation.ErrAccessDenied),
		errors.Is(err, continuation.ErrOwnershipConflict):
		return statusWithFailure(
			codes.PermissionDenied,
			"continuation access is denied",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_AUTHORIZATION,
				false,
				"ACCESS_DENIED",
			),
		)
	case errors.Is(err, continuation.ErrAuthorizationFailed):
		return statusWithFailure(
			codes.Internal,
			"continuation authorization is unavailable",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_AUTHORIZATION,
				false,
				"AUTHORIZATION_FAILED",
			),
		)
	case errors.Is(err, continuation.ErrGap):
		return statusWithFailure(
			codes.OutOfRange,
			"continuation history is no longer retained",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_GAP,
				false,
				"RETENTION_GAP",
			),
		)
	case errors.Is(err, continuation.ErrCursorAhead):
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation cursor is ahead of durable state",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_CURSOR,
				false,
				"CURSOR_AHEAD",
			),
		)
	case errors.Is(err, continuation.ErrLeaseHeld):
		return leaseStatus(codes.Aborted, "LEASE_HELD")
	case errors.Is(err, continuation.ErrLeaseLost):
		return leaseStatus(codes.Aborted, "LEASE_LOST")
	case errors.Is(err, continuation.ErrStaleFence):
		return leaseStatus(codes.Aborted, "STALE_FENCE")
	case errors.Is(err, continuation.ErrNotReady):
		return leaseStatus(codes.FailedPrecondition, "CALL_NOT_READY")
	case errors.Is(err, continuation.ErrLeaseUnsupported):
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation lease capability is unavailable",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_LEASE,
				false,
				"LEASE_UNSUPPORTED",
			),
		)
	case errors.Is(err, continuation.ErrMachineNotFound):
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation operation schema is unavailable",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_SCHEMA,
				false,
				"MACHINE_NOT_FOUND",
			),
		)
	case errors.Is(err, continuation.ErrWorkflowDefinitionMismatch),
		errors.Is(err, continuation.ErrWorkflowHandlerNotFound):
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation workflow schema is unavailable",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_SCHEMA,
				false,
				"WORKFLOW_SCHEMA_MISMATCH",
			),
		)
	case errors.Is(err, continuation.ErrTerminal):
		return statusWithFailure(
			codes.FailedPrecondition,
			"continuation call is terminal",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_TERMINAL,
				false,
				"CALL_TERMINAL",
			),
		)
	case errors.Is(err, continuation.ErrNotFound),
		errors.Is(err, continuation.ErrOwnershipNotFound):
		return statusWithFailure(
			codes.NotFound,
			"continuation call was not found",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_NOT_FOUND,
				false,
				"CALL_NOT_FOUND",
			),
		)
	case errors.Is(err, continuation.ErrAlreadyExists):
		return statusWithFailure(
			codes.AlreadyExists,
			"continuation call already exists",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_CONFLICT,
				false,
				"CALL_ALREADY_EXISTS",
			),
		)
	case errors.Is(err, continuation.ErrConflict),
		errors.Is(err, continuation.ErrCommandConflict),
		errors.Is(err, continuation.ErrTransition):
		return statusWithFailure(
			codes.Aborted,
			"continuation state conflicts with the request",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_CONFLICT,
				true,
				"STATE_CONFLICT",
			),
		)
	case errors.Is(err, ErrInvalidWireMessage),
		errors.Is(err, continuation.ErrInvalidIdentity),
		errors.Is(err, continuation.ErrInvalidFrame),
		errors.Is(err, continuation.ErrInvalidAttach),
		errors.Is(err, continuation.ErrInvalidInlineBudget),
		errors.Is(err, continuation.ErrInvalidHistory),
		errors.Is(err, continuation.ErrInvalidWorkflow),
		errors.Is(err, continuation.ErrWorkflowCycle):
		return invalidRequestStatus("INVALID_REQUEST")
	case errors.Is(err, continuation.ErrInvalidService),
		errors.Is(err, continuation.ErrInvalidRuntime),
		errors.Is(err, continuation.ErrInvalidStore):
		return statusWithFailure(
			codes.Internal,
			"continuation service is unavailable",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_UNAVAILABLE,
				false,
				"SERVICE_UNAVAILABLE",
			),
		)
	default:
		return statusWithFailure(
			codes.Unavailable,
			"continuation backend is unavailable",
			failureDetail(
				continuationv1.FailureKind_FAILURE_KIND_UNAVAILABLE,
				true,
				"BACKEND_UNAVAILABLE",
			),
		)
	}
}

func invalidRequestStatus(reason string) error {
	return statusWithFailure(
		codes.InvalidArgument,
		"continuation request is invalid",
		failureDetail(
			continuationv1.FailureKind_FAILURE_KIND_INVALID_REQUEST,
			false,
			reason,
		),
	)
}

func messageBudgetStatus() error {
	return statusWithFailure(
		codes.ResourceExhausted,
		"continuation message exceeds configured budget",
		failureDetail(
			continuationv1.FailureKind_FAILURE_KIND_INVALID_REQUEST,
			false,
			"MESSAGE_BUDGET_EXCEEDED",
		),
	)
}

func invalidBackendStatus() error {
	return statusWithFailure(
		codes.Internal,
		"continuation backend emitted invalid state",
		failureDetail(
			continuationv1.FailureKind_FAILURE_KIND_UNAVAILABLE,
			false,
			"INVALID_BACKEND_STATE",
		),
	)
}

func leaseStatus(code codes.Code, reason string) error {
	return statusWithFailure(
		code,
		"continuation execution lease changed",
		failureDetail(
			continuationv1.FailureKind_FAILURE_KIND_LEASE,
			true,
			reason,
		),
	)
}

func failureDetail(
	kind continuationv1.FailureKind,
	retryable bool,
	reason string,
) *continuationv1.FailureDetail {
	return &continuationv1.FailureDetail{
		Kind:         kind,
		Retryable:    retryable,
		StableReason: reason,
	}
}

func statusWithFailure(
	code codes.Code,
	message string,
	detail *continuationv1.FailureDetail,
) error {
	base := status.New(code, message)
	withDetails, err := base.WithDetails(detail)
	if err != nil {
		return base.Err()
	}
	return withDetails.Err()
}
