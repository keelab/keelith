// Package health provides instance-scoped lifecycle and health reporting.
package health

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalid means a health kind, contributor name, or checker is invalid.
	ErrInvalid = errors.New("health: invalid contributor")
	// ErrDuplicate means a contributor name is already registered for a kind.
	ErrDuplicate = errors.New("health: duplicate contributor")
	// ErrLimit means one health kind reached its bounded contributor capacity.
	ErrLimit = errors.New("health: contributor limit exceeded")
)

// Kind identifies an independently evaluated health signal.
type Kind string

const (
	// KindStartup reports whether application startup has ever completed.
	KindStartup Kind = "startup"
	// KindLiveness reports whether the application can continue operating.
	KindLiveness Kind = "liveness"
	// KindReadiness reports whether the application should receive new work.
	KindReadiness Kind = "readiness"
	// KindDependency reports explicitly registered dependency checks.
	KindDependency Kind = "dependency"
)

// Status is the normalized outcome of a health check.
type Status string

const (
	// StatusPass means a check succeeded.
	StatusPass Status = "pass"
	// StatusFail means a check failed.
	StatusFail Status = "fail"
	// StatusUnknown means a check could not determine health.
	StatusUnknown Status = "unknown"
)

// Phase is the lifecycle phase observed by a Registry.
type Phase uint8

const (
	// PhaseNew means application startup has not begun.
	PhaseNew Phase = iota
	// PhaseStarting means application components are starting.
	PhaseStarting
	// PhaseReady means application startup completed successfully.
	PhaseReady
	// PhaseDraining means readiness is disabled and components are stopping.
	PhaseDraining
	// PhaseStopped means application shutdown completed.
	PhaseStopped
	// PhaseFailed means application startup, runtime, or shutdown failed.
	PhaseFailed
)

// Result is returned by a Checker.
type Result struct {
	Status    Status    `json:"status"`
	Reason    string    `json:"reason"`
	CheckedAt time.Time `json:"checked_at"`
}

// CheckResult attaches a stable contributor name to a Result.
type CheckResult struct {
	Name      string    `json:"name"`
	Status    Status    `json:"status"`
	Reason    string    `json:"reason"`
	CheckedAt time.Time `json:"checked_at"`
}

// Report aggregates one health Kind and all its contributors.
type Report struct {
	Kind      Kind          `json:"kind"`
	Status    Status        `json:"status"`
	Reason    string        `json:"reason"`
	CheckedAt time.Time     `json:"checked_at"`
	Checks    []CheckResult `json:"checks"`
}

// Checker evaluates one named health contribution.
type Checker func(context.Context) Result

// Pass constructs a successful Result.
func Pass(reason string) Result {
	return Result{Status: StatusPass, Reason: reason}
}

// Fail constructs a failed Result.
func Fail(reason string) Result {
	return Result{Status: StatusFail, Reason: reason}
}

// Unknown constructs an indeterminate Result.
func Unknown(reason string) Result {
	return Result{Status: StatusUnknown, Reason: reason}
}
