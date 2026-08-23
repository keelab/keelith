// Package saga provides durable, lease-fenced orchestration and compensation.
package saga

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxIdentityBytes = 256
	maxSteps         = 128
)

var (
	// ErrInvalidOption reports an invalid definition, instance, or dependency.
	ErrInvalidOption = errors.New("saga: invalid option")
	// ErrNotFound reports a missing durable saga instance.
	ErrNotFound = errors.New("saga: instance not found")
	// ErrAlreadyExists reports duplicate instance creation.
	ErrAlreadyExists = errors.New("saga: instance already exists")
	// ErrConflict reports a stale record revision.
	ErrConflict = errors.New("saga: revision conflict")
	// ErrStaleFence reports a write from an expired owner.
	ErrStaleFence = errors.New("saga: stale fence")
	// ErrContended reports that another owner currently executes the instance.
	ErrContended = errors.New("saga: instance is owned")
	// ErrDefinitionMismatch reports reuse of an ID by another definition.
	ErrDefinitionMismatch = errors.New("saga: definition mismatch")
	// ErrCompensated reports a terminal saga whose completed actions were undone.
	ErrCompensated = errors.New("saga: compensated")
	// ErrFailed reports terminal compensation failure.
	ErrFailed       = errors.New("saga: terminal failure")
	errHandlerPanic = errors.New("saga: handler panicked")
)

// Status is the durable orchestration state.
type Status string

const (
	// StatusRunning indicates forward execution is active.
	StatusRunning Status = "running"
	// StatusCompensating indicates reverse compensation is active.
	StatusCompensating Status = "compensating"
	// StatusCompleted indicates all forward actions completed.
	StatusCompleted Status = "completed"
	// StatusCompensated indicates completed actions were compensated.
	StatusCompensated Status = "compensated"
	// StatusFailed indicates compensation failed terminally.
	StatusFailed Status = "failed"
)

// Phase identifies a forward action or reverse compensation.
type Phase string

const (
	// PhaseAction identifies forward execution.
	PhaseAction Phase = "action"
	// PhaseCompensation identifies reverse compensation.
	PhaseCompensation Phase = "compensation"
)

// Invocation is a payload-free, stable handler identity.
//
// IdempotencyKey remains stable across retries and ownership changes. Business
// effects must enforce that key in their own transactional boundary.
type Invocation struct {
	SagaID         string
	Definition     string
	Version        string
	Step           string
	StepIndex      int
	Phase          Phase
	Attempt        int
	Fence          uint64
	IdempotencyKey string
}

// Handler performs one idempotent action or compensation.
type Handler func(context.Context, Invocation) error

// Step defines one forward action and its optional compensation.
type Step struct {
	Name       string
	Action     Handler
	Compensate Handler
}

// Definition is an immutable, versioned sequence of steps.
type Definition struct {
	Name    string
	Version string
	Steps   []Step
}

// Validate rejects ambiguous or unbounded definitions.
func (definition Definition) Validate() error {
	if !validIdentity(definition.Name, maxIdentityBytes) ||
		!validIdentity(definition.Version, maxIdentityBytes) ||
		len(definition.Steps) == 0 ||
		len(definition.Steps) > maxSteps {
		return fmt.Errorf("%w: definition identity or step count", ErrInvalidOption)
	}
	seen := make(map[string]struct{}, len(definition.Steps))
	for index, step := range definition.Steps {
		if !validIdentity(step.Name, maxIdentityBytes) || step.Action == nil {
			return fmt.Errorf("%w: step %d", ErrInvalidOption, index)
		}
		if _, duplicate := seen[step.Name]; duplicate {
			return fmt.Errorf(
				"%w: duplicate step %q",
				ErrInvalidOption,
				step.Name,
			)
		}
		seen[step.Name] = struct{}{}
	}
	return nil
}

// Record is the complete durable control state. It never contains business
// payloads, credentials, or raw error text.
type Record struct {
	ID                string
	Definition        string
	Version           string
	Status            Status
	NextStep          int
	CompensationIndex int
	Attempt           int
	CauseReason       string
	FailureReason     string
	Revision          uint64
	UpdatedAt         time.Time
}

// Terminal reports whether no automatic transition remains.
func (record Record) Terminal() bool {
	return record.Status == StatusCompleted ||
		record.Status == StatusCompensated ||
		record.Status == StatusFailed
}

// Validate checks durable state invariants independent of one Definition.
func (record Record) Validate() error {
	if !validIdentity(record.ID, maxIdentityBytes) ||
		!validIdentity(record.Definition, maxIdentityBytes) ||
		!validIdentity(record.Version, maxIdentityBytes) ||
		record.Revision == 0 ||
		record.NextStep < 0 ||
		record.CompensationIndex < -1 ||
		record.Attempt < 0 ||
		(record.CauseReason != "" &&
			!validIdentity(record.CauseReason, maxIdentityBytes)) ||
		(record.FailureReason != "" &&
			!validIdentity(record.FailureReason, maxIdentityBytes)) ||
		record.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: invalid durable record", ErrInvalidOption)
	}
	switch record.Status {
	case StatusRunning, StatusCompensating, StatusCompleted,
		StatusCompensated, StatusFailed:
		return nil
	default:
		return fmt.Errorf("%w: invalid status", ErrInvalidOption)
	}
}

// Repository stores saga state with revision CAS and fencing enforcement.
type Repository interface {
	Load(context.Context, string) (Record, error)
	Create(context.Context, Record, uint64) (Record, error)
	Save(context.Context, Record, uint64, uint64) (Record, error)
}

// Result is one orchestration attempt outcome.
type Result struct {
	Record      Record
	Actions     int
	Compensated int
}

func validIdentity(value string, maximum int) bool {
	if value == "" ||
		len(value) > maximum ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func idempotencyKey(
	id string,
	definition string,
	version string,
	step string,
	phase Phase,
) string {
	sum := sha256.Sum256([]byte(strings.Join(
		[]string{id, definition, version, step, string(phase)},
		"\x00",
	)))
	return "saga:v1:" + hex.EncodeToString(sum[:])
}
