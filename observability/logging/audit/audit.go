package audit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

const maxFieldBytes = 256

// Event is the stable audit schema. It intentionally excludes arbitrary
// attributes, payloads, headers, credentials, and raw error text.
type Event struct {
	Actor        string
	Action       string
	ResourceType string
	ResourceID   string
	Outcome      string
	Reason       string
}

// Status is a low-cardinality audit delivery snapshot.
type Status struct {
	Records  uint64
	Failures uint64
	Dropped  uint64
}

// Policy controls whether a destination failure is returned to the caller.
type Policy uint8

const (
	// Required returns failures so security-sensitive callers can fail closed.
	Required Policy = iota
	// BestEffort counts and drops failed records without changing business flow.
	BestEffort
)

// Logger emits mandatory audit events through a dedicated slog pipeline.
// Record returns destination failures so applications can fail closed when
// their audit policy requires it.
type Logger struct {
	logger   *slog.Logger
	records  atomic.Uint64
	failures atomic.Uint64
	dropped  atomic.Uint64
	policy   Policy
	mu       sync.Mutex
	started  bool
}

// New constructs an audit logger. Pass a separately constructed logger when
// audit records require an isolated destination or retention policy.
func New(logger *slog.Logger) (*Logger, error) {
	return NewWithPolicy(logger, Required)
}

// NewWithPolicy constructs an audit logger with an explicit failure policy.
func NewWithPolicy(logger *slog.Logger, policy Policy) (*Logger, error) {
	if logger == nil {
		return nil, fmt.Errorf("audit: logger is nil")
	}
	if policy != Required && policy != BestEffort {
		return nil, fmt.Errorf("audit: invalid policy")
	}
	return &Logger{logger: logger, policy: policy}, nil
}

// Name returns the stable App component identity.
func (a *Logger) Name() string { return "keelith.audit" }

// Start marks the logger available after validating its context.
func (a *Logger) Start(ctx context.Context) error {
	if a == nil || ctx == nil {
		return fmt.Errorf("audit: invalid start")
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	a.mu.Lock()
	a.started = true
	a.mu.Unlock()
	return nil
}

// Stop flushes and shuts down destination handlers that expose those standard
// lifecycle methods. It is idempotent.
func (a *Logger) Stop(ctx context.Context) error {
	if a == nil || ctx == nil {
		return fmt.Errorf("audit: invalid stop")
	}
	a.mu.Lock()
	a.started = false
	a.mu.Unlock()
	return a.flushAndShutdown(ctx)
}

// Flush exports pending audit records when the destination supports it.
func (a *Logger) Flush(ctx context.Context) error {
	if a == nil || ctx == nil {
		return fmt.Errorf("audit: invalid flush")
	}
	if flusher, ok := a.logger.Handler().(interface{ ForceFlush(context.Context) error }); ok {
		return flusher.ForceFlush(ctx)
	}
	return nil
}

// Record validates and synchronously emits one audit event without sampling.
func (a *Logger) Record(ctx context.Context, event Event) error {
	if a == nil || a.logger == nil {
		return fmt.Errorf("audit: logger is nil")
	}
	if ctx == nil {
		return fmt.Errorf("audit: context is nil")
	}
	if err := validateEvent(event); err != nil {
		return err
	}
	record := slog.NewRecord(timeNow(), slog.LevelInfo, "audit event", 0)
	record.AddAttrs(
		slog.String("event", "audit.recorded"),
		slog.String("audit.actor", event.Actor),
		slog.String("audit.action", event.Action),
		slog.String("audit.resource_type", event.ResourceType),
		slog.String("audit.resource_id", event.ResourceID),
		slog.String("audit.outcome", event.Outcome),
		slog.String("audit.reason", event.Reason),
	)
	if err := a.logger.Handler().Handle(ctx, record); err != nil {
		a.failures.Add(1)
		if a.policy == BestEffort {
			a.dropped.Add(1)
			return nil
		}
		return fmt.Errorf("audit: emit record: %w", err)
	}
	a.records.Add(1)
	return nil
}

// Status returns non-sensitive delivery counters.
func (a *Logger) Status() Status {
	if a == nil {
		return Status{}
	}
	return Status{Records: a.records.Load(), Failures: a.failures.Load(), Dropped: a.dropped.Load()}
}

func (a *Logger) flushAndShutdown(ctx context.Context) error {
	flushErr := a.Flush(ctx)
	var shutdownErr error
	if shutdown, ok := a.logger.Handler().(interface{ Shutdown(context.Context) error }); ok {
		shutdownErr = shutdown.Shutdown(ctx)
	}
	return errors.Join(flushErr, shutdownErr)
}

func validateEvent(event Event) error {
	fields := []struct {
		name     string
		value    string
		required bool
	}{
		{name: "actor", value: event.Actor, required: true},
		{name: "action", value: event.Action, required: true},
		{name: "resource type", value: event.ResourceType, required: true},
		{name: "resource ID", value: event.ResourceID},
		{name: "outcome", value: event.Outcome, required: true},
		{name: "reason", value: event.Reason},
	}
	for _, field := range fields {
		if field.required && strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("audit: %s is required", field.name)
		}
		if !validField(field.value) {
			return fmt.Errorf("audit: %s is malformed or too large", field.name)
		}
	}
	switch event.Outcome {
	case "allowed", "denied", "succeeded", "failed":
	default:
		return fmt.Errorf("audit: unsupported outcome %q", event.Outcome)
	}
	return nil
}

func validField(value string) bool {
	if len(value) > maxFieldBytes || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
