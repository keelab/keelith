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
func (logger *Logger) Name() string { return "keelith.audit" }

// Start marks the logger available after validating its context.
func (logger *Logger) Start(ctx context.Context) error {
	if logger == nil || ctx == nil {
		return fmt.Errorf("audit: invalid start")
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	logger.mu.Lock()
	logger.started = true
	logger.mu.Unlock()
	return nil
}

// Stop flushes and shuts down destination handlers that expose those standard
// lifecycle methods. It is idempotent.
func (logger *Logger) Stop(ctx context.Context) error {
	if logger == nil || ctx == nil {
		return fmt.Errorf("audit: invalid stop")
	}
	logger.mu.Lock()
	logger.started = false
	logger.mu.Unlock()
	return logger.flushAndShutdown(ctx)
}

// Flush exports pending audit records when the destination supports it.
func (logger *Logger) Flush(ctx context.Context) error {
	if logger == nil || ctx == nil {
		return fmt.Errorf("audit: invalid flush")
	}
	if flusher, ok := logger.logger.Handler().(interface{ ForceFlush(context.Context) error }); ok {
		return flusher.ForceFlush(ctx)
	}
	return nil
}

// Record validates and synchronously emits one audit event without sampling.
func (logger *Logger) Record(ctx context.Context, event Event) error {
	if logger == nil || logger.logger == nil {
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
	if err := logger.logger.Handler().Handle(ctx, record); err != nil {
		logger.failures.Add(1)
		if logger.policy == BestEffort {
			logger.dropped.Add(1)
			return nil
		}
		return fmt.Errorf("audit: emit record: %w", err)
	}
	logger.records.Add(1)
	return nil
}

// Status returns non-sensitive delivery counters.
func (logger *Logger) Status() Status {
	if logger == nil {
		return Status{}
	}
	return Status{Records: logger.records.Load(), Failures: logger.failures.Load(), Dropped: logger.dropped.Load()}
}

func (logger *Logger) flushAndShutdown(ctx context.Context) error {
	flushErr := logger.Flush(ctx)
	var shutdownErr error
	if shutdown, ok := logger.logger.Handler().(interface{ Shutdown(context.Context) error }); ok {
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
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
