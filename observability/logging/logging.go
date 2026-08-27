// Package logging provides instance-scoped slog with redaction and trace IDs.
package logging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	kresource "github.com/keelab/keelith/observability/resource"
	"github.com/keelab/keelith/placement"
	"go.opentelemetry.io/otel/trace"
)

// ErrInvalidOption reports an invalid logging dependency.
var ErrInvalidOption = errors.New("logging: invalid option")

// ContextHandler adds trace correlation and redacts every attribute path.
type ContextHandler struct {
	handler  slog.Handler
	redacter Redacter
}

// New creates an instance Logger with immutable Resource attributes.
func New(h slog.Handler, resource *kresource.Resource, redacter Redacter) (*Logger, error) {
	if h == nil || resource == nil {
		return nil, fmt.Errorf("%w: handler or resource is nil", ErrInvalidOption)
	}
	return &Logger{
		base: slog.New(&ContextHandler{
			handler:  h.WithAttrs(resource.SlogAttributes()),
			redacter: redacter,
		}),
	}, nil
}

// Logger is the application logger facade. It records the caller of the
// facade, so business logging wrappers can keep source locations accurate.
type Logger struct {
	base       *slog.Logger
	callerSkip int
}

// Slog returns the underlying slog logger for integrations that require it.
func (l *Logger) Slog() *slog.Logger {
	if l == nil {
		return nil
	}
	return l.base
}

// Handler returns the underlying handler tree.
func (l *Logger) Handler() slog.Handler {
	if l == nil || l.base == nil {
		return nil
	}
	return l.base.Handler()
}

// Enabled reports whether level is enabled.
func (l *Logger) Enabled(ctx context.Context, level slog.Level) bool {
	return l != nil && l.base != nil && l.base.Enabled(ctx, level)
}

// WithCallerSkip returns a logger that skips additional facade frames.
func (l *Logger) WithCallerSkip(skip int) *Logger {
	if l == nil {
		return nil
	}
	if skip < 0 {
		skip = 0
	}
	return &Logger{base: l.base, callerSkip: l.callerSkip + skip}
}

// With returns a logger with bound attributes.
func (l *Logger) With(args ...any) *Logger {
	if l == nil || l.base == nil {
		return nil
	}
	return &Logger{base: l.base.With(args...), callerSkip: l.callerSkip}
}

// WithGroup returns a logger with a bound group.
func (l *Logger) WithGroup(name string) *Logger {
	if l == nil || l.base == nil {
		return nil
	}
	return &Logger{base: l.base.WithGroup(name), callerSkip: l.callerSkip}
}

// DebugContext logs a debug-level message with the supplied context.
func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelDebug, msg, args...)
}

// Debug logs a debug-level message without an explicit context.
func (l *Logger) Debug(msg string, args ...any) {
	l.DebugContext(context.Background(), msg, args...)
}

// InfoContext logs an info-level message with the supplied context.
func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelInfo, msg, args...)
}

// Info logs an info-level message without an explicit context.
func (l *Logger) Info(msg string, args ...any) {
	l.InfoContext(context.Background(), msg, args...)
}

// WarnContext logs a warning-level message with the supplied context.
func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelWarn, msg, args...)
}

// Warn logs a warning-level message without an explicit context.
func (l *Logger) Warn(msg string, args ...any) {
	l.WarnContext(context.Background(), msg, args...)
}

// ErrorContext logs an error-level message with the supplied context.
func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelError, msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.ErrorContext(context.Background(), msg, args...)
}

// Log logs a message at the supplied level with the supplied context.
func (l *Logger) Log(ctx context.Context, level slog.Level, msg string, args ...any) {
	l.log(ctx, level, msg, args...)
}

// LogAttrs logs a message at the supplied level with structured attributes.
func (l *Logger) LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	if l == nil || l.base == nil || !l.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3+l.callerSkip, pcs[:])
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.AddAttrs(attrs...)
	_ = l.base.Handler().Handle(ctx, record)
}

func (l *Logger) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if l == nil || l.base == nil || !l.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3+l.callerSkip, pcs[:])
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.Add(args...)
	_ = l.base.Handler().Handle(ctx, record)
}

// Enabled delegates level filtering to the wrapped Handler.
func (h *ContextHandler) Enabled(
	ctx context.Context,
	level slog.Level,
) bool {
	return h.handler.Enabled(ctx, level)
}

// Handle redacts attributes and adds valid trace correlation before delegation.
func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	safe := slog.NewRecord(
		record.Time,
		record.Level,
		record.Message,
		record.PC,
	)
	record.Attrs(func(attribute slog.Attr) bool {
		redacted := h.redacter.Redact(attribute)
		if !redacted.Equal(slog.Attr{}) {
			safe.AddAttrs(redacted)
		}
		return true
	})
	span := trace.SpanContextFromContext(ctx)
	if span.IsValid() {
		safe.AddAttrs(
			slog.String("trace_id", span.TraceID().String()),
			slog.String("span_id", span.SpanID().String()),
		)
	}
	if current, ok := placement.FromContext(ctx); ok {
		safe.AddAttrs(
			slog.String("listener.name", current.Listener()),
			slog.String("profile.name", current.Profile()),
			slog.String("group.name", current.GroupAttribute()),
			slog.String("placement.service", current.Service()),
		)
	}
	return h.handler.Handle(ctx, safe)
}

// WithAttrs returns a derived Handler with defensively redacted attributes.
func (h *ContextHandler) WithAttrs(
	attributes []slog.Attr,
) slog.Handler {
	safe := make([]slog.Attr, 0, len(attributes))
	for _, attribute := range attributes {
		redacted := h.redacter.Redact(attribute)
		if !redacted.Equal(slog.Attr{}) {
			safe = append(safe, redacted)
		}
	}
	return &ContextHandler{
		handler:  h.handler.WithAttrs(safe),
		redacter: h.redacter,
	}
}

// WithGroup returns a derived Handler that preserves redaction.
func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{
		handler:  h.handler.WithGroup(name),
		redacter: h.redacter,
	}
}
