// Package logging provides instance-scoped slog with redaction and trace IDs.
package logging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

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
func New(
	handler slog.Handler,
	resource *kresource.Resource,
	redacter Redacter,
) (*slog.Logger, error) {
	if handler == nil || resource == nil {
		return nil, fmt.Errorf("%w: handler or resource is nil", ErrInvalidOption)
	}
	return slog.New(&ContextHandler{
		handler:  handler.WithAttrs(resource.SlogAttributes()),
		redacter: redacter,
	}), nil
}

// Enabled delegates level filtering to the wrapped Handler.
func (handler *ContextHandler) Enabled(
	ctx context.Context,
	level slog.Level,
) bool {
	return handler.handler.Enabled(ctx, level)
}

// Handle redacts attributes and adds valid trace correlation before delegation.
func (handler *ContextHandler) Handle(
	ctx context.Context,
	record slog.Record,
) error {
	safe := slog.NewRecord(
		record.Time,
		record.Level,
		record.Message,
		record.PC,
	)
	record.Attrs(func(attribute slog.Attr) bool {
		redacted := handler.redacter.Redact(attribute)
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
	return handler.handler.Handle(ctx, safe)
}

// WithAttrs returns a derived Handler with defensively redacted attributes.
func (handler *ContextHandler) WithAttrs(
	attributes []slog.Attr,
) slog.Handler {
	safe := make([]slog.Attr, 0, len(attributes))
	for _, attribute := range attributes {
		redacted := handler.redacter.Redact(attribute)
		if !redacted.Equal(slog.Attr{}) {
			safe = append(safe, redacted)
		}
	}
	return &ContextHandler{
		handler:  handler.handler.WithAttrs(safe),
		redacter: handler.redacter,
	}
}

// WithGroup returns a derived Handler that preserves redaction.
func (handler *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{
		handler:  handler.handler.WithGroup(name),
		redacter: handler.redacter,
	}
}
