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
	h slog.Handler,
	resource *kresource.Resource,
	redacter Redacter,
) (*slog.Logger, error) {
	if h == nil || resource == nil {
		return nil, fmt.Errorf("%w: handler or resource is nil", ErrInvalidOption)
	}
	return slog.New(&ContextHandler{
		handler:  h.WithAttrs(resource.SlogAttributes()),
		redacter: redacter,
	}), nil
}

// Enabled delegates level filtering to the wrapped Handler.
func (h *ContextHandler) Enabled(
	ctx context.Context,
	level slog.Level,
) bool {
	return h.handler.Enabled(ctx, level)
}

// Handle redacts attributes and adds valid trace correlation before delegation.
func (h *ContextHandler) Handle(
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
