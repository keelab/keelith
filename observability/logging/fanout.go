package logging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
)

// FanoutHandler dispatches one record to multiple independent handlers.
// Derived attributes and groups are applied consistently to every destination.
type FanoutHandler struct {
	handlers []slog.Handler
}

// NewFanout creates an immutable handler fanout. At least one concrete handler
// is required; typed nil handlers are rejected as invalid dependencies.
func NewFanout(handlers ...slog.Handler) (slog.Handler, error) {
	if len(handlers) == 0 {
		return nil, fmt.Errorf("%w: fanout requires at least one handler", ErrInvalidOption)
	}
	cloned := make([]slog.Handler, len(handlers))
	for index, handler := range handlers {
		if nilHandler(handler) {
			return nil, fmt.Errorf("%w: fanout handler %d is nil", ErrInvalidOption, index)
		}
		cloned[index] = handler
	}
	return &FanoutHandler{handlers: cloned}, nil
}

// Enabled reports whether any destination accepts the level.
func (handler *FanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if handler == nil {
		return false
	}
	for _, destination := range handler.handlers {
		if destination.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Handle dispatches the record to every enabled destination and joins errors.
func (handler *FanoutHandler) Handle(ctx context.Context, record slog.Record) error {
	if handler == nil {
		return fmt.Errorf("%w: fanout is nil", ErrInvalidOption)
	}
	var result error
	for _, destination := range handler.handlers {
		if destination.Enabled(ctx, record.Level) {
			result = errors.Join(result, destination.Handle(ctx, record.Clone()))
		}
	}
	return result
}

// WithAttrs derives every destination with the same attributes.
func (handler *FanoutHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	if handler == nil {
		return handler
	}
	derived := make([]slog.Handler, 0, len(handler.handlers))
	for _, destination := range handler.handlers {
		derived = append(derived, destination.WithAttrs(attributes))
	}
	return &FanoutHandler{handlers: derived}
}

// WithGroup derives every destination with the same group.
func (handler *FanoutHandler) WithGroup(name string) slog.Handler {
	if handler == nil {
		return handler
	}
	derived := make([]slog.Handler, 0, len(handler.handlers))
	for _, destination := range handler.handlers {
		derived = append(derived, destination.WithGroup(name))
	}
	return &FanoutHandler{handlers: derived}
}

func nilHandler(handler slog.Handler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
