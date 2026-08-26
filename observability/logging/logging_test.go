package logging

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"testing"
)

type callerHandler struct {
	pc uintptr
}

func (handler *callerHandler) Enabled(context.Context, slog.Level) bool { return true }

func (handler *callerHandler) Handle(_ context.Context, record slog.Record) error {
	handler.pc = record.PC
	return nil
}

func (handler *callerHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }

func (handler *callerHandler) WithGroup(string) slog.Handler { return handler }

func TestLoggerCallerSkip(t *testing.T) {
	handler := &callerHandler{}
	base := &Logger{base: slog.New(handler)}
	logger := base.WithCallerSkip(1)

	emitWrapped(logger)
	function := runtime.FuncForPC(handler.pc)
	if function == nil || !strings.HasSuffix(function.Name(), ".TestLoggerCallerSkip") {
		t.Fatalf("source function = %v, want TestLoggerCallerSkip", function)
	}
}

func emitWrapped(logger *Logger) {
	logger.ErrorContext(context.Background(), "wrapped")
}
