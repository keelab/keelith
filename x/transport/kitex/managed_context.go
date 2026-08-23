package kitex

import (
	"context"

	"github.com/cloudwego/kitex/client/callopt"
)

// WithoutNativeCallOptions preserves cancellation, deadlines, and ordinary
// context values while hiding Kitex call options from a generated client.
//
// Generated Kitex clients do not overwrite an existing call-option value when
// invoked with zero options. Managed facades call this at the final boundary so
// callers cannot inject native retry or fallback through context.
func WithoutNativeCallOptions(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return managedCallContext{Context: ctx}
}

type managedCallContext struct {
	context.Context
}

func (ctx managedCallContext) Value(key any) any {
	value := ctx.Context.Value(key)
	if _, native := value.([]callopt.Option); native {
		return nil
	}
	return value
}
