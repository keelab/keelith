package idempotency

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus adapts Runtime counters to the value-free Ops catalog.
func RuntimeStatus(runtime *Runtime) ops.RuntimeStatusProvider {
	if runtime == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ops.ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := runtime.Describe()
		degraded := description.BackendErrors > 0 ||
			description.CompletionErrors > 0 ||
			description.AbandonErrors > 0 ||
			description.CodecErrors > 0
		return ops.RuntimeStatus{
			State:    "active",
			Ready:    true,
			Degraded: degraded,
			Counters: []ops.RuntimeCounter{
				{Name: "abandon_errors", Value: description.AbandonErrors},
				{Name: "acquired", Value: description.Acquired},
				{Name: "backend_errors", Value: description.BackendErrors},
				{Name: "codec_errors", Value: description.CodecErrors},
				{Name: "completion_errors", Value: description.CompletionErrors},
				{Name: "conflicts", Value: description.Conflicts},
				{Name: "handler_failures", Value: description.HandlerFailures},
				{Name: "in_progress", Value: description.InProgress},
				{Name: "replayed", Value: description.Replayed},
			},
			Capabilities: []string{
				"bounded-result-replay",
				"fail-closed",
				"fenced-owner",
				"operation-scoped",
			},
		}, nil
	}
}
