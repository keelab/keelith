package cpu

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus adapts Controller to the low-sensitive Ops Runtime Catalog.
func RuntimeStatus(controller *Controller) ops.RuntimeStatusProvider {
	if controller == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := controller.Description()
		state := "idle"
		active := 0
		if description.Active {
			state = "capturing"
			active = 1
		}
		return ops.RuntimeStatus{
			State:    state,
			Ready:    true,
			Degraded: description.Failures > 0,
			Active:   active,
			Counters: []ops.RuntimeCounter{
				{Name: "captures", Value: description.Captures},
				{Name: "failures", Value: description.Failures},
				{Name: "rejected", Value: description.Rejected},
			},
			Capabilities: []string{
				"bounded-capture",
				"operation-labels",
				"process-global",
			},
		}, nil
	}
}
