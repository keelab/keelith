package ops

import (
	"context"

	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
)

// LoggingRuntimeStatus adapts a Controller to the bounded runtime catalog.
func LoggingRuntimeStatus(controller *logging.Controller) RuntimeStatusProvider {
	if controller == nil {
		return nil
	}
	return func(context.Context) (RuntimeStatus, error) {
		status := controller.Status()
		return RuntimeStatus{
			State: "ready", Ready: true,
			Counters:     []RuntimeCounter{{Name: "level_updates", Value: status.Updates}},
			Capabilities: []string{"dynamic-level"},
		}, nil
	}
}

// AuditRuntimeStatus adapts audit delivery counters without exposing events.
func AuditRuntimeStatus(logger *audit.Logger) RuntimeStatusProvider {
	if logger == nil {
		return nil
	}
	return func(context.Context) (RuntimeStatus, error) {
		status := logger.Status()
		return RuntimeStatus{
			State: "ready", Ready: true, Degraded: status.Failures > 0,
			Counters: []RuntimeCounter{
				{Name: "dropped", Value: status.Dropped},
				{Name: "failures", Value: status.Failures},
				{Name: "records", Value: status.Records},
			},
			Capabilities: []string{"failure-reporting", "non-sampled"},
		}, nil
	}
}
