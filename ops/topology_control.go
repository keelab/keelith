package ops

import (
	"context"

	"github.com/keelab/keelith/programmable/component"
)

// TopologyControlRuntimeStatus adapts revisioned epoch control without
// exposing plan hashes, placements, component IDs, signatures or errors.
func TopologyControlRuntimeStatus(
	runtime *component.ControlledEpochRuntime,
) RuntimeStatusProvider {
	if runtime == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		status := runtime.Status()
		state := "bootstrap"
		switch {
		case status.Stopped:
			state = "stopped"
		case status.Running && status.Control.Degraded:
			state = "degraded"
		case status.Running:
			state = "running"
		}
		active := 0
		if status.ActiveEpoch != 0 {
			active = 1
		}
		return RuntimeStatus{
			State: state,
			Ready: status.Running && status.ActiveEpoch != 0 &&
				status.Control.AppliedRevision != 0,
			Degraded: status.Control.Degraded,
			Active:   active,
			Counters: []RuntimeCounter{
				{Name: "applied_revision", Value: status.Control.AppliedRevision},
				{Name: "epoch", Value: status.ActiveEpoch},
				{Name: "observed_revision", Value: status.Control.ObservedRevision},
				{Name: "reconnects", Value: status.Control.Reconnects},
			},
			Capabilities: []string{
				"bounded-reconnect",
				"epoch-drain",
				"last-good",
				"revisioned-plan",
			},
		}, nil
	}
}
