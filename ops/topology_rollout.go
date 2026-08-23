package ops

import (
	"context"

	"github.com/keelab/keelith/programmable/topology/control"
)

// TopologyRollbackRuntimeStatus adapts the bounded rollout health window
// without exposing routing keys, business identities, plan bodies, or errors.
func TopologyRollbackRuntimeStatus(
	controller *control.RollbackController,
) RuntimeStatusProvider {
	if controller == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		status := controller.Status()
		state := "idle"
		active := 0
		if status.Armed {
			state = "observing"
			active = 1
		}
		if status.Triggered {
			state = "rollback-published"
			active = 0
		}
		return RuntimeStatus{
			State: state, Ready: status.Armed,
			Degraded: status.Triggered, Active: active,
			Counters: []RuntimeCounter{
				{Name: "candidate_revision", Value: status.CandidateRevision},
				{Name: "error_basis_points", Value: uint64(status.ErrorBasisPoints)},
				{Name: "published_revision", Value: status.PublishedRevision},
				{Name: "samples", Value: uint64(status.Samples)},
				{Name: "slow_basis_points", Value: uint64(status.SlowBasisPoints)},
			},
			Capabilities: []string{
				"bounded-health-window",
				"fixed-outcome-classes",
				"immutable-rollback-revision",
			},
		}, nil
	}
}
