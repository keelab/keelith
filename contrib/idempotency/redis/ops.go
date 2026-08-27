package redis

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus adapts Client state to the value-free Ops catalog.
func RuntimeStatus(client *Client) ops.RuntimeStatusProvider {
	if client == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ops.ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := client.Describe()
		state := "active"
		if description.Closed {
			state = "stopped"
		}
		return ops.RuntimeStatus{
			State:    state,
			Ready:    !description.Closed,
			Degraded: description.Errors > 0 || description.StaleOwners > 0,
			Counters: []ops.RuntimeCounter{
				{Name: "abandoned", Value: description.Abandoned},
				{Name: "acquired", Value: description.Acquired},
				{Name: "completed", Value: description.Completed},
				{Name: "conflicts", Value: description.Conflicts},
				{Name: "errors", Value: description.Errors},
				{Name: "in_progress", Value: description.InProgress},
				{Name: "replayed", Value: description.Replayed},
				{Name: "stale_owners", Value: description.StaleOwners},
			},
			Capabilities: []string{
				"atomic-claim",
				"backend:redis",
				"fenced-completion",
				"result-replay",
			},
		}, nil
	}
}
