package xds

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus adapts bounded xDS discovery counters to the shared Keelith
// runtime catalog without exposing node, resource, version, nonce, endpoint,
// or rejection values.
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
		state := "idle"
		if description.Closed {
			state = "closed"
		} else if description.Watchers != 0 {
			state = "active"
		}
		return ops.RuntimeStatus{
			State:    state,
			Ready:    !description.Closed,
			Degraded: client.degraded(),
			Active:   description.Watchers,
			Counters: []ops.RuntimeCounter{
				{Name: "accepted", Value: description.Accepted},
				{Name: "expired", Value: description.Expired},
				{Name: "rejected", Value: description.Rejected},
				{Name: "resources", Value: uint64(description.Resources)},
				{Name: "responses", Value: description.Responses},
				{Name: "watchers", Value: uint64(description.Watchers)},
			},
			Capabilities: []string{
				"ads-v3",
				"eds",
				"endpoint-stale-expiry",
				"full-snapshot-watch",
				"last-good",
				"sotw",
			},
		}, nil
	}
}

// ManagedRuntimeStatus adapts owned connection rotation and aggregate eds
// counters to the shared runtime catalog without exposing targets, generations,
// tls material, node identity, resource names, versions, nonces, or errors.
func ManagedRuntimeStatus(runtime *Runtime) ops.RuntimeStatusProvider {
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
		state := "idle"
		switch {
		case description.Closed:
			state = "closed"
		case description.Rotating:
			state = "rotating"
		case description.Watchers != 0:
			state = "active"
		case description.Started:
			state = "started"
		}
		capabilities := []string{
			"ads-v3",
			"eds",
			"endpoint-stale-expiry",
			"full-snapshot-watch",
			"last-good",
			"owned-connection",
			"sotw",
		}
		if description.RotationEnabled {
			capabilities = append(
				capabilities,
				"active-tls-connection-rotation",
			)
		}
		return ops.RuntimeStatus{
			State:    state,
			Ready:    description.Started && !description.Closed,
			Degraded: description.Degraded,
			Active:   description.Watchers,
			Counters: []ops.RuntimeCounter{
				{Name: "accepted", Value: description.Accepted},
				{Name: "expired", Value: description.Expired},
				{Name: "rejected", Value: description.Rejected},
				{Name: "resources", Value: uint64(description.Resources)},
				{Name: "responses", Value: description.Responses},
				{Name: "rotation_failures", Value: description.RotationFailures},
				{Name: "rotations", Value: description.Rotations},
				{Name: "watchers", Value: uint64(description.Watchers)},
			},
			Capabilities: capabilities,
		}, nil
	}
}
