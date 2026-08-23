package websocket

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus adapts a Hub to the low-sensitive Ops Runtime Catalog.
func RuntimeStatus(hub *Hub) ops.RuntimeStatusProvider {
	if hub == nil {
		return nil
	}
	return func(
		ctx context.Context,
	) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := hub.Describe()
		return ops.RuntimeStatus{
			State:  description.State,
			Ready:  description.Ready,
			Active: description.Active,
			Counters: []ops.RuntimeCounter{
				{Name: "accepted", Value: description.Accepted},
				{Name: "finished", Value: description.Finished},
				{Name: "rejected", Value: description.Rejected},
				{Name: "sent", Value: description.Sent},
				{Name: "received", Value: description.Received},
				{
					Name:  "heartbeat_failures",
					Value: description.HeartbeatFailures,
				},
			},
			Capabilities: []string{
				"bidi-stream",
				"connection-drain",
				"origin-policy",
				"subprotocol",
			},
		}, nil
	}
}
