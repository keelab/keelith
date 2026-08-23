package genericrpc

import (
	"context"
	"sort"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus returns a low-sensitive Ops provider for client.
func RuntimeStatus(client *Client) ops.RuntimeStatusProvider {
	if client == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ErrInvalidConfig
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := client.Description()
		capabilities := []string{"proto-json", "unary"}
		if description.Encrypted {
			capabilities = append(capabilities, "tls")
		} else {
			capabilities = append(capabilities, "explicit-insecure")
		}
		sort.Strings(capabilities)
		return ops.RuntimeStatus{
			State:    string(description.State),
			Ready:    description.Ready,
			Degraded: description.State == StateFailed,
			Active:   description.Active,
			Counters: []ops.RuntimeCounter{
				{Name: "calls", Value: description.Calls},
				{Name: "failures", Value: description.Failures},
				{Name: "rejected", Value: description.Rejected},
				{
					Name:  "request_oversized",
					Value: description.RequestOversized,
				},
				{
					Name:  "response_oversized",
					Value: description.ResponseOversized,
				},
			},
			Capabilities: capabilities,
		}, nil
	}
}
