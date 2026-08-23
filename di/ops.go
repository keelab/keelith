package di

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatusRegistration exposes bounded dependency graph construction
// counters without including values, configuration, or secrets.
func RuntimeStatusRegistration(name string, graph *Graph) ops.RuntimeStatusRegistration {
	return ops.RuntimeStatusRegistration{
		Name: name,
		Kind: "dependency-graph",
		Provider: func(ctx context.Context) (ops.RuntimeStatus, error) {
			if graph == nil {
				return ops.RuntimeStatus{}, fmt.Errorf("di: graph is nil")
			}
			if ctx == nil {
				return ops.RuntimeStatus{}, fmt.Errorf("di: context is nil")
			}
			description := graph.Description()
			var constructed uint64
			for _, provider := range description.Providers {
				if provider.Constructed {
					constructed++
				}
			}
			state, ready := "ready", true
			if graph.Closed() {
				state, ready = "stopped", false
			}
			return ops.RuntimeStatus{
				State: state, Ready: ready,
				Counters: []ops.RuntimeCounter{
					{Name: "components", Value: uint64(len(graph.Components()))},
					{Name: "constructed", Value: constructed},
					{Name: "providers", Value: uint64(len(description.Providers))},
				},
				Capabilities: []string{"cleanup", "components", "describe"},
			}, nil
		},
	}
}
