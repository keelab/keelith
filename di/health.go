package di

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/health"
)

// RegisterHealth adds a bounded dependency check for the graph. Provider-owned
// component health remains registered by the component itself; this checker
// reports only graph construction ownership and closure state.
func RegisterHealth(registry *health.Registry, name string, graph *Graph) error {
	if registry == nil || graph == nil {
		return fmt.Errorf("di: registry or graph is nil")
	}
	return registry.Register(health.KindDependency, name, func(context.Context) health.Result {
		if graph.Closed() {
			return health.Fail("dependency graph is closed")
		}
		return health.Pass("dependency graph is ready")
	})
}
