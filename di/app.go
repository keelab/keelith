package di

import "github.com/keelab/keelith/app"

// AppOption connects graph-discovered lifecycle components to app.App.
func AppOption(graph *Graph) app.Option {
	if graph == nil {
		return app.WithComponents()
	}
	return app.WithComponents(graph.Components()...)
}
