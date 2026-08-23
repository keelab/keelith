package service

import "fmt"

const maxSurfacesPerRuntime = 32

// SurfaceRegistry is an immutable, runtime-wide listener identity registry.
// Listener names are unique across transports so telemetry never aliases two
// independently served surfaces.
type SurfaceRegistry struct {
	surfaces []*Surface
}

// NewSurfaceRegistry validates and snapshots every listener Surface.
func NewSurfaceRegistry(surfaces ...*Surface) (*SurfaceRegistry, error) {
	if len(surfaces) == 0 || len(surfaces) > maxSurfacesPerRuntime {
		return nil, fmt.Errorf(
			"%w: surface count must be between 1 and %d",
			ErrInvalidBinding,
			maxSurfacesPerRuntime,
		)
	}
	snapshot := append([]*Surface(nil), surfaces...)
	names := make(map[string]struct{}, len(snapshot))
	for index, surface := range snapshot {
		if surface == nil || surface.profile == nil {
			return nil, fmt.Errorf("%w: surface %d is nil", ErrInvalidBinding, index)
		}
		if _, exists := names[surface.name]; exists {
			return nil, fmt.Errorf("%w: listener %q is duplicated", ErrInvalidBinding, surface.name)
		}
		names[surface.name] = struct{}{}
	}
	return &SurfaceRegistry{surfaces: snapshot}, nil
}

// Surfaces returns a defensive copy in declaration order.
func (registry *SurfaceRegistry) Surfaces() []*Surface {
	if registry == nil {
		return nil
	}
	return append([]*Surface(nil), registry.surfaces...)
}

// Describe returns one atomic defensive identity snapshot.
func (registry *SurfaceRegistry) Describe() []SurfaceDescription {
	if registry == nil {
		return nil
	}
	result := make([]SurfaceDescription, len(registry.surfaces))
	for index, surface := range registry.surfaces {
		result[index] = surface.Describe()
	}
	return result
}
