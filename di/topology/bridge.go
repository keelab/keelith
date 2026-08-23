package topology

import (
	"github.com/keelab/keelith/programmable/component"
	ktopology "github.com/keelab/keelith/programmable/topology"
)

// Bind resolves a frozen local/remote provider reference for injection.
func Bind[T any](
	runtime *component.Runtime,
	source ktopology.ComponentID,
	target ktopology.ComponentID,
) (component.Ref[T], error) {
	return component.Bind[T](runtime, source, target)
}
