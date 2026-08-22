package topology

import "fmt"

// Resolve returns the frozen binding for one declared dependency.
func (snapshot Snapshot) Resolve(source ComponentID, target ComponentID) (Binding, error) {
	if snapshot.epoch == 0 || snapshot.hash == "" {
		return Binding{}, fmt.Errorf("%w: snapshot is not activated", ErrInvalidPlan)
	}
	sourcePlacement, sourceExists := snapshot.components[source]
	targetPlacement, targetExists := snapshot.components[target]
	targets, declared := snapshot.dependencies[source]
	constraint, bound := targets[target]
	if !sourceExists || !targetExists || !declared || !bound {
		return Binding{}, fmt.Errorf("%w: %q -> %q", ErrUnboundDependency, source, target)
	}

	mode := constraint
	if mode == BindingAuto {
		mode = BindingRemote
		if sourcePlacement == targetPlacement {
			mode = BindingLocal
		}
	}
	return Binding{
		Source:          source,
		Target:          target,
		SourcePlacement: sourcePlacement,
		TargetPlacement: targetPlacement,
		Mode:            mode,
		Epoch:           snapshot.epoch,
		PlanHash:        snapshot.hash,
	}, nil
}
