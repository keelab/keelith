package topology

import (
	"fmt"
	"sort"
)

// Bindings returns every resolved dependency decision in stable source/target
// order. The returned slice contains values only and cannot mutate Snapshot.
func (snapshot Snapshot) Bindings() ([]Binding, error) {
	if snapshot.epoch == 0 || snapshot.hash == "" {
		return nil, fmt.Errorf("%w: snapshot is not activated", ErrInvalidPlan)
	}
	result := make([]Binding, 0)
	for source, targets := range snapshot.dependencies {
		for target := range targets {
			binding, err := snapshot.Resolve(source, target)
			if err != nil {
				return nil, err
			}
			result = append(result, binding)
		}
	}
	sort.Slice(result, func(first, second int) bool {
		if result[first].Source != result[second].Source {
			return result[first].Source < result[second].Source
		}
		return result[first].Target < result[second].Target
	})
	return result, nil
}
