package ops

import (
	"context"

	kconfigured "github.com/keelab/keelith/feature/configured"
)

// FeatureRuntimeStatus adapts dynamic feature evaluation to the value-free
// runtime catalog. It never exposes flag keys, variants, targeting attributes,
// context values, or configuration paths.
func FeatureRuntimeStatus(binding *kconfigured.Binding) RuntimeStatusProvider {
	if binding == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := binding.Description()
		state := "bootstrap"
		if description.Loaded {
			state = "active"
		}
		return RuntimeStatus{
			State:    state,
			Ready:    description.Loaded,
			Degraded: description.Failed,
			Active:   description.Flags,
			Counters: []RuntimeCounter{
				{Name: "defaults", Value: description.Defaults},
				{Name: "evaluations", Value: description.Evaluations},
				{Name: "fallbacks", Value: description.Fallbacks},
				{Name: "flags", Value: uint64(description.Flags)},
				{Name: "percentage_matches", Value: description.PercentageMatched},
				{Name: "rule_matches", Value: description.RulesMatched},
				{Name: "rules", Value: uint64(description.Rules)},
				{Name: "updates", Value: description.Updates},
			},
			Capabilities: []string{
				"atomic-reload",
				"deterministic-percentage",
				"last-good",
				"typed-evaluation",
			},
		}, nil
	}
}
