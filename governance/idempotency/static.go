package idempotency

import (
	"fmt"

	"github.com/keelab/keelith/operation"
)

// Registration binds an exact unary Operation to one Rule.
type Registration struct {
	Operation operation.Operation
	Rule      Rule
}

// StaticResolver is an immutable exact-operation rule table.
type StaticResolver struct {
	rules map[string]Rule
}

// NewStaticResolver validates and snapshots registrations.
func NewStaticResolver(registrations ...Registration) (*StaticResolver, error) {
	rules := make(map[string]Rule, len(registrations))
	for index, registration := range registrations {
		target := registration.Operation
		if target.Transport() == "" || target.Service() == "" ||
			target.Method() == "" || target.Kind() != operation.KindUnary {
			return nil, fmt.Errorf("%w: registration %d requires a unary operation", ErrInvalidConfig, index)
		}
		if err := ValidateRule(registration.Rule); err != nil {
			return nil, fmt.Errorf("registration %d: %w", index, err)
		}
		key := target.PolicyKey()
		if _, duplicate := rules[key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate operation %q", ErrInvalidConfig, key)
		}
		rules[key] = registration.Rule
	}
	return &StaticResolver{rules: rules}, nil
}

// Resolve returns an exact immutable Rule.
func (r *StaticResolver) Resolve(target operation.Operation) (Rule, bool) {
	if r == nil {
		return Rule{}, false
	}
	rule, exists := r.rules[target.PolicyKey()]
	return rule, exists
}
