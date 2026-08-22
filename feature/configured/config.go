// Package configured adapts Keelith dynamic configuration to feature.Store.
package configured

import (
	"context"
	"fmt"

	kconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/feature"
)

// Variation is one strongly typed named value in configuration.
type Variation struct {
	Name    string  `config:"name"`
	Boolean bool    `config:"boolean"`
	String  string  `config:"string"`
	Integer int64   `config:"integer"`
	Float   float64 `config:"float"`
}

// Condition is one bounded exact string comparison.
type Condition struct {
	Attribute string   `config:"attribute"`
	Operator  string   `config:"operator"`
	Values    []string `config:"values"`
}

// Allocation assigns basis points to one named variation.
type Allocation struct {
	Variation   string `config:"variation"`
	BasisPoints int    `config:"basisPoints"`
}

// Rule is one ordered targeting rule.
type Rule struct {
	Name       string       `config:"name"`
	Conditions []Condition  `config:"conditions"`
	Variation  string       `config:"variation"`
	Rollout    []Allocation `config:"rollout"`
}

// Flag is one typed flag definition.
type Flag struct {
	Key              string      `config:"key"`
	Kind             string      `config:"kind"`
	DefaultVariation string      `config:"defaultVariation"`
	Variations       []Variation `config:"variations"`
	Rules            []Rule      `config:"rules"`
}

// Config is one complete dynamic feature subtree.
type Config struct {
	Flags []Flag `config:"flags"`
}

// Description is a value-free feature configuration diagnostic.
type Description struct {
	Name              string
	Path              string
	Loaded            bool
	Failed            bool
	Revision          string
	Flags             int
	Rules             int
	Updates           uint64
	Evaluations       uint64
	Defaults          uint64
	RulesMatched      uint64
	PercentageMatched uint64
	Fallbacks         uint64
}

// Binding validates complete revisions before publishing them to a stable
// lock-free Store.
type Binding struct {
	component *kconfig.Component[Config]
	store     *feature.Store
}

// NewBinding constructs a hot-reload feature binding at path. An absent
// subtree publishes an empty definition so application fallbacks remain safe.
func NewBinding(name string, path string) (*Binding, error) {
	component, err := kconfig.NewComponent[Config](
		name,
		path,
		kconfig.WithComponentDefault(Config{}),
		kconfig.WithComponentValidator(func(value Config) error {
			_, err := definitionFromConfig("candidate", value)
			return err
		}),
		kconfig.WithReloadableFields[Config]("flags"),
	)
	if err != nil {
		return nil, err
	}
	store, err := feature.NewStore(feature.Definition{Revision: "bootstrap"})
	if err != nil {
		return nil, err
	}
	return &Binding{component: component, store: store}, nil
}

// Name returns the Config Manager subscriber identity.
func (binding *Binding) Name() string {
	if binding == nil || binding.component == nil {
		return ""
	}
	return binding.component.Name()
}

// Validate strictly decodes and compiles one candidate before publication.
func (binding *Binding) Validate(snapshot kconfig.Snapshot) error {
	if binding == nil || binding.component == nil {
		return fmt.Errorf("%w: configured binding is nil", feature.ErrInvalidDefinition)
	}
	return binding.component.Validate(snapshot)
}

// Apply publishes the typed config and corresponding immutable flag snapshot.
func (binding *Binding) Apply(ctx context.Context, snapshot kconfig.Snapshot) error {
	if binding == nil || binding.component == nil || binding.store == nil {
		return fmt.Errorf("%w: configured binding is nil", feature.ErrInvalidDefinition)
	}
	if err := binding.component.Apply(ctx, snapshot); err != nil {
		return err
	}
	current, ok := binding.component.Current()
	if !ok {
		return fmt.Errorf("%w: feature config was not published", feature.ErrInvalidDefinition)
	}
	definition, err := definitionFromConfig(snapshot.Revision(), current)
	if err != nil {
		return err
	}
	_, err = binding.store.Update(definition)
	return err
}

// Store returns the stable evaluator updated by this Binding.
func (binding *Binding) Store() *feature.Store {
	if binding == nil {
		return nil
	}
	return binding.store
}

// Description returns only lifecycle, cardinality, and aggregate decisions.
func (binding *Binding) Description() Description {
	if binding == nil || binding.component == nil || binding.store == nil {
		return Description{}
	}
	component := binding.component.Description()
	store := binding.store.Describe()
	return Description{
		Name:              component.Name,
		Path:              component.Path,
		Loaded:            component.Loaded,
		Failed:            component.Failed,
		Revision:          store.Revision,
		Flags:             store.Flags,
		Rules:             store.Rules,
		Updates:           store.Updates,
		Evaluations:       store.Evaluations,
		Defaults:          store.Defaults,
		RulesMatched:      store.RulesMatched,
		PercentageMatched: store.PercentageMatched,
		Fallbacks:         store.Fallbacks,
	}
}

func definitionFromConfig(revision string, value Config) (feature.Definition, error) {
	definition := feature.Definition{
		Revision: revision,
		Flags:    make([]feature.Flag, len(value.Flags)),
	}

	for flagIndex, configuredFlag := range value.Flags {
		flag := feature.Flag{
			Key:              configuredFlag.Key,
			DefaultVariation: configuredFlag.DefaultVariation,
			Variations:       make([]feature.Variation, len(configuredFlag.Variations)),
			Rules:            make([]feature.Rule, len(configuredFlag.Rules)),
		}
		for variationIndex, configuredVariation := range configuredFlag.Variations {
			value, err := configuredValue(configuredFlag.Kind, configuredVariation)
			if err != nil {
				return feature.Definition{}, fmt.Errorf("flag %d variation %d: %w", flagIndex, variationIndex, err)
			}
			flag.Variations[variationIndex] = feature.Variation{
				Name:  configuredVariation.Name,
				Value: value,
			}
		}
		for ruleIndex, configuredRule := range configuredFlag.Rules {
			rule := feature.Rule{
				Name:       configuredRule.Name,
				Variation:  configuredRule.Variation,
				Conditions: make([]feature.Condition, len(configuredRule.Conditions)),
				Rollout:    make([]feature.Allocation, len(configuredRule.Rollout)),
			}
			for conditionIndex, configuredCondition := range configuredRule.Conditions {
				rule.Conditions[conditionIndex] = feature.Condition{
					Attribute: configuredCondition.Attribute,
					Operator:  feature.Operator(configuredCondition.Operator),
					Values:    append([]string(nil), configuredCondition.Values...),
				}
			}
			for allocationIndex, configuredAllocation := range configuredRule.Rollout {
				if configuredAllocation.BasisPoints < 0 ||
					configuredAllocation.BasisPoints > int(^uint16(0)) {
					return feature.Definition{}, fmt.Errorf("%w: flag %d rule %d allocation %d basis points are invalid", feature.ErrInvalidDefinition, flagIndex, ruleIndex, allocationIndex)
				}
				rule.Rollout[allocationIndex] = feature.Allocation{
					Variation:   configuredAllocation.Variation,
					BasisPoints: uint16(configuredAllocation.BasisPoints),
				}
			}
			flag.Rules[ruleIndex] = rule
		}
		definition.Flags[flagIndex] = flag
	}
	if _, err := feature.NewStore(definition); err != nil {
		return feature.Definition{}, err
	}
	return definition, nil
}

func configuredValue(kind string, value Variation) (feature.Value, error) {
	invalidUnused := func(condition bool) (feature.Value, error) {
		if condition {
			return feature.Value{}, fmt.Errorf("%w: variation contains a value for another kind", feature.ErrInvalidDefinition)
		}
		return feature.Value{}, nil
	}
	switch feature.Kind(kind) {
	case feature.KindBoolean:
		if _, err := invalidUnused(
			value.String != "" || value.Integer != 0 || value.Float != 0,
		); err != nil {
			return feature.Value{}, err
		}
		return feature.BooleanValue(value.Boolean), nil
	case feature.KindString:
		if _, err := invalidUnused(
			value.Boolean || value.Integer != 0 || value.Float != 0,
		); err != nil {
			return feature.Value{}, err
		}
		return feature.StringValue(value.String), nil
	case feature.KindInteger:
		if _, err := invalidUnused(
			value.Boolean || value.String != "" || value.Float != 0,
		); err != nil {
			return feature.Value{}, err
		}
		return feature.IntegerValue(value.Integer), nil
	case feature.KindFloat:
		if _, err := invalidUnused(
			value.Boolean || value.String != "" || value.Integer != 0,
		); err != nil {
			return feature.Value{}, err
		}
		return feature.FloatValue(value.Float)
	default:
		return feature.Value{}, fmt.Errorf("%w: variation kind %q is unsupported", feature.ErrInvalidDefinition, kind)
	}
}
