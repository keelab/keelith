package authz

import (
	"context"
	"fmt"

	kconfig "github.com/keelab/keelith/config"
)

// RuleConfig is one strictly decoded dynamic RBAC rule.
type RuleConfig struct {
	Service        string   `config:"service"`
	Method         string   `config:"method"`
	AnyRole        []string `config:"anyRole"`
	RequiredScopes []string `config:"requiredScopes"`
}

// Config is one complete dynamic RBAC subtree.
type Config struct {
	Rules []RuleConfig `config:"rules"`
}

// ConfigDescription is a value-free authorization binding snapshot.
type ConfigDescription struct {
	Name        string
	Path        string
	Loaded      bool
	Failed      bool
	Revision    string
	Rules       int
	Updates     uint64
	Evaluations uint64
	Allowed     uint64
	Denied      uint64
}

// ConfigBinding validates complete RBAC revisions before atomically publishing
// them to a stable Store.
type ConfigBinding struct {
	component *kconfig.Component[Config]
	store     *Store
}

// NewConfigBinding constructs a strict hot-reload binding at path. An absent
// subtree publishes an empty fail-closed rule set.
func NewConfigBinding(name string, path string) (*ConfigBinding, error) {
	component, err := kconfig.NewComponent[Config](
		name,
		path,
		kconfig.WithComponentDefault(Config{}),
		kconfig.WithComponentValidator(func(value Config) error {
			_, err := definitionFromConfig("candidate", value)
			return err
		}),
		kconfig.WithReloadableFields[Config]("rules"),
	)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(Definition{Revision: "bootstrap"})
	if err != nil {
		return nil, err
	}
	return &ConfigBinding{component: component, store: store}, nil
}

// Name returns the Config Manager subscriber identity.
func (binding *ConfigBinding) Name() string {
	if binding == nil || binding.component == nil {
		return ""
	}
	return binding.component.Name()
}

// Validate strictly decodes and compiles a candidate before publication.
func (binding *ConfigBinding) Validate(snapshot kconfig.Snapshot) error {
	if binding == nil || binding.component == nil {
		return ErrInvalidAuthorizer
	}
	if !validRevision(snapshot.Revision()) {
		return fmt.Errorf(
			"%w: config revision is malformed",
			ErrInvalidAuthorizer,
		)
	}
	return binding.component.Validate(snapshot)
}

// Apply publishes the typed component and corresponding immutable Store
// snapshot.
func (binding *ConfigBinding) Apply(
	ctx context.Context,
	snapshot kconfig.Snapshot,
) error {
	if binding == nil || binding.component == nil || binding.store == nil {
		return ErrInvalidAuthorizer
	}
	if err := binding.component.Apply(ctx, snapshot); err != nil {
		return err
	}
	current, ok := binding.component.Current()
	if !ok {
		return fmt.Errorf(
			"%w: authorization config was not published",
			ErrInvalidAuthorizer,
		)
	}
	definition, err := definitionFromConfig(snapshot.Revision(), current)
	if err != nil {
		return err
	}
	_, err = binding.store.Update(definition)
	return err
}

// Store returns the stable Authorizer updated by this binding.
func (binding *ConfigBinding) Store() *Store {
	if binding == nil {
		return nil
	}
	return binding.store
}

// Description returns only lifecycle, cardinality, and aggregate decisions.
func (binding *ConfigBinding) Description() ConfigDescription {
	if binding == nil || binding.component == nil || binding.store == nil {
		return ConfigDescription{}
	}
	component := binding.component.Description()
	store := binding.store.Describe()
	return ConfigDescription{
		Name:        component.Name,
		Path:        component.Path,
		Loaded:      component.Loaded,
		Failed:      component.Failed,
		Revision:    store.Revision,
		Rules:       store.Rules,
		Updates:     store.Updates,
		Evaluations: store.Evaluations,
		Allowed:     store.Allowed,
		Denied:      store.Denied,
	}
}

func definitionFromConfig(revision string, value Config) (Definition, error) {
	if len(value.Rules) > maxRules {
		return Definition{}, fmt.Errorf(
			"%w: rule count exceeds %d",
			ErrInvalidAuthorizer,
			maxRules,
		)
	}
	definition := Definition{
		Revision: revision,
		Rules:    make([]Rule, len(value.Rules)),
	}
	for index, rule := range value.Rules {
		definition.Rules[index] = Rule{
			Service:       rule.Service,
			Method:        rule.Method,
			AnyRole:       append([]string(nil), rule.AnyRole...),
			RequiredScope: append([]string(nil), rule.RequiredScopes...),
		}
	}
	if _, err := compileDefinition(definition); err != nil {
		return Definition{}, err
	}
	return definition, nil
}
