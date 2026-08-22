package policy

import (
	"context"
	"fmt"
	"time"

	kconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/operation"
)

const (
	maxConfigServiceRules = 1024
	maxConfigMatcherRules = 256
	maxConfigMethodRules  = 16 * 1024
)

// RetryConfig is a partial retry policy read from typed configuration.
type RetryConfig struct {
	Enabled     *bool          `config:"enabled"`
	Idempotent  *bool          `config:"idempotent"`
	MaxAttempts *int           `config:"maxAttempts"`
	BackoffMin  *time.Duration `config:"backoffMin"`
	BackoffMax  *time.Duration `config:"backoffMax"`
	BudgetRatio *float64       `config:"budgetRatio"`
}

// HedgingConfig is a partial hedging policy.
type HedgingConfig struct {
	Enabled     *bool          `config:"enabled"`
	Idempotent  *bool          `config:"idempotent"`
	MaxAttempts *int           `config:"maxAttempts"`
	Delay       *time.Duration `config:"delay"`
}

// BreakerConfig is a partial circuit-breaker policy.
type BreakerConfig struct {
	Enabled        *bool          `config:"enabled"`
	FailureRatio   *float64       `config:"failureRatio"`
	Window         *time.Duration `config:"window"`
	MinRequests    *int           `config:"minRequests"`
	OpenTimeout    *time.Duration `config:"openTimeout"`
	HalfOpenProbes *int           `config:"halfOpenProbes"`
}

// BulkheadConfig is a partial dependency-isolation policy.
type BulkheadConfig struct {
	Enabled        *bool          `config:"enabled"`
	MaxConcurrency *int           `config:"maxConcurrency"`
	MaxQueue       *int           `config:"maxQueue"`
	QueueTimeout   *time.Duration `config:"queueTimeout"`
}

// RateLimitConfig is a partial local rate/concurrency policy.
type RateLimitConfig struct {
	Enabled           *bool    `config:"enabled"`
	RequestsPerSecond *float64 `config:"requestsPerSecond"`
	Burst             *int     `config:"burst"`
	MaxConcurrency    *int     `config:"maxConcurrency"`
}

// LoadSheddingConfig is a partial local overload policy.
type LoadSheddingConfig struct {
	Enabled        *bool    `config:"enabled"`
	MaxConcurrency *int     `config:"maxConcurrency"`
	CPUThreshold   *float64 `config:"cpuThreshold"`
}

// StreamConfig is a partial per-stream and shared message policy.
type StreamConfig struct {
	Enabled            *bool    `config:"enabled"`
	MaxSendMessages    *int     `config:"maxSendMessages"`
	MaxReceiveMessages *int     `config:"maxReceiveMessages"`
	MessagesPerSecond  *float64 `config:"messagesPerSecond"`
	Burst              *int     `config:"burst"`
	MaxConcurrency     *int     `config:"maxConcurrency"`
}

// Overrides selectively replaces fields in a base Policy.
type Overrides struct {
	Timeout      *time.Duration      `config:"timeout"`
	Retry        *RetryConfig        `config:"retry"`
	Hedging      *HedgingConfig      `config:"hedging"`
	Breaker      *BreakerConfig      `config:"breaker"`
	Bulkhead     *BulkheadConfig     `config:"bulkhead"`
	RateLimit    *RateLimitConfig    `config:"rateLimit"`
	LoadShedding *LoadSheddingConfig `config:"loadShedding"`
	Stream       *StreamConfig       `config:"stream"`
}

// ServiceConfig applies one policy patch to a logical service.
type ServiceConfig struct {
	Service string    `config:"service"`
	Policy  Overrides `config:"policy"`
}

// MethodConfig applies one policy patch to an exact Operation.
type MethodConfig struct {
	Transport string    `config:"transport"`
	Service   string    `config:"service"`
	Method    string    `config:"method"`
	Kind      string    `config:"kind"`
	Policy    Overrides `config:"policy"`
}

// MatcherConfig applies one policy patch to the first matching Operation.
//
// Transport and kind are exact optional filters. Mode applies to service and
// method and must be exact, prefix, or regexp.
type MatcherConfig struct {
	Name      string    `config:"name"`
	Mode      string    `config:"mode"`
	Transport string    `config:"transport"`
	Service   string    `config:"service"`
	Method    string    `config:"method"`
	Kind      string    `config:"kind"`
	Policy    Overrides `config:"policy"`
}

// Config is the complete typed Method Policy subtree.
type Config struct {
	Global   Overrides       `config:"global"`
	Services []ServiceConfig `config:"services"`
	Matchers []MatcherConfig `config:"matchers"`
	Methods  []MethodConfig  `config:"methods"`
}

// ConfigDescription is a value-free policy binding snapshot.
type ConfigDescription struct {
	Name         string
	Path         string
	Loaded       bool
	Failed       bool
	Revision     string
	ServiceRules int
	MatcherRules int
	MethodRules  int
}

// ConfigBinding validates and atomically publishes Method Policy definitions.
//
// It implements config.Binding. Store is immediately usable with the safe
// default policy and moves to the merged config Snapshot revision after Apply.
type ConfigBinding struct {
	component *kconfig.Component[Config]
	store     *Store
}

// NewConfigBinding constructs a strict hot-reload binding at path.
func NewConfigBinding(name string, path string) (*ConfigBinding, error) {
	component, err := kconfig.NewComponent[Config](
		name,
		path,
		kconfig.WithComponentDefault(Config{}),
		kconfig.WithComponentValidator(func(value Config) error {
			_, err := definitionFromConfig("candidate", value)
			return err
		}),
		kconfig.WithReloadableFields[Config]("global", "services", "matchers", "methods"),
	)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(Definition{
		Revision: "bootstrap",
		Global:   Default(),
	})
	if err != nil {
		return nil, err
	}
	return &ConfigBinding{component: component, store: store}, nil
}

// Name returns the Manager subscriber identity.
func (binding *ConfigBinding) Name() string {
	if binding == nil || binding.component == nil {
		return ""
	}
	return binding.component.Name()
}

// Validate strictly decodes and compiles a candidate definition before
// Manager publication.
func (binding *ConfigBinding) Validate(snapshot kconfig.Snapshot) error {
	if binding == nil || binding.component == nil {
		return fmt.Errorf("%w: policy config binding is nil", ErrInvalidDefinition)
	}
	if !validIdentity(snapshot.Revision()) {
		return invalidDefinition("config revision %q is invalid", snapshot.Revision())
	}
	return binding.component.Validate(snapshot)
}

// Apply publishes the candidate to the typed component and atomic Store.
func (binding *ConfigBinding) Apply(
	ctx context.Context,
	snapshot kconfig.Snapshot,
) error {
	if binding == nil || binding.component == nil || binding.store == nil {
		return fmt.Errorf("%w: policy config binding is nil", ErrInvalidDefinition)
	}
	if err := binding.component.Apply(ctx, snapshot); err != nil {
		return err
	}
	current, ok := binding.component.Current()
	if !ok {
		return fmt.Errorf("%w: policy config was not published", ErrInvalidDefinition)
	}
	definition, err := definitionFromConfig(snapshot.Revision(), current)
	if err != nil {
		return err
	}
	_, err = binding.store.Update(definition)
	return err
}

// Store returns the stable Resolver updated by this binding.
func (binding *ConfigBinding) Store() *Store {
	if binding == nil {
		return nil
	}
	return binding.store
}

// Description returns policy revision and rule counts without policy values.
func (binding *ConfigBinding) Description() ConfigDescription {
	if binding == nil || binding.component == nil || binding.store == nil {
		return ConfigDescription{}
	}
	component := binding.component.Description()
	store := binding.store.Describe()
	return ConfigDescription{
		Name:         component.Name,
		Path:         component.Path,
		Loaded:       component.Loaded,
		Failed:       component.Failed,
		Revision:     store.Revision,
		ServiceRules: store.ServiceRules,
		MatcherRules: store.MatcherRules,
		MethodRules:  store.MethodRules,
	}
}

func definitionFromConfig(revision string, value Config) (Definition, error) {
	if len(value.Services) > maxConfigServiceRules ||
		len(value.Matchers) > maxConfigMatcherRules ||
		len(value.Methods) > maxConfigMethodRules {
		return Definition{}, fmt.Errorf(
			"%w: policy config exceeds rule limits",
			ErrInvalidDefinition,
		)
	}
	globalPatch := value.Global.patch(Default())
	global := globalPatch.apply(Default())
	definition := Definition{
		Revision: revision,
		Global:   global,
		Services: make([]ServiceRule, 0, len(value.Services)),
		Matchers: make([]MatchRule, 0, len(value.Matchers)),
		Methods:  make([]MethodRule, 0, len(value.Methods)),
	}
	servicePolicies := make(map[string]Policy, len(value.Services))
	for _, rule := range value.Services {
		patch := rule.Policy.patch(global)
		definition.Services = append(definition.Services, ServiceRule{
			Service: rule.Service,
			Patch:   patch,
		})
		servicePolicies[rule.Service] = patch.apply(global)
	}
	for _, rule := range value.Matchers {
		matcher, err := operation.CompileMatcher(operation.MatchPattern{
			Mode:      operation.MatchMode(rule.Mode),
			Transport: rule.Transport,
			Service:   rule.Service,
			Method:    rule.Method,
			Kind:      operation.Kind(rule.Kind),
		})
		if err != nil {
			return Definition{}, fmt.Errorf(
				"%w: matcher %q: %w",
				ErrInvalidDefinition,
				rule.Name,
				err,
			)
		}
		matchRule := MatchRule{
			Name:    rule.Name,
			Matcher: matcher,
			Patch:   rule.Policy.patch(global),
		}
		if len(servicePolicies) > 0 {
			matchRule.servicePatches = make(
				map[string]Patch,
				len(servicePolicies),
			)
			for service, base := range servicePolicies {
				matchRule.servicePatches[service] = rule.Policy.patch(base)
			}
		}
		definition.Matchers = append(definition.Matchers, matchRule)
	}
	for _, rule := range value.Methods {
		target, err := operation.New(rule.Transport, rule.Service, rule.Method, operation.Kind(rule.Kind))
		if err != nil {
			return Definition{}, fmt.Errorf(
				"%w: method operation: %w",
				ErrInvalidDefinition,
				err,
			)
		}
		base := global
		if servicePolicy, ok := servicePolicies[target.Service()]; ok {
			base = servicePolicy
		}
		definition.Methods = append(definition.Methods, MethodRule{
			Operation: target,
			Patch:     rule.Policy.patch(base),
		})
	}
	if _, err := compile(definition); err != nil {
		return Definition{}, err
	}
	return definition, nil
}

func (value Overrides) patch(base Policy) Patch {
	var patch Patch
	if value.Timeout != nil {
		patch.Timeout = Some(*value.Timeout)
	}
	if value.Retry != nil {
		resolved := base.Retry
		applyRetryConfig(&resolved, *value.Retry)
		patch.Retry = Some(resolved)
	}
	if value.Hedging != nil {
		resolved := base.Hedging
		applyHedgingConfig(&resolved, *value.Hedging)
		patch.Hedging = Some(resolved)
	}
	if value.Breaker != nil {
		resolved := base.Breaker
		applyBreakerConfig(&resolved, *value.Breaker)
		patch.Breaker = Some(resolved)
	}
	if value.Bulkhead != nil {
		resolved := base.Bulkhead
		applyBulkheadConfig(&resolved, *value.Bulkhead)
		patch.Bulkhead = Some(resolved)
	}
	if value.RateLimit != nil {
		resolved := base.RateLimit
		applyRateLimitConfig(&resolved, *value.RateLimit)
		patch.RateLimit = Some(resolved)
	}
	if value.LoadShedding != nil {
		resolved := base.LoadShedding
		applyLoadSheddingConfig(&resolved, *value.LoadShedding)
		patch.LoadShedding = Some(resolved)
	}
	if value.Stream != nil {
		resolved := base.Stream
		applyStreamConfig(&resolved, *value.Stream)
		patch.Stream = Some(resolved)
	}
	return patch
}

func applyRetryConfig(target *RetryPolicy, value RetryConfig) {
	assign(value.Enabled, &target.Enabled)
	assign(value.Idempotent, &target.Idempotent)
	assign(value.MaxAttempts, &target.MaxAttempts)
	assign(value.BackoffMin, &target.BackoffMin)
	assign(value.BackoffMax, &target.BackoffMax)
	assign(value.BudgetRatio, &target.BudgetRatio)
}

func applyHedgingConfig(target *HedgingPolicy, value HedgingConfig) {
	assign(value.Enabled, &target.Enabled)
	assign(value.Idempotent, &target.Idempotent)
	assign(value.MaxAttempts, &target.MaxAttempts)
	assign(value.Delay, &target.Delay)
}

func applyBreakerConfig(target *BreakerPolicy, value BreakerConfig) {
	assign(value.Enabled, &target.Enabled)
	assign(value.FailureRatio, &target.FailureRatio)
	assign(value.Window, &target.Window)
	assign(value.MinRequests, &target.MinRequests)
	assign(value.OpenTimeout, &target.OpenTimeout)
	assign(value.HalfOpenProbes, &target.HalfOpenProbes)
}

func applyBulkheadConfig(target *BulkheadPolicy, value BulkheadConfig) {
	assign(value.Enabled, &target.Enabled)
	assign(value.MaxConcurrency, &target.MaxConcurrency)
	assign(value.MaxQueue, &target.MaxQueue)
	assign(value.QueueTimeout, &target.QueueTimeout)
}

func applyRateLimitConfig(target *RateLimitPolicy, value RateLimitConfig) {
	assign(value.Enabled, &target.Enabled)
	assign(value.RequestsPerSecond, &target.RequestsPerSecond)
	assign(value.Burst, &target.Burst)
	assign(value.MaxConcurrency, &target.MaxConcurrency)
}

func applyLoadSheddingConfig(
	target *LoadSheddingPolicy,
	value LoadSheddingConfig,
) {
	assign(value.Enabled, &target.Enabled)
	assign(value.MaxConcurrency, &target.MaxConcurrency)
	assign(value.CPUThreshold, &target.CPUThreshold)
}

func applyStreamConfig(target *StreamPolicy, value StreamConfig) {
	assign(value.Enabled, &target.Enabled)
	assign(value.MaxSendMessages, &target.MaxSendMessages)
	assign(value.MaxReceiveMessages, &target.MaxReceiveMessages)
	assign(value.MessagesPerSecond, &target.MessagesPerSecond)
	assign(value.Burst, &target.Burst)
	assign(value.MaxConcurrency, &target.MaxConcurrency)
}

func assign[T any](source *T, target *T) {
	if source != nil {
		*target = *source
	}
}
