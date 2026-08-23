package policy

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/keelab/keelith/operation"
)

// ServiceRule applies a Patch to every operation in a service.
type ServiceRule struct {
	Service string
	Patch   Patch
}

// MethodRule applies a Patch to one exact operation identity.
type MethodRule struct {
	Operation operation.Operation
	Patch     Patch
}

// MatchRule applies Patch to the first matching non-exact Operation.
//
// Rules are evaluated in declaration order after exact MethodRule lookup and
// before returning the service/global base policy.
type MatchRule struct {
	Name    string
	Matcher operation.Matcher
	Patch   Patch

	servicePatches map[string]Patch
}

// Definition is a complete, versioned policy configuration.
type Definition struct {
	Revision string
	Global   Policy
	Services []ServiceRule
	Matchers []MatchRule
	Methods  []MethodRule
}

type compiledMatchRule struct {
	name     string
	matcher  operation.Matcher
	patch    Patch
	services map[string]Patch
}

// Snapshot is an immutable compiled policy definition.
type Snapshot struct {
	revision string
	global   Policy
	services map[string]Policy
	matchers []compiledMatchRule
	methods  map[string]Policy
}

// Revision returns the source revision of the Snapshot.
func (snapshot Snapshot) Revision() string {
	return snapshot.revision
}

// Global returns the validated global policy.
func (snapshot Snapshot) Global() Policy {
	return snapshot.global
}

// Resolve applies exact method, ordered matcher, service, and global precedence.
func (snapshot Snapshot) Resolve(target operation.Operation) Policy {
	key := target.PolicyKey()
	if value, ok := snapshot.methods[key]; ok {
		return value
	}
	base := snapshot.global
	if value, ok := snapshot.services[target.Service()]; ok {
		base = value
	}
	resolved := base
	for _, rule := range snapshot.matchers {
		if safeMatch(rule.matcher, target) {
			patch := rule.patch
			if servicePatch, ok := rule.services[target.Service()]; ok {
				patch = servicePatch
			}
			resolved = patch.apply(base)
			break
		}
	}
	return resolved
}

// Store atomically publishes immutable policy snapshots.
type Store struct {
	updateMu sync.Mutex
	current  atomic.Pointer[Snapshot]
}

// Description is a value-free Store snapshot.
type Description struct {
	Revision     string
	ServiceRules int
	MatcherRules int
	MethodRules  int
}

// NewStore validates definition and creates a Store.
func NewStore(definition Definition) (*Store, error) {
	snapshot, err := compile(definition)
	if err != nil {
		return nil, err
	}
	s := &Store{}
	s.current.Store(&snapshot)
	return s, nil
}

// Update atomically publishes a new revision. A duplicate revision is ignored.
func (s *Store) Update(definition Definition) (bool, error) {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	current := s.current.Load()
	if current != nil && current.revision == definition.Revision {
		return false, nil
	}
	next, err := compile(definition)
	if err != nil {
		return false, err
	}
	s.current.Store(&next)
	return true, nil
}

// Resolve returns the complete policy for target.
func (s *Store) Resolve(target operation.Operation) Policy {
	return s.current.Load().Resolve(target)
}

// Current returns the current immutable Snapshot.
func (s *Store) Current() Snapshot {
	return *s.current.Load()
}

// Describe returns revision and rule cardinality without policy values.
func (s *Store) Describe() Description {
	if s == nil {
		return Description{}
	}
	current := s.current.Load()
	if current == nil {
		return Description{}
	}
	return Description{
		Revision:     current.revision,
		ServiceRules: len(current.services),
		MatcherRules: len(current.matchers),
		MethodRules:  len(current.methods),
	}
}

func compile(definition Definition) (Snapshot, error) {
	if !validIdentity(definition.Revision) {
		return Snapshot{}, invalidDefinition("revision %q is invalid", definition.Revision)
	}
	if err := Validate(definition.Global); err != nil {
		return Snapshot{}, fmt.Errorf("%w: global: %w", ErrInvalidDefinition, err)
	}

	snapshot := Snapshot{
		revision: definition.Revision,
		global:   definition.Global,
		services: make(map[string]Policy, len(definition.Services)),
		matchers: make([]compiledMatchRule, 0, len(definition.Matchers)),
		methods:  make(map[string]Policy, len(definition.Methods)),
	}
	for _, rule := range definition.Services {
		if !validIdentity(rule.Service) {
			return Snapshot{}, invalidDefinition("service %q is invalid", rule.Service)
		}
		if _, exists := snapshot.services[rule.Service]; exists {
			return Snapshot{}, invalidDefinition("service %q is duplicated", rule.Service)
		}
		resolved := rule.Patch.apply(definition.Global)
		if err := Validate(resolved); err != nil {
			return Snapshot{}, fmt.Errorf(
				"%w: service %q: %w",
				ErrInvalidDefinition,
				rule.Service,
				err,
			)
		}
		snapshot.services[rule.Service] = resolved
	}
	matcherNames := make(map[string]struct{}, len(definition.Matchers))
	for _, rule := range definition.Matchers {
		if !validIdentity(rule.Name) {
			return Snapshot{}, invalidDefinition(
				"matcher name %q is invalid",
				rule.Name,
			)
		}
		if _, duplicate := matcherNames[rule.Name]; duplicate {
			return Snapshot{}, invalidDefinition(
				"matcher name %q is duplicated",
				rule.Name,
			)
		}
		if isNilMatcher(rule.Matcher) {
			return Snapshot{}, invalidDefinition(
				"matcher %q is nil",
				rule.Name,
			)
		}
		if err := validateMatchPatch(
			rule.Name,
			rule.Patch,
			rule.servicePatches,
			definition.Global,
			snapshot.services,
		); err != nil {
			return Snapshot{}, err
		}
		matcherNames[rule.Name] = struct{}{}
		snapshot.matchers = append(snapshot.matchers, compiledMatchRule{
			name:     rule.Name,
			matcher:  rule.Matcher,
			patch:    rule.Patch,
			services: cloneMatchPatches(rule.servicePatches),
		})
	}
	for _, rule := range definition.Methods {
		if !validOperation(rule.Operation) {
			return Snapshot{}, invalidDefinition("method operation %q is invalid", rule.Operation.PolicyKey())
		}
		key := rule.Operation.PolicyKey()
		if _, exists := snapshot.methods[key]; exists {
			return Snapshot{}, invalidDefinition("method %q is duplicated", key)
		}
		base := definition.Global
		if service, ok := snapshot.services[rule.Operation.Service()]; ok {
			base = service
		}
		resolved := rule.Patch.apply(base)
		if err := Validate(resolved); err != nil {
			return Snapshot{}, fmt.Errorf(
				"%w: method %q: %w",
				ErrInvalidDefinition,
				key,
				err,
			)
		}
		snapshot.methods[key] = resolved
	}
	return snapshot, nil
}

func validateMatchPatch(
	name string,
	patch Patch,
	servicePatches map[string]Patch,
	global Policy,
	services map[string]Policy,
) error {
	if err := Validate(patch.apply(global)); err != nil {
		return fmt.Errorf(
			"%w: matcher %q with global base: %w",
			ErrInvalidDefinition,
			name,
			err,
		)
	}
	for service, base := range services {
		servicePatch := patch
		if configured, ok := servicePatches[service]; ok {
			servicePatch = configured
		}
		if err := Validate(servicePatch.apply(base)); err != nil {
			return fmt.Errorf(
				"%w: matcher %q with service %q: %w",
				ErrInvalidDefinition,
				name,
				service,
				err,
			)
		}
	}
	return nil
}

func cloneMatchPatches(source map[string]Patch) map[string]Patch {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]Patch, len(source))
	for service, patch := range source {
		cloned[service] = patch
	}
	return cloned
}

func safeMatch(matcher operation.Matcher, target operation.Operation) (
	matched bool,
) {
	defer func() {
		if recover() != nil {
			matched = false
		}
	}()
	return matcher.Match(target)
}

func isNilMatcher(matcher operation.Matcher) bool {
	if matcher == nil {
		return true
	}
	value := reflect.ValueOf(matcher)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validIdentity(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validOperation(target operation.Operation) bool {
	return target.Transport() != "" &&
		validIdentity(target.Service()) &&
		validIdentity(target.Method()) &&
		target.Kind() != ""
}

func invalidDefinition(format string, values ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidDefinition, fmt.Sprintf(format, values...))
}
