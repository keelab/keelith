package feature

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

const (
	maxFlags              = 1024
	maxVariationsPerFlag  = 64
	maxRulesPerFlag       = 64
	maxConditionsPerRule  = 16
	maxConditionValues    = 32
	maxIdentityBytes      = 256
	percentageBasisPoints = 10_000
)

// Operator selects one bounded string comparison for a rule condition.
type Operator string

// Supported condition operators.
const (
	OperatorExists    Operator = "exists"
	OperatorNotExists Operator = "not_exists"
	OperatorEquals    Operator = "equals"
	OperatorNotEquals Operator = "not_equals"
	OperatorOneOf     Operator = "one_of"
	OperatorNotOneOf  Operator = "not_one_of"
	OperatorPrefix    Operator = "prefix"
	OperatorSuffix    Operator = "suffix"
)

// Reason explains how a value was selected without exposing context values.
type Reason string

// Evaluation selection reasons.
const (
	ReasonDefault    Reason = "default"
	ReasonRule       Reason = "rule"
	ReasonPercentage Reason = "percentage"
	ReasonFallback   Reason = "fallback"
)

// ErrorCode is a bounded, value-free evaluation failure classification.
type ErrorCode string

// Evaluation error codes.
const (
	ErrorNone           ErrorCode = ""
	ErrorInvalidContext ErrorCode = "invalid_context"
	ErrorFlagNotFound   ErrorCode = "flag_not_found"
	ErrorTypeMismatch   ErrorCode = "type_mismatch"
	ErrorNotReady       ErrorCode = "not_ready"
	ErrorCanceled       ErrorCode = "canceled"
)

// Definition is one complete revisioned feature flag set.
type Definition struct {
	Revision string
	Flags    []Flag
}

// Flag declares named variations, ordered rules, and a safe default.
type Flag struct {
	Key              string
	DefaultVariation string
	Variations       []Variation
	Rules            []Rule
}

// Variation associates a stable name with one typed value.
type Variation struct {
	Name  string
	Value Value
}

// Rule selects either one fixed Variation or one percentage Rollout when all
// Conditions match. Percentage rules are skipped without a targeting key.
type Rule struct {
	Name       string
	Conditions []Condition
	Variation  string
	Rollout    []Allocation
}

// Condition compares one targeting attribute using an Operator.
type Condition struct {
	Attribute string
	Operator  Operator
	Values    []string
}

// Allocation assigns basis points to one Variation. A rollout must total
// exactly 10,000 basis points.
type Allocation struct {
	Variation   string
	BasisPoints uint16
}

// Details is a value-free explanation of an evaluation.
type Details struct {
	Reason    Reason
	Variant   string
	Revision  string
	ErrorCode ErrorCode
}

// Evaluation contains the selected or caller-provided fallback Value.
type Evaluation struct {
	Value   Value
	Details Details
}

// Description exposes only revision, bounded counts, and aggregate counters.
type Description struct {
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

type compiledRule struct {
	name       string
	conditions []Condition
	variation  string
	rollout    []Allocation
}

type compiledFlag struct {
	kind       Kind
	defaultVar string
	variations map[string]Value
	rules      []compiledRule
}

type compiledSnapshot struct {
	revision string
	flags    map[string]compiledFlag
	rules    int
}

// Store atomically publishes complete immutable flag revisions and evaluates
// the hot path without locks.
type Store struct {
	updateMu sync.Mutex
	current  atomic.Pointer[compiledSnapshot]

	updates           atomic.Uint64
	evaluations       atomic.Uint64
	defaults          atomic.Uint64
	rulesMatched      atomic.Uint64
	percentageMatched atomic.Uint64
	fallbacks         atomic.Uint64
}

// NewStore validates and publishes an initial complete Definition.
func NewStore(definition Definition) (*Store, error) {
	snapshot, err := compileDefinition(definition)
	if err != nil {
		return nil, err
	}
	store := &Store{}
	store.current.Store(&snapshot)
	return store, nil
}

// Update atomically publishes a complete Definition. An invalid candidate
// leaves the current revision untouched, and an identical revision is ignored.
func (store *Store) Update(definition Definition) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("%w: store is nil", ErrInvalidDefinition)
	}
	store.updateMu.Lock()
	defer store.updateMu.Unlock()
	current := store.current.Load()
	if current != nil && current.revision == definition.Revision {
		return false, nil
	}
	next, err := compileDefinition(definition)
	if err != nil {
		return false, err
	}
	store.current.Store(&next)
	store.updates.Add(1)
	return true, nil
}

// Evaluate selects a typed Value from the active immutable snapshot. All
// failures return the caller-owned fallback with a bounded ErrorCode.
func (store *Store) Evaluate(
	ctx context.Context,
	key string,
	fallback Value,
) Evaluation {
	if store == nil || store.current.Load() == nil {
		return fallbackEvaluation(fallback, ErrorNotReady)
	}
	store.evaluations.Add(1)
	if ctx == nil {
		return store.fallback(fallback, ErrorInvalidContext)
	}
	if context.Cause(ctx) != nil {
		return store.fallback(fallback, ErrorCanceled)
	}
	snapshot := store.current.Load()
	flag, exists := snapshot.flags[key]
	if !exists {
		return store.fallbackAt(snapshot, fallback, ErrorFlagNotFound)
	}
	if !fallback.valid() || fallback.Kind() != flag.kind {
		return store.fallbackAt(snapshot, fallback, ErrorTypeMismatch)
	}
	evaluation, _ := EvaluationContextFromContext(ctx)
	for _, rule := range flag.rules {
		if !matchesAll(evaluation, rule.conditions) {
			continue
		}
		if rule.variation != "" {
			store.rulesMatched.Add(1)
			return Evaluation{
				Value: flag.variations[rule.variation],
				Details: Details{
					Reason:   ReasonRule,
					Variant:  rule.variation,
					Revision: snapshot.revision,
				},
			}
		}
		if evaluation.targetingKey == "" {
			continue
		}
		variant := rolloutVariation(key, rule, evaluation.targetingKey)
		store.percentageMatched.Add(1)
		return Evaluation{
			Value: flag.variations[variant],
			Details: Details{
				Reason:   ReasonPercentage,
				Variant:  variant,
				Revision: snapshot.revision,
			},
		}
	}
	store.defaults.Add(1)
	return Evaluation{
		Value: flag.variations[flag.defaultVar],
		Details: Details{
			Reason:   ReasonDefault,
			Variant:  flag.defaultVar,
			Revision: snapshot.revision,
		},
	}
}

// Boolean evaluates one Boolean flag.
func (store *Store) Boolean(ctx context.Context, key string, fallback bool) (bool, Details) {
	result := store.Evaluate(ctx, key, BooleanValue(fallback))
	value, ok := result.Value.Boolean()
	if !ok {
		return fallback, result.Details
	}
	return value, result.Details
}

// String evaluates one String flag.
func (store *Store) String(ctx context.Context, key string, fallback string) (string, Details) {
	result := store.Evaluate(ctx, key, StringValue(fallback))
	value, ok := result.Value.String()
	if !ok {
		return fallback, result.Details
	}
	return value, result.Details
}

// Integer evaluates one Integer flag.
func (store *Store) Integer(ctx context.Context, key string, fallback int64) (int64, Details) {
	result := store.Evaluate(ctx, key, IntegerValue(fallback))
	value, ok := result.Value.Integer()
	if !ok {
		return fallback, result.Details
	}
	return value, result.Details
}

// Float evaluates one Float flag. A non-finite fallback is rejected as a type
// mismatch and returned unchanged.
func (store *Store) Float(ctx context.Context, key string, fallback float64) (float64, Details) {
	fallbackValue, err := FloatValue(fallback)
	if err != nil {
		return fallback, Details{Reason: ReasonFallback, ErrorCode: ErrorTypeMismatch}
	}
	result := store.Evaluate(ctx, key, fallbackValue)
	value, ok := result.Value.Float()
	if !ok {
		return fallback, result.Details
	}
	return value, result.Details
}

// Describe returns value-free snapshot and aggregate evaluation diagnostics.
func (store *Store) Describe() Description {
	if store == nil {
		return Description{}
	}
	description := Description{
		Updates:           store.updates.Load(),
		Evaluations:       store.evaluations.Load(),
		Defaults:          store.defaults.Load(),
		RulesMatched:      store.rulesMatched.Load(),
		PercentageMatched: store.percentageMatched.Load(),
		Fallbacks:         store.fallbacks.Load(),
	}
	if snapshot := store.current.Load(); snapshot != nil {
		description.Revision = snapshot.revision
		description.Flags = len(snapshot.flags)
		description.Rules = snapshot.rules
	}
	return description
}

func (store *Store) fallback(value Value, code ErrorCode) Evaluation {
	store.fallbacks.Add(1)
	return fallbackEvaluation(value, code)
}

func (store *Store) fallbackAt(snapshot *compiledSnapshot, value Value, code ErrorCode) Evaluation {
	result := store.fallback(value, code)
	if snapshot != nil {
		result.Details.Revision = snapshot.revision
	}
	return result
}

func fallbackEvaluation(value Value, code ErrorCode) Evaluation {
	return Evaluation{
		Value:   value,
		Details: Details{Reason: ReasonFallback, ErrorCode: code},
	}
}

func compileDefinition(definition Definition) (compiledSnapshot, error) {
	if !validIdentity(definition.Revision) {
		return compiledSnapshot{}, invalidDefinition("revision is malformed")
	}
	if len(definition.Flags) > maxFlags {
		return compiledSnapshot{}, invalidDefinition("flag count exceeds %d", maxFlags)
	}
	snapshot := compiledSnapshot{
		revision: definition.Revision,
		flags:    make(map[string]compiledFlag, len(definition.Flags)),
	}
	for _, definitionFlag := range definition.Flags {
		if !validFlagKey(definitionFlag.Key) {
			return compiledSnapshot{}, invalidDefinition("flag key is malformed")
		}
		if _, duplicate := snapshot.flags[definitionFlag.Key]; duplicate {
			return compiledSnapshot{}, invalidDefinition("duplicate flag %q", definitionFlag.Key)
		}
		flag, err := compileFlag(definitionFlag)
		if err != nil {
			return compiledSnapshot{}, fmt.Errorf("flag %q: %w", definitionFlag.Key, err)
		}
		snapshot.flags[definitionFlag.Key] = flag
		snapshot.rules += len(flag.rules)
	}
	return snapshot, nil
}

func compileFlag(definition Flag) (compiledFlag, error) {
	if len(definition.Variations) == 0 ||
		len(definition.Variations) > maxVariationsPerFlag {
		return compiledFlag{}, invalidDefinition(
			"variation count must be between 1 and %d",
			maxVariationsPerFlag,
		)
	}
	if len(definition.Rules) > maxRulesPerFlag {
		return compiledFlag{}, invalidDefinition("rule count exceeds %d", maxRulesPerFlag)
	}
	result := compiledFlag{
		defaultVar: definition.DefaultVariation,
		variations: make(map[string]Value, len(definition.Variations)),
		rules:      make([]compiledRule, 0, len(definition.Rules)),
	}
	for index, variation := range definition.Variations {
		if !validIdentity(variation.Name) || !variation.Value.valid() {
			return compiledFlag{}, invalidDefinition("variation %d is malformed", index)
		}
		if _, duplicate := result.variations[variation.Name]; duplicate {
			return compiledFlag{}, invalidDefinition("duplicate variation %q", variation.Name)
		}
		if index == 0 {
			result.kind = variation.Value.Kind()
		} else if variation.Value.Kind() != result.kind {
			return compiledFlag{}, invalidDefinition("variations must have one value kind")
		}
		result.variations[variation.Name] = variation.Value
	}
	if _, exists := result.variations[result.defaultVar]; !exists {
		return compiledFlag{}, invalidDefinition("default variation does not exist")
	}
	ruleNames := make(map[string]struct{}, len(definition.Rules))
	for index, rule := range definition.Rules {
		compiled, err := compileRule(rule, result.variations)
		if err != nil {
			return compiledFlag{}, fmt.Errorf("rule %d: %w", index, err)
		}
		if _, duplicate := ruleNames[compiled.name]; duplicate {
			return compiledFlag{}, invalidDefinition("duplicate rule %q", compiled.name)
		}
		ruleNames[compiled.name] = struct{}{}
		result.rules = append(result.rules, compiled)
	}
	return result, nil
}

func compileRule(rule Rule, variations map[string]Value) (compiledRule, error) {
	if !validIdentity(rule.Name) {
		return compiledRule{}, invalidDefinition("name is malformed")
	}
	if len(rule.Conditions) == 0 || len(rule.Conditions) > maxConditionsPerRule {
		return compiledRule{}, invalidDefinition(
			"condition count must be between 1 and %d",
			maxConditionsPerRule,
		)
	}
	if (rule.Variation == "") == (len(rule.Rollout) == 0) {
		return compiledRule{}, invalidDefinition(
			"exactly one fixed variation or percentage rollout is required",
		)
	}
	result := compiledRule{
		name:       rule.Name,
		conditions: make([]Condition, len(rule.Conditions)),
		variation:  rule.Variation,
		rollout:    append([]Allocation(nil), rule.Rollout...),
	}
	for index, condition := range rule.Conditions {
		if err := validateCondition(condition); err != nil {
			return compiledRule{}, fmt.Errorf("condition %d: %w", index, err)
		}
		result.conditions[index] = Condition{
			Attribute: condition.Attribute,
			Operator:  condition.Operator,
			Values:    append([]string(nil), condition.Values...),
		}
	}
	if rule.Variation != "" {
		if _, exists := variations[rule.Variation]; !exists {
			return compiledRule{}, invalidDefinition("fixed variation does not exist")
		}
		return result, nil
	}
	total := 0
	for index, allocation := range result.rollout {
		if allocation.BasisPoints == 0 {
			return compiledRule{}, invalidDefinition("allocation %d has zero weight", index)
		}
		if _, exists := variations[allocation.Variation]; !exists {
			return compiledRule{}, invalidDefinition("allocation %d variation does not exist", index)
		}
		total += int(allocation.BasisPoints)
	}
	if total != percentageBasisPoints {
		return compiledRule{}, invalidDefinition(
			"rollout totals %d basis points instead of %d",
			total,
			percentageBasisPoints,
		)
	}
	return result, nil
}

func validateCondition(condition Condition) error {
	if !validAttributeKey(condition.Attribute) && condition.Attribute != targetingKeyAttribute {
		return invalidDefinition("attribute is malformed")
	}
	if len(condition.Values) > maxConditionValues {
		return invalidDefinition("value count exceeds %d", maxConditionValues)
	}
	for _, value := range condition.Values {
		if !validContextValue(value, maxAttributeValueBytes, true) {
			return invalidDefinition("comparison value is malformed")
		}
	}
	switch condition.Operator {
	case OperatorExists, OperatorNotExists:
		if len(condition.Values) != 0 {
			return invalidDefinition("existence operator does not accept values")
		}
	case OperatorEquals, OperatorNotEquals, OperatorPrefix, OperatorSuffix:
		if len(condition.Values) != 1 {
			return invalidDefinition("comparison operator requires one value")
		}
	case OperatorOneOf, OperatorNotOneOf:
		if len(condition.Values) == 0 {
			return invalidDefinition("set operator requires at least one value")
		}
	default:
		return invalidDefinition("operator %q is unsupported", condition.Operator)
	}
	return nil
}

func matchesAll(evaluation EvaluationContext, conditions []Condition) bool {
	for _, condition := range conditions {
		value, exists := evaluation.Attribute(condition.Attribute)
		if !matchesCondition(value, exists, condition) {
			return false
		}
	}
	return true
}

func matchesCondition(value string, exists bool, condition Condition) bool {
	switch condition.Operator {
	case OperatorExists:
		return exists
	case OperatorNotExists:
		return !exists
	}
	if !exists {
		return false
	}
	switch condition.Operator {
	case OperatorEquals:
		return value == condition.Values[0]
	case OperatorNotEquals:
		return value != condition.Values[0]
	case OperatorPrefix:
		return strings.HasPrefix(value, condition.Values[0])
	case OperatorSuffix:
		return strings.HasSuffix(value, condition.Values[0])
	case OperatorOneOf, OperatorNotOneOf:
		matched := slices.Contains(condition.Values, value)
		if condition.Operator == OperatorNotOneOf {
			return !matched
		}
		return matched
	default:
		return false
	}
}

func rolloutVariation(flagKey string, rule compiledRule, targetingKey string) string {
	digest := sha256.Sum256([]byte(flagKey + "\x00" + rule.name + "\x00" + targetingKey))
	bucket := int(binary.BigEndian.Uint64(digest[:8]) % percentageBasisPoints)
	upper := 0
	for _, allocation := range rule.rollout {
		upper += int(allocation.BasisPoints)
		if bucket < upper {
			return allocation.Variation
		}
	}
	return rule.rollout[len(rule.rollout)-1].Variation
}

func validFlagKey(value string) bool {
	if !validIdentity(value) {
		return false
	}
	for index, character := range value {
		if unicode.IsLower(character) || character == '-' || character == '_' {
			continue
		}
		if index > 0 && (unicode.IsDigit(character) || character == '.') {
			continue
		}
		return false
	}
	return true
}

func validIdentity(value string) bool {
	if value == "" || len(value) > maxIdentityBytes || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func invalidDefinition(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidDefinition, fmt.Sprintf(format, arguments...))
}
