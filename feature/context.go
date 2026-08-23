package feature

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxContextAttributes   = 64
	maxAttributeKeyBytes   = 128
	maxAttributeValueBytes = 2 * 1024
	maxTargetingKeyBytes   = 512
	targetingKeyAttribute  = "targeting_key"
)

// EvaluationContext is an immutable set of caller-selected, low-risk
// targeting attributes. Keelith never derives it implicitly from credentials.
type EvaluationContext struct {
	targetingKey string
	attributes   map[string]string
}

// NewEvaluationContext validates and snapshots targeting facts. An empty
// targeting key is valid for fixed rules, but percentage rules will be skipped.
func NewEvaluationContext(targetingKey string, attributes map[string]string) (EvaluationContext, error) {
	if !validContextValue(targetingKey, maxTargetingKeyBytes, true) {
		return EvaluationContext{}, fmt.Errorf(
			"%w: targeting key is malformed",
			ErrInvalidContext,
		)
	}
	if len(attributes) > maxContextAttributes {
		return EvaluationContext{}, fmt.Errorf(
			"%w: attribute count exceeds %d",
			ErrInvalidContext,
			maxContextAttributes,
		)
	}
	snapshot := make(map[string]string, len(attributes))
	for key, value := range attributes {
		if !validAttributeKey(key) || key == targetingKeyAttribute {
			return EvaluationContext{}, fmt.Errorf(
				"%w: attribute key %q is malformed or reserved",
				ErrInvalidContext,
				key,
			)
		}
		if !validContextValue(value, maxAttributeValueBytes, true) {
			return EvaluationContext{}, fmt.Errorf(
				"%w: attribute %q value is malformed",
				ErrInvalidContext,
				key,
			)
		}
		snapshot[key] = value
	}
	return EvaluationContext{
		targetingKey: targetingKey,
		attributes:   snapshot,
	}, nil
}

// TargetingKey returns the stable key used for deterministic percentage
// allocation. It must not be logged by framework diagnostics.
func (evaluation EvaluationContext) TargetingKey() string {
	return evaluation.targetingKey
}

// Attribute returns one exact, case-sensitive attribute.
func (evaluation EvaluationContext) Attribute(key string) (string, bool) {
	if key == targetingKeyAttribute {
		return evaluation.targetingKey, evaluation.targetingKey != ""
	}
	value, exists := evaluation.attributes[key]
	return value, exists
}

type evaluationContextKey struct{}

// WithEvaluationContext attaches immutable feature targeting facts to ctx.
func WithEvaluationContext(ctx context.Context, evaluation EvaluationContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, evaluationContextKey{}, evaluation)
}

// EvaluationContextFromContext returns attached targeting facts.
func EvaluationContextFromContext(ctx context.Context) (EvaluationContext, bool) {
	if ctx == nil {
		return EvaluationContext{}, false
	}
	evaluation, ok := ctx.Value(evaluationContextKey{}).(EvaluationContext)
	return evaluation, ok
}

func validAttributeKey(value string) bool {
	if value == "" || len(value) > maxAttributeKeyBytes || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for index, r := range value {
		if unicode.IsLetter(r) || r == '_' || r == '-' {
			continue
		}
		if index > 0 && (unicode.IsDigit(r) || r == '.') {
			continue
		}
		return false
	}
	return true
}

func validContextValue(value string, maxBytes int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
