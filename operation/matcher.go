package operation

import (
	"fmt"
	"regexp"
	"strings"
)

const maxMatcherExpressionBytes = 512

// MatchMode controls how non-empty service and method expressions match.
type MatchMode string

const (
	// MatchExact requires complete field equality.
	MatchExact MatchMode = "exact"
	// MatchPrefix requires the operation field to start with the expression.
	MatchPrefix MatchMode = "prefix"
	// MatchRegexp evaluates the expression as a regular expression.
	MatchRegexp MatchMode = "regexp"
)

// Matcher decides whether one immutable Operation belongs to a policy group.
//
// Implementations must be deterministic, concurrency-safe, and fast. Match
// must not mutate target or depend on request payload.
type Matcher interface {
	Match(Operation) bool
}

// MatcherFunc adapts a deterministic function to Matcher.
type MatcherFunc func(Operation) bool

// Match invokes function.
func (function MatcherFunc) Match(target Operation) bool {
	return function != nil && function(target)
}

// MatchPattern describes a bounded field-level Operation matcher.
//
// Transport and Kind are optional exact filters. Mode applies independently
// to non-empty Service and Method expressions. At least one field must be set.
type MatchPattern struct {
	Mode      MatchMode
	Transport string
	Service   string
	Method    string
	Kind      Kind
}

type compiledMatcher struct {
	mode      MatchMode
	transport string
	service   string
	method    string
	kind      Kind
	serviceRE *regexp.Regexp
	methodRE  *regexp.Regexp
}

// CompileMatcher validates and compiles a field-level matcher.
func CompileMatcher(pattern MatchPattern) (Matcher, error) {
	mode := pattern.Mode
	if mode == "" {
		mode = MatchExact
	}
	if mode != MatchExact && mode != MatchPrefix && mode != MatchRegexp {
		return nil, fmt.Errorf("%w: matcher mode %q is invalid", ErrInvalid, pattern.Mode)
	}
	transport := strings.ToLower(pattern.Transport)
	if pattern.Transport != "" &&
		(transport != pattern.Transport || !validTransport(transport)) {
		return nil, fmt.Errorf("%w: matcher transport %q is invalid", ErrInvalid, pattern.Transport)
	}
	if pattern.Kind != "" && !validKind(pattern.Kind) {
		return nil, fmt.Errorf("%w: matcher kind %q is invalid", ErrInvalid, pattern.Kind)
	}
	if pattern.Service == "" &&
		pattern.Method == "" &&
		transport == "" &&
		pattern.Kind == "" {
		return nil, fmt.Errorf("%w: matcher has no filters", ErrInvalid)
	}
	if err := validateMatcherExpression(pattern.Service); err != nil {
		return nil, fmt.Errorf("%w: service matcher: %w", ErrInvalid, err)
	}
	if err := validateMatcherExpression(pattern.Method); err != nil {
		return nil, fmt.Errorf("%w: method matcher: %w", ErrInvalid, err)
	}
	matcher := &compiledMatcher{
		mode:      mode,
		transport: transport,
		service:   pattern.Service,
		method:    pattern.Method,
		kind:      pattern.Kind,
	}
	if mode == MatchRegexp {
		var err error
		if matcher.service != "" {
			matcher.serviceRE, err = regexp.Compile(matcher.service)
			if err != nil {
				return nil, fmt.Errorf("%w: service regexp: %w", ErrInvalid, err)
			}
		}
		if matcher.method != "" {
			matcher.methodRE, err = regexp.Compile(matcher.method)
			if err != nil {
				return nil, fmt.Errorf("%w: method regexp: %w", ErrInvalid, err)
			}
		}
	}
	return matcher, nil
}

func (matcher *compiledMatcher) Match(target Operation) bool {
	if matcher == nil {
		return false
	}
	if matcher.transport != "" && target.transport != matcher.transport {
		return false
	}
	if matcher.kind != "" && target.kind != matcher.kind {
		return false
	}
	switch matcher.mode {
	case MatchExact:
		return matchExact(matcher.service, target.service) &&
			matchExact(matcher.method, target.method)
	case MatchPrefix:
		return matchPrefix(matcher.service, target.service) &&
			matchPrefix(matcher.method, target.method)
	case MatchRegexp:
		return matchRegexp(matcher.serviceRE, target.service) &&
			matchRegexp(matcher.methodRE, target.method)
	default:
		return false
	}
}

func validateMatcherExpression(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxMatcherExpressionBytes ||
		strings.TrimSpace(value) != value ||
		!validComponent(value) {
		return fmt.Errorf("expression is oversized or malformed")
	}
	return nil
}

func matchExact(expression, value string) bool {
	return expression == "" || expression == value
}

func matchPrefix(expression, value string) bool {
	return expression == "" || strings.HasPrefix(value, expression)
}

func matchRegexp(expression *regexp.Regexp, value string) bool {
	return expression == nil || expression.MatchString(value)
}
