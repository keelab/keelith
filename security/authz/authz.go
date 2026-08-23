// Package authz provides transport-neutral authorization middleware and a
// small exact/wildcard RBAC policy.
package authz

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/security"
)

const (
	// DeniedReason is the stable permission-denied reason.
	DeniedReason = "PERMISSION_DENIED"
	// DeniedCode is the transport-neutral permission-denied code.
	DeniedCode        int32 = 403
	maxRules                = 8 * 1024
	maxRuleValues           = 64
	maxRuleValueBytes       = 256
)

var (
	// ErrInvalidAuthorizer reports a nil or malformed authorization policy.
	ErrInvalidAuthorizer = errors.New("authz: invalid authorizer")
	// ErrPrincipalMissing reports authorization without AuthN context.
	ErrPrincipalMissing = errors.New("authz: principal is missing")
	// ErrOperationMissing reports authorization without operation context.
	ErrOperationMissing = errors.New("authz: operation is missing")
)

// Decision is an explainable authorization result.
type Decision struct {
	Allowed bool
	Reason  string
}

// Authorizer evaluates one principal and stable operation.
type Authorizer interface {
	Authorize(
		context.Context,
		security.Principal,
		operation.Operation,
		any,
	) (Decision, error)
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(
	context.Context,
	security.Principal,
	operation.Operation,
	any,
) (Decision, error)

// Authorize delegates to fn.
func (fn AuthorizerFunc) Authorize(
	ctx context.Context,
	principal security.Principal,
	target operation.Operation,
	request any,
) (Decision, error) {
	return fn(ctx, principal, target, request)
}

// Middleware enforces authorization after authentication.
func Middleware(authorizer Authorizer) (middleware.Middleware, error) {
	if isNil(authorizer) {
		return nil, ErrInvalidAuthorizer
	}
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			principal, ok := security.PrincipalFromContext(ctx)
			if !ok {
				return nil, kerrors.Wrap(
					ErrPrincipalMissing,
					401,
					"UNAUTHENTICATED",
					"authentication is required",
				)
			}
			if err := principal.Validate(time.Now()); err != nil {
				return nil, kerrors.Wrap(
					err,
					401,
					"UNAUTHENTICATED",
					"authentication is required",
				)
			}
			target, ok := operation.FromContext(ctx)
			if !ok {
				return nil, kerrors.Wrap(
					ErrOperationMissing,
					500,
					"AUTHORIZATION_CONTEXT_MISSING",
					"authorization context is incomplete",
				)
			}
			decision, err := authorizer.Authorize(
				ctx,
				principal,
				target,
				request,
			)
			if err != nil {
				return nil, kerrors.Wrap(
					err,
					500,
					"AUTHORIZATION_FAILED",
					"authorization evaluation failed",
				)
			}
			if !decision.Allowed {
				return nil, kerrors.New(
					DeniedCode,
					DeniedReason,
					"permission denied",
				)
			}
			return next(ctx, request)
		}
	}, nil
}

// Rule matches service and method exactly or with "*".
type Rule struct {
	Service       string
	Method        string
	AnyRole       []string
	RequiredScope []string
}

// RBAC evaluates rules in declaration order and denies unmatched operations.
type RBAC struct {
	rules []Rule
}

// NewRBAC validates and snapshots ordered rules.
func NewRBAC(rules ...Rule) (*RBAC, error) {
	if len(rules) > maxRules {
		return nil, fmt.Errorf(
			"%w: rule count exceeds %d",
			ErrInvalidAuthorizer,
			maxRules,
		)
	}
	snapshot := make([]Rule, len(rules))
	for index, rule := range rules {
		if !validMatcher(rule.Service) || !validMatcher(rule.Method) {
			return nil, fmt.Errorf(
				"%w: rule %d matcher",
				ErrInvalidAuthorizer,
				index,
			)
		}
		roles, err := normalized(rule.AnyRole)
		if err != nil {
			return nil, fmt.Errorf("authz: rule %d roles: %w", index, err)
		}
		scopes, err := normalized(rule.RequiredScope)
		if err != nil {
			return nil, fmt.Errorf("authz: rule %d scopes: %w", index, err)
		}
		rule.AnyRole = roles
		rule.RequiredScope = scopes
		snapshot[index] = rule
	}
	return &RBAC{rules: snapshot}, nil
}

// Authorize allows the first matching rule whose role/scope requirements are
// satisfied.
func (rbac *RBAC) Authorize(
	_ context.Context,
	principal security.Principal,
	target operation.Operation,
	_ any,
) (Decision, error) {
	matched := false
	reason := "no matching rbac rule"
	for _, rule := range rbac.rules {
		if !matches(rule.Service, target.Service()) ||
			!matches(rule.Method, target.Method()) {
			continue
		}
		matched = true
		if len(rule.AnyRole) > 0 && !hasAnyRole(principal, rule.AnyRole) {
			reason = "required role is missing"
			continue
		}
		missingScope := false
		for _, scope := range rule.RequiredScope {
			if !principal.HasScope(scope) {
				missingScope = true
				break
			}
		}
		if missingScope {
			reason = "required scope is missing"
			continue
		}
		return Decision{Allowed: true, Reason: "matched rbac rule"}, nil
	}
	if !matched {
		reason = "no matching rbac rule"
	}
	return Decision{Reason: reason}, nil
}

func hasAnyRole(principal security.Principal, roles []string) bool {
	for _, role := range roles {
		if principal.HasRole(role) {
			return true
		}
	}
	return false
}

func validMatcher(value string) bool {
	return value == "*" ||
		value != "" &&
			len(value) <= maxRuleValueBytes &&
			strings.TrimSpace(value) == value &&
			!strings.ContainsAny(value, "\r\n\x00") &&
			!strings.Contains(value, "*")
}

func matches(matcher, value string) bool {
	return matcher == "*" || matcher == value
}

func normalized(values []string) ([]string, error) {
	if len(values) > maxRuleValues {
		return nil, ErrInvalidAuthorizer
	}
	result := append([]string(nil), values...)
	for index, value := range result {
		if value == "" ||
			len(value) > maxRuleValueBytes ||
			strings.TrimSpace(value) != value ||
			strings.ContainsAny(value, "\r\n\x00") {
			return nil, ErrInvalidAuthorizer
		}
		if index > 0 && value == result[index-1] {
			return nil, ErrInvalidAuthorizer
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, ErrInvalidAuthorizer
		}
	}
	return result, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
