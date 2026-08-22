package continuation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/security"
	"github.com/keelab/keelith/security/authz"
)

const (
	maximumCallACLRules      = 1024
	maximumCallACLPrincipals = 64
	maximumCallACLValues     = 64
	maximumCallACLValueBytes = 256
)

var (
	// ErrInvalidService reports an incomplete runtime or access policy.
	ErrInvalidService = errors.New("continuation: invalid service")
	// ErrAuthenticationRequired reports a protected call without a principal.
	ErrAuthenticationRequired = errors.New(
		"continuation: authentication required",
	)
	// ErrAccessDenied reports a resource-level continuation authorization deny.
	ErrAccessDenied = errors.New("continuation: access denied")
	// ErrAuthorizationFailed reports an Authorizer failure or panic.
	ErrAuthorizationFailed = errors.New("continuation: authorization failed")
	// ErrOwnershipConflict reports a CallID already bound to another principal
	// or operation.
	ErrOwnershipConflict = errors.New("continuation: ownership conflict")
	// ErrOwnershipNotFound reports an absent durable access binding.
	ErrOwnershipNotFound = errors.New("continuation: ownership not found")
)

// Action is one fixed resource-level continuation permission.
type Action string

const (
	// ActionStart permits creating and binding a new durable call.
	ActionStart Action = "Start"
	// ActionAttach permits observing the live bounded event stream.
	ActionAttach Action = "Attach"
	// ActionSignal permits submitting one idempotent signal command.
	ActionSignal Action = "Signal"
	// ActionCancel permits requesting cooperative cancellation.
	ActionCancel Action = "Cancel"
	// ActionHistory permits reading bounded historical frames.
	ActionHistory Action = "History"
	// ActionHistoryDetail permits reading separately budgeted frame payloads.
	ActionHistoryDetail Action = "HistoryDetail"
)

// AuthorizationRequest is the bounded resource value passed to Authorizer.
type AuthorizationRequest struct {
	CallID    CallID
	Operation Operation
	Action    Action
}

// PrincipalIdentity matches one issuer-qualified authenticated subject.
type PrincipalIdentity struct {
	Issuer  string
	Subject string
}

// Matches reports whether principal has the exact snapshotted identity.
func (identity PrincipalIdentity) Matches(principal security.Principal) bool {
	return identity.Issuer == principal.Issuer() &&
		identity.Subject == principal.Subject()
}

// CallACLRule grants actions on an exact or wildcard CallID and durable
// Operation. Principal, role and scope constraints are all conjunctive.
type CallACLRule struct {
	CallID        string
	Operation     string
	Actions       []Action
	Principals    []PrincipalIdentity
	AnyRole       []string
	RequiredScope []string
}

type callACLRule struct {
	callID     string
	operation  string
	actions    map[Action]struct{}
	principals []PrincipalIdentity
	policy     *authz.RBAC
}

// CallACL is an immutable ordered resource-level authorization policy.
type CallACL struct {
	rules []callACLRule
}

// NewCallACL validates and snapshots exact continuation resource rules.
func NewCallACL(rules ...CallACLRule) (*CallACL, error) {
	if len(rules) == 0 || len(rules) > maximumCallACLRules {
		return nil, ErrInvalidService
	}
	result := &CallACL{rules: make([]callACLRule, len(rules))}
	for index, rule := range rules {
		if !validCallIDMatcher(rule.CallID) ||
			!validOperationMatcher(rule.Operation) ||
			len(rule.Actions) == 0 ||
			len(rule.Actions) > len(validActions()) ||
			len(rule.Principals) > maximumCallACLPrincipals {
			return nil, fmt.Errorf(
				"%w: ACL rule %d resource",
				ErrInvalidService,
				index,
			)
		}
		actions := make(map[Action]struct{}, len(rule.Actions))
		for _, action := range rule.Actions {
			if !action.valid() {
				return nil, fmt.Errorf(
					"%w: ACL rule %d action",
					ErrInvalidService,
					index,
				)
			}
			if _, duplicate := actions[action]; duplicate {
				return nil, fmt.Errorf(
					"%w: ACL rule %d duplicate action",
					ErrInvalidService,
					index,
				)
			}
			actions[action] = struct{}{}
		}
		principals := append([]PrincipalIdentity(nil), rule.Principals...)
		for principalIndex, identity := range principals {
			if !validACLValue(identity.Subject, true) ||
				!validACLValue(identity.Issuer, false) {
				return nil, fmt.Errorf(
					"%w: ACL rule %d principal %d",
					ErrInvalidService,
					index,
					principalIndex,
				)
			}
		}
		sort.Slice(principals, func(first, second int) bool {
			if principals[first].Issuer != principals[second].Issuer {
				return principals[first].Issuer < principals[second].Issuer
			}
			return principals[first].Subject < principals[second].Subject
		})
		for principalIndex := 1; principalIndex < len(principals); principalIndex++ {
			if principals[principalIndex] == principals[principalIndex-1] {
				return nil, fmt.Errorf(
					"%w: ACL rule %d duplicate principal",
					ErrInvalidService,
					index,
				)
			}
		}
		if len(rule.AnyRole) > maximumCallACLValues ||
			len(rule.RequiredScope) > maximumCallACLValues {
			return nil, fmt.Errorf(
				"%w: ACL rule %d claims",
				ErrInvalidService,
				index,
			)
		}
		policy, err := authz.NewRBAC(authz.Rule{
			Service:       "*",
			Method:        "*",
			AnyRole:       rule.AnyRole,
			RequiredScope: rule.RequiredScope,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"%w: ACL rule %d claims",
				ErrInvalidService,
				index,
			)
		}
		result.rules[index] = callACLRule{
			callID:     rule.CallID,
			operation:  rule.Operation,
			actions:    actions,
			principals: principals,
			policy:     policy,
		}
	}
	return result, nil
}

// Authorize implements authz.Authorizer and denies unmatched resources.
func (acl *CallACL) Authorize(
	ctx context.Context,
	principal security.Principal,
	target operation.Operation,
	request any,
) (authz.Decision, error) {
	if acl == nil {
		return authz.Decision{}, ErrInvalidService
	}
	access, ok := request.(AuthorizationRequest)
	if !ok || !access.Action.valid() ||
		access.CallID.String() == "" || access.Operation.String() == "" {
		return authz.Decision{}, ErrInvalidService
	}
	for _, rule := range acl.rules {
		if !matchesACL(rule.callID, access.CallID.String()) ||
			!matchesACL(rule.operation, access.Operation.String()) {
			continue
		}
		if _, allowedAction := rule.actions[access.Action]; !allowedAction {
			continue
		}
		if len(rule.principals) > 0 {
			principalIndex := sort.Search(len(rule.principals), func(index int) bool {
				candidate := rule.principals[index]
				return candidate.Issuer > principal.Issuer() ||
					candidate.Issuer == principal.Issuer() &&
						candidate.Subject >= principal.Subject()
			})
			if principalIndex == len(rule.principals) ||
				!rule.principals[principalIndex].Matches(principal) {
				continue
			}
		}
		decision, err := rule.policy.Authorize(ctx, principal, target, access)
		if err != nil {
			return authz.Decision{}, err
		}
		if decision.Allowed {
			return decision, nil
		}
	}
	return authz.Decision{Allowed: false}, nil
}

func validCallIDMatcher(value string) bool {
	if value == "*" {
		return true
	}
	_, err := NewCallID(value)
	return err == nil
}

func validOperationMatcher(value string) bool {
	if value == "*" {
		return true
	}
	_, err := NewOperation(value)
	return err == nil
}

func validACLValue(value string, required bool) bool {
	if value == "" {
		return !required
	}
	return len(value) <= maximumCallACLValueBytes &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\r\n\x00")
}

func matchesACL(matcher, value string) bool {
	return matcher == "*" || matcher == value
}

func (action Action) valid() bool {
	switch action {
	case ActionStart, ActionAttach, ActionSignal, ActionCancel, ActionHistory,
		ActionHistoryDetail:
		return true
	default:
		return false
	}
}

func validActions() []Action {
	return []Action{
		ActionStart,
		ActionAttach,
		ActionSignal,
		ActionCancel,
		ActionHistory,
		ActionHistoryDetail,
	}
}
