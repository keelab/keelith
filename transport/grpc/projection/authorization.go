package projectiongrpc

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/programmable/projection"
	"github.com/keelab/keelith/security"
	"github.com/keelab/keelith/security/authz"
)

const maximumProjectionACLRules = 1024

// ProjectionACLRule grants roles/scopes access to one exact ProjectionID.
type ProjectionACLRule struct {
	Projection    projection.ProjectionID
	AnyRole       []string
	RequiredScope []string
}

// ProjectionACL is an immutable exact-ID authorization policy.
type ProjectionACL struct {
	rules map[projection.ProjectionID]*authz.RBAC
}

// NewProjectionACL validates and snapshots exact projection rules.
func NewProjectionACL(rules ...ProjectionACLRule) (*ProjectionACL, error) {
	if len(rules) == 0 || len(rules) > maximumProjectionACLRules {
		return nil, ErrInvalidServer
	}
	result := &ProjectionACL{
		rules: make(map[projection.ProjectionID]*authz.RBAC, len(rules)),
	}
	for index, rule := range rules {
		if err := rule.Projection.Validate(); err != nil {
			return nil, fmt.Errorf(
				"%w: ACL rule %d projection",
				ErrInvalidServer,
				index,
			)
		}
		if _, duplicate := result.rules[rule.Projection]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate ACL projection %q",
				ErrInvalidServer,
				rule.Projection,
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
				"%w: ACL rule %d",
				ErrInvalidServer,
				index,
			)
		}
		result.rules[rule.Projection] = policy
	}
	return result, nil
}

// Authorize implements authz.Authorizer and denies unknown projection IDs.
func (acl *ProjectionACL) Authorize(
	ctx context.Context,
	principal security.Principal,
	target operation.Operation,
	request any,
) (authz.Decision, error) {
	if acl == nil {
		return authz.Decision{}, ErrInvalidServer
	}
	access, ok := request.(AuthorizationRequest)
	if !ok {
		return authz.Decision{}, ErrInvalidServer
	}
	policy := acl.rules[access.Projection]
	if policy == nil {
		return authz.Decision{Allowed: false}, nil
	}
	return policy.Authorize(ctx, principal, target, request)
}
