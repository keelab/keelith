// Package security defines authenticated principal state shared by AuthN and
// AuthZ adapters.
package security

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

// ErrInvalidPrincipal reports incomplete or malformed authenticated identity.
var ErrInvalidPrincipal = errors.New("security: invalid principal")

// PrincipalSpec contains identity claims accepted from an Authenticator.
type PrincipalSpec struct {
	Subject   string
	Issuer    string
	Audiences []string
	Roles     []string
	Scopes    []string
	Claims    map[string]string
	ExpiresAt time.Time
}

// Principal is an immutable authenticated identity.
type Principal struct {
	subject   string
	issuer    string
	audiences []string
	roles     []string
	scopes    []string
	claims    map[string]string
	expiresAt time.Time
}

// NewPrincipal validates and snapshots authenticated identity.
func NewPrincipal(spec PrincipalSpec) (Principal, error) {
	if !valid(spec.Subject, true) || !valid(spec.Issuer, false) {
		return Principal{}, ErrInvalidPrincipal
	}
	audiences, err := normalizedSet("audience", spec.Audiences)
	if err != nil {
		return Principal{}, err
	}
	roles, err := normalizedSet("role", spec.Roles)
	if err != nil {
		return Principal{}, err
	}
	scopes, err := normalizedSet("scope", spec.Scopes)
	if err != nil {
		return Principal{}, err
	}
	claims := make(map[string]string, len(spec.Claims))
	for key, value := range spec.Claims {
		if !valid(key, true) || !valid(value, false) {
			return Principal{}, fmt.Errorf(
				"%w: claim %q is malformed",
				ErrInvalidPrincipal,
				key,
			)
		}
		claims[key] = value
	}
	return Principal{
		subject:   spec.Subject,
		issuer:    spec.Issuer,
		audiences: audiences,
		roles:     roles,
		scopes:    scopes,
		claims:    claims,
		expiresAt: spec.ExpiresAt,
	}, nil
}

// Subject returns the stable authenticated subject.
func (principal Principal) Subject() string { return principal.subject }

// Issuer returns the authenticator issuer.
func (principal Principal) Issuer() string { return principal.issuer }

// Audiences returns a sorted independent audience list.
func (principal Principal) Audiences() []string {
	return append([]string(nil), principal.audiences...)
}

// Roles returns a sorted independent role list.
func (principal Principal) Roles() []string {
	return append([]string(nil), principal.roles...)
}

// Scopes returns a sorted independent scope list.
func (principal Principal) Scopes() []string {
	return append([]string(nil), principal.scopes...)
}

// Claims returns an independent low-risk claim snapshot. Credential material
// must never be stored here.
func (principal Principal) Claims() map[string]string {
	result := make(map[string]string, len(principal.claims))
	for key, value := range principal.claims {
		result[key] = value
	}
	return result
}

// ExpiresAt returns the authenticated identity expiry, or zero if unknown.
func (principal Principal) ExpiresAt() time.Time { return principal.expiresAt }

// Expired reports whether a non-zero principal expiry has passed.
func (principal Principal) Expired(now time.Time) bool {
	return !principal.expiresAt.IsZero() && !now.Before(principal.expiresAt)
}

// HasRole reports membership in the normalized role set.
func (principal Principal) HasRole(role string) bool {
	index := sort.SearchStrings(principal.roles, role)
	return index < len(principal.roles) && principal.roles[index] == role
}

// HasScope reports membership in the normalized scope set.
func (principal Principal) HasScope(scope string) bool {
	index := sort.SearchStrings(principal.scopes, scope)
	return index < len(principal.scopes) && principal.scopes[index] == scope
}

// Validate reports whether Principal is initialized and unexpired at now.
func (principal Principal) Validate(now time.Time) error {
	_, err := NewPrincipal(PrincipalSpec{
		Subject:   principal.subject,
		Issuer:    principal.issuer,
		Audiences: principal.audiences,
		Roles:     principal.roles,
		Scopes:    principal.scopes,
		Claims:    principal.claims,
		ExpiresAt: principal.expiresAt,
	})
	if err != nil {
		return err
	}
	if principal.Expired(now) {
		return fmt.Errorf("%w: principal is expired", ErrInvalidPrincipal)
	}
	return nil
}

type principalContextKey struct{}

// WithPrincipal attaches an immutable authenticated Principal to ctx.
func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// PrincipalFromContext returns the authenticated Principal.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

func normalizedSet(name string, values []string) ([]string, error) {
	result := append([]string(nil), values...)
	for index, value := range result {
		if !valid(value, true) {
			return nil, fmt.Errorf(
				"%w: %s %d is malformed",
				ErrInvalidPrincipal,
				name,
				index,
			)
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index-1] == result[index] {
			return nil, fmt.Errorf(
				"%w: duplicate %s %q",
				ErrInvalidPrincipal,
				name,
				result[index],
			)
		}
	}
	return result, nil
}

func valid(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
