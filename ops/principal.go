package ops

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type principalContextKey struct{}

// Principal is the authenticated, bounded identity of one Ops caller.
type Principal struct {
	Subject string
}

// PrincipalPolicy authenticates an Ops request and returns its trusted actor.
type PrincipalPolicy func(*http.Request) (Principal, error)

// WithPrincipalAccessPolicy protects diagnostics and attaches the authenticated
// Principal to the request context for audit consumers.
func WithPrincipalAccessPolicy(policy PrincipalPolicy) Option {
	if policy == nil {
		return optionFunc(func(*options) error { return fmt.Errorf("principal policy is nil") })
	}
	return WithAccessPolicy(func(request *http.Request) error {
		principal, err := policy(request)
		if err != nil {
			return err
		}
		principal.Subject = strings.TrimSpace(principal.Subject)
		if principal.Subject == "" || !validDiagnosticString(principal.Subject) {
			return fmt.Errorf("principal subject is invalid")
		}
		*request = *request.WithContext(context.WithValue(request.Context(), principalContextKey{}, principal))
		return nil
	})
}

// PrincipalFromContext returns the trusted identity established by Ops auth.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	if ctx == nil {
		return Principal{}, false
	}
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok && principal.Subject != ""
}
