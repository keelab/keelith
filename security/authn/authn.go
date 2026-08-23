// Package authn provides transport-neutral authentication middleware.
package authn

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/security"
)

const (
	// Reason is the stable unauthenticated error reason.
	Reason = "UNAUTHENTICATED"
	// Code is the transport-neutral unauthenticated code.
	Code int32 = 401
)

var (
	// ErrCredentialMissing reports a missing transport credential.
	ErrCredentialMissing = errors.New("authn: credential is missing")
	// ErrCredentialMalformed reports an invalid credential envelope.
	ErrCredentialMalformed = errors.New("authn: credential is malformed")
	// ErrInvalidAuthenticator reports a nil authentication dependency.
	ErrInvalidAuthenticator = errors.New("authn: invalid authenticator")
)

// Credential contains a normalized scheme and opaque token.
type Credential struct {
	scheme string
	token  string
}

// NewCredential validates one opaque credential.
func NewCredential(scheme, token string) (Credential, error) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	token = strings.TrimSpace(token)
	if scheme == "" || token == "" {
		return Credential{}, ErrCredentialMalformed
	}
	return Credential{scheme: scheme, token: token}, nil
}

// Scheme returns the normalized credential scheme.
func (credential Credential) Scheme() string { return credential.scheme }

// Token returns the opaque token for an Authenticator. Callers must not log
// or retain it.
func (credential Credential) Token() string { return credential.token }

// Extractor extracts a credential from transport-neutral request context.
type Extractor interface {
	Extract(context.Context, any) (Credential, error)
}

// ExtractorFunc adapts a function to Extractor.
type ExtractorFunc func(context.Context, any) (Credential, error)

// Extract delegates to fn.
func (fn ExtractorFunc) Extract(
	ctx context.Context,
	request any,
) (Credential, error) {
	return fn(ctx, request)
}

// Authenticator validates a credential and returns an immutable Principal.
type Authenticator interface {
	Authenticate(context.Context, Credential) (security.Principal, error)
}

// AuthenticatorFunc adapts a function to Authenticator.
type AuthenticatorFunc func(
	context.Context,
	Credential,
) (security.Principal, error)

// Authenticate delegates to fn.
func (fn AuthenticatorFunc) Authenticate(
	ctx context.Context,
	credential Credential,
) (security.Principal, error) {
	return fn(ctx, credential)
}

// MetadataBearer extracts exactly one Bearer token from inbound metadata.
func MetadataBearer(key string, maxBytes int) (Extractor, error) {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" || maxBytes <= 0 {
		return nil, ErrCredentialMalformed
	}
	return ExtractorFunc(func(
		ctx context.Context,
		_ any,
	) (Credential, error) {
		inbound, ok := metadata.Inbound(ctx)
		if !ok {
			return Credential{}, ErrCredentialMissing
		}
		values := inbound.Values(key)
		if len(values) == 0 {
			return Credential{}, ErrCredentialMissing
		}
		if len(values) != 1 || len(values[0]) > maxBytes {
			return Credential{}, ErrCredentialMalformed
		}
		scheme, token, found := strings.Cut(values[0], " ")
		if !found || !strings.EqualFold(scheme, "Bearer") {
			return Credential{}, ErrCredentialMalformed
		}
		return NewCredential("bearer", token)
	}), nil
}

// Middleware authenticates a request and attaches Principal to its context.
func Middleware(
	extractor Extractor,
	authenticator Authenticator,
) (middleware.Middleware, error) {
	if isNil(extractor) || isNil(authenticator) {
		return nil, ErrInvalidAuthenticator
	}
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			credential, err := extractor.Extract(ctx, request)
			if err != nil {
				return nil, frameworkError(err)
			}
			principal, err := authenticator.Authenticate(ctx, credential)
			if err != nil {
				return nil, frameworkError(err)
			}
			if err := principal.Validate(time.Now()); err != nil {
				return nil, frameworkError(err)
			}
			return next(security.WithPrincipal(ctx, principal), request)
		}
	}, nil
}

func frameworkError(cause error) error {
	return kerrors.Wrap(
		cause,
		Code,
		Reason,
		"authentication failed",
	)
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

// Bearer authenticates one fixed token map. It is suitable for bootstrap and
// tests, not a replacement for a managed identity provider.
type Bearer struct {
	principals map[string]security.Principal
}

// NewBearer snapshots token-to-principal mappings.
func NewBearer(values map[string]security.Principal) (*Bearer, error) {
	principals := make(map[string]security.Principal, len(values))
	for token, principal := range values {
		if strings.TrimSpace(token) == "" {
			return nil, fmt.Errorf("%w: empty token", ErrCredentialMalformed)
		}
		if err := principal.Validate(time.Now()); err != nil {
			return nil, err
		}
		principals[token] = principal
	}
	return &Bearer{principals: principals}, nil
}

// Authenticate resolves an exact Bearer token.
func (bearer *Bearer) Authenticate(
	ctx context.Context,
	credential Credential,
) (security.Principal, error) {
	if err := context.Cause(ctx); err != nil {
		return security.Principal{}, err
	}
	if credential.scheme != "bearer" {
		return security.Principal{}, ErrCredentialMalformed
	}
	principal, exists := bearer.principals[credential.token]
	if !exists {
		return security.Principal{}, ErrCredentialMalformed
	}
	return principal, nil
}
