package jwt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"
	"github.com/keelab/keelith/security"
	"github.com/keelab/keelith/security/authn"
)

// Description is a credential-free aggregate authenticator snapshot.
type Description struct {
	Successful  uint64
	Rejected    uint64
	KeyFailures uint64
}

// Authenticator validates signed JWT bearer credentials and maps only
// explicitly configured low-risk claims into an immutable Principal.
type Authenticator struct {
	config normalizedConfig
	parser *jwtlib.Parser

	successful  atomic.Uint64
	rejected    atomic.Uint64
	keyFailures atomic.Uint64
}

// New constructs a strict JWT authenticator.
func New(config Config) (*Authenticator, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	parser := jwtlib.NewParser(
		jwtlib.WithAudience(normalized.audiences...),
		jwtlib.WithExpirationRequired(),
		jwtlib.WithIssuedAt(),
		jwtlib.WithIssuer(normalized.issuer),
		jwtlib.WithLeeway(normalized.leeway),
		jwtlib.WithStrictDecoding(),
		jwtlib.WithTimeFunc(normalized.timeFunc),
		jwtlib.WithValidMethods(normalized.algorithms),
	)
	return &Authenticator{
		config: normalized,
		parser: parser,
	}, nil
}

// Authenticate validates one signed bearer token.
func (authenticator *Authenticator) Authenticate(
	ctx context.Context,
	credential authn.Credential,
) (security.Principal, error) {
	if ctx == nil {
		return security.Principal{}, ErrInvalidCredential
	}
	if cause := context.Cause(ctx); cause != nil {
		return security.Principal{}, cause
	}
	if authenticator == nil ||
		credential.Scheme() != "bearer" ||
		len(credential.Token()) < 16 ||
		len(credential.Token()) > authenticator.config.maxTokenBytes {
		if authenticator != nil {
			authenticator.rejected.Add(1)
		}
		return security.Principal{}, ErrInvalidCredential
	}

	claims := jwtlib.MapClaims{}
	token, err := authenticator.parser.ParseWithClaims(
		credential.Token(),
		claims,
		func(token *jwtlib.Token) (any, error) {
			algorithm := token.Method.Alg()
			if _, allowed := authenticator.config.algorithmSet[algorithm]; !allowed {
				return nil, ErrInvalidCredential
			}
			keyid, ok := token.Header["kid"].(string)
			if !ok || !validKeyid(keyid) {
				return nil, ErrInvalidCredential
			}
			key, keyErr := authenticator.config.keys.Key(
				ctx,
				keyid,
				algorithm,
			)
			if keyErr != nil {
				authenticator.keyFailures.Add(1)
				if cause := context.Cause(ctx); cause != nil {
					return nil, cause
				}
				return nil, ErrKeyUnavailable
			}
			if err := validatePublicKey(algorithm, key); err != nil {
				authenticator.keyFailures.Add(1)
				return nil, ErrKeyUnavailable
			}
			return key, nil
		},
	)
	if err != nil || token == nil || !token.Valid {
		authenticator.rejected.Add(1)
		if cause := context.Cause(ctx); cause != nil {
			return security.Principal{}, cause
		}
		return security.Principal{}, ErrInvalidCredential
	}

	principal, err := authenticator.principal(claims)
	if err != nil {
		authenticator.rejected.Add(1)
		return security.Principal{}, ErrInvalidCredential
	}
	authenticator.successful.Add(1)
	return principal, nil
}

// Description returns aggregate counters without token, claim, key, issuer,
// audience, or subject values.
func (authenticator *Authenticator) Description() Description {
	if authenticator == nil {
		return Description{}
	}
	return Description{
		Successful:  authenticator.successful.Load(),
		Rejected:    authenticator.rejected.Load(),
		KeyFailures: authenticator.keyFailures.Load(),
	}
}

func (authenticator *Authenticator) principal(
	claims jwtlib.MapClaims,
) (security.Principal, error) {
	subject, err := claims.GetSubject()
	if err != nil || !validTokenValue(subject, true) {
		return security.Principal{}, ErrInvalidCredential
	}
	issuer, err := claims.GetIssuer()
	if err != nil || issuer != authenticator.config.issuer {
		return security.Principal{}, ErrInvalidCredential
	}
	audiences, err := claims.GetAudience()
	if err != nil {
		return security.Principal{}, ErrInvalidCredential
	}
	audiences, err = normalizeIdentityClaims(audiences, false)
	if err != nil {
		return security.Principal{}, err
	}
	expiresAt, err := claims.GetExpirationTime()
	if err != nil || expiresAt == nil || !expiresAt.After(time.Time{}) {
		return security.Principal{}, ErrInvalidCredential
	}
	roles, err := extractIdentityClaim(
		claims[authenticator.config.rolesClaim],
		false,
	)
	if err != nil {
		return security.Principal{}, err
	}
	scopes, err := extractIdentityClaim(
		claims[authenticator.config.scopesClaim],
		true,
	)
	if err != nil {
		return security.Principal{}, err
	}
	principalClaims := make(
		map[string]string,
		len(authenticator.config.principalClaims),
	)
	for principalName, tokenName := range authenticator.config.principalClaims {
		value, exists := claims[tokenName]
		if !exists {
			continue
		}
		text, ok := value.(string)
		if !ok || !validTokenValue(text, false) {
			return security.Principal{}, ErrInvalidCredential
		}
		principalClaims[principalName] = text
	}
	return security.NewPrincipal(security.PrincipalSpec{
		Subject:   subject,
		Issuer:    issuer,
		Audiences: audiences,
		Roles:     roles,
		Scopes:    scopes,
		Claims:    principalClaims,
		ExpiresAt: expiresAt.Time,
	})
}

func extractIdentityClaim(value any, splitSpace bool) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	var values []string
	switch typed := value.(type) {
	case string:
		if splitSpace {
			values = strings.Fields(typed)
		} else {
			values = []string{typed}
		}
	case []string:
		values = append([]string(nil), typed...)
	case []any:
		values = make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, ErrInvalidCredential
			}
			values = append(values, text)
		}
	default:
		return nil, ErrInvalidCredential
	}
	return normalizeIdentityClaims(values, true)
}

func normalizeIdentityClaims(
	values []string,
	optional bool,
) ([]string, error) {
	minimum := 1
	if optional {
		minimum = 0
	}
	normalized, err := normalizeValues("identity", values, minimum)
	if err != nil {
		if errors.Is(err, ErrInvalidConfig) {
			return nil, ErrInvalidCredential
		}
		return nil, fmt.Errorf("%w", ErrInvalidCredential)
	}
	return normalized, nil
}
