// Package jwt provides a production JWT authenticator and rotating JWKS key
// provider for Keelith's transport-neutral authentication contract.
package jwt

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	defaultMaxTokenBytes = 16 * 1024
	defaultLeeway        = 30 * time.Second
	maxTokenBytes        = 64 * 1024
	maxIdentityValues    = 64
	maxClaimMappings     = 32
	maxIdentityBytes     = 8 * 1024
	maxTokenValueBytes   = 2 * 1024
)

var (
	// ErrInvalidConfig reports an unsafe or incomplete JWT configuration.
	ErrInvalidConfig = errors.New("jwt: invalid config")
	// ErrInvalidCredential reports a token that cannot be trusted.
	ErrInvalidCredential = errors.New("jwt: invalid credential")
	// ErrKeyNotFound reports that no verification key matches a token header.
	ErrKeyNotFound = errors.New("jwt: verification key not found")
	// ErrKeyUnavailable reports that verification keys cannot currently be
	// obtained.
	ErrKeyUnavailable = errors.New("jwt: verification keys unavailable")
	// ErrInvalidState reports an invalid JWKS lifecycle transition.
	ErrInvalidState = errors.New("jwt: invalid lifecycle state")
)

// KeyProvider resolves an immutable public verification key by key id and
// algorithm. Implementations must not choose a key based only on token data
// other than these already-validated header fields.
type KeyProvider interface {
	Key(ctx context.Context, keyid string, algorithm string) (any, error)
}

// KeyProviderFunc adapts a function to KeyProvider.
type KeyProviderFunc func(context.Context, string, string) (any, error)

// Key calls function.
func (function KeyProviderFunc) Key(
	ctx context.Context,
	keyid string,
	algorithm string,
) (any, error) {
	return function(ctx, keyid, algorithm)
}

// Config configures one JWT Authenticator.
type Config struct {
	// Issuer is matched exactly against the required iss claim.
	Issuer string
	// Audiences contains accepted service audiences. At least one token
	// audience must match.
	Audiences []string
	// Algorithms is the explicit asymmetric algorithm allowlist. Defaults to
	// RS256, ES256, and EdDSA.
	Algorithms []string
	// Keys resolves verification keys. RemoteKeySet and StaticKeySet implement
	// this contract.
	Keys KeyProvider
	// MaxTokenBytes rejects oversized credentials before parsing.
	MaxTokenBytes int
	// Leeway bounds accepted clock skew. Zero selects 30 seconds.
	Leeway time.Duration
	// RolesClaim is a top-level string or string-array claim. Empty selects
	// "roles".
	RolesClaim string
	// ScopesClaim is a top-level space-delimited string or string-array claim.
	// Empty selects "scope".
	ScopesClaim string
	// PrincipalClaims maps low-risk Principal claim names to top-level JWT
	// string claim names. Registered claims and credentials are never copied.
	PrincipalClaims map[string]string
	// TimeFunc is intended for deterministic tests. Nil uses time.Now.
	TimeFunc func() time.Time
}

type normalizedConfig struct {
	issuer          string
	audiences       []string
	algorithms      []string
	algorithmSet    map[string]struct{}
	keys            KeyProvider
	maxTokenBytes   int
	leeway          time.Duration
	rolesClaim      string
	scopesClaim     string
	principalClaims map[string]string
	timeFunc        func() time.Time
}

func normalizeConfig(config Config) (normalizedConfig, error) {
	if !validTokenValue(config.Issuer, true) {
		return normalizedConfig{}, fmt.Errorf(
			"%w: issuer is required or malformed",
			ErrInvalidConfig,
		)
	}
	if isNil(config.Keys) {
		return normalizedConfig{}, fmt.Errorf(
			"%w: key provider is required",
			ErrInvalidConfig,
		)
	}
	audiences, err := normalizeValues("audience", config.Audiences, 1)
	if err != nil {
		return normalizedConfig{}, err
	}
	algorithms := config.Algorithms
	if len(algorithms) == 0 {
		algorithms = []string{"EdDSA", "ES256", "RS256"}
	}
	algorithms, err = normalizeAlgorithms(algorithms)
	if err != nil {
		return normalizedConfig{}, err
	}
	maxBytes := config.MaxTokenBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxTokenBytes
	}
	if maxBytes < 256 || maxBytes > maxTokenBytes {
		return normalizedConfig{}, fmt.Errorf(
			"%w: max token bytes must be between 256 and %d",
			ErrInvalidConfig,
			maxTokenBytes,
		)
	}
	leeway := config.Leeway
	if leeway == 0 {
		leeway = defaultLeeway
	}
	if leeway < 0 || leeway > 5*time.Minute {
		return normalizedConfig{}, fmt.Errorf(
			"%w: leeway must be between zero and five minutes",
			ErrInvalidConfig,
		)
	}
	rolesClaim := config.RolesClaim
	if rolesClaim == "" {
		rolesClaim = "roles"
	}
	scopesClaim := config.ScopesClaim
	if scopesClaim == "" {
		scopesClaim = "scope"
	}
	if !validClaimName(rolesClaim) || !validClaimName(scopesClaim) {
		return normalizedConfig{}, fmt.Errorf(
			"%w: role or scope claim name is malformed",
			ErrInvalidConfig,
		)
	}
	claimMappings, err := normalizeClaimMappings(config.PrincipalClaims)
	if err != nil {
		return normalizedConfig{}, err
	}
	algorithmSet := make(map[string]struct{}, len(algorithms))
	for _, algorithm := range algorithms {
		algorithmSet[algorithm] = struct{}{}
	}
	timeFunction := config.TimeFunc
	if timeFunction == nil {
		timeFunction = time.Now
	}
	return normalizedConfig{
		issuer:          config.Issuer,
		audiences:       audiences,
		algorithms:      algorithms,
		algorithmSet:    algorithmSet,
		keys:            config.Keys,
		maxTokenBytes:   maxBytes,
		leeway:          leeway,
		rolesClaim:      rolesClaim,
		scopesClaim:     scopesClaim,
		principalClaims: claimMappings,
		timeFunc:        timeFunction,
	}, nil
}

func normalizeAlgorithms(values []string) ([]string, error) {
	result := append([]string(nil), values...)
	if len(result) == 0 || len(result) > 16 {
		return nil, fmt.Errorf(
			"%w: algorithm count must be between 1 and 16",
			ErrInvalidConfig,
		)
	}
	for _, value := range result {
		if !supportedAlgorithm(value) {
			return nil, fmt.Errorf(
				"%w: unsupported asymmetric algorithm",
				ErrInvalidConfig,
			)
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index-1] == result[index] {
			return nil, fmt.Errorf(
				"%w: duplicate algorithm",
				ErrInvalidConfig,
			)
		}
	}
	return result, nil
}

func normalizeValues(
	name string,
	values []string,
	minimum int,
) ([]string, error) {
	result := append([]string(nil), values...)
	if len(result) < minimum || len(result) > maxIdentityValues {
		return nil, fmt.Errorf(
			"%w: %s count is outside supported bounds",
			ErrInvalidConfig,
			name,
		)
	}
	total := 0
	for _, value := range result {
		if !validTokenValue(value, true) {
			return nil, fmt.Errorf(
				"%w: %s value is malformed",
				ErrInvalidConfig,
				name,
			)
		}
		total += len(value)
	}
	if total > maxIdentityBytes {
		return nil, fmt.Errorf(
			"%w: %s values are too large",
			ErrInvalidConfig,
			name,
		)
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index-1] == result[index] {
			return nil, fmt.Errorf(
				"%w: duplicate %s",
				ErrInvalidConfig,
				name,
			)
		}
	}
	return result, nil
}

func normalizeClaimMappings(values map[string]string) (map[string]string, error) {
	if len(values) > maxClaimMappings {
		return nil, fmt.Errorf(
			"%w: too many principal claim mappings",
			ErrInvalidConfig,
		)
	}
	result := make(map[string]string, len(values))
	for principalName, tokenName := range values {
		if !validClaimName(principalName) || !validClaimName(tokenName) ||
			registeredClaim(tokenName) {
			return nil, fmt.Errorf(
				"%w: principal claim mapping is malformed",
				ErrInvalidConfig,
			)
		}
		result[principalName] = tokenName
	}
	return result, nil
}

func supportedAlgorithm(algorithm string) bool {
	switch algorithm {
	case "RS256", "RS384", "RS512",
		"PS256", "PS384", "PS512",
		"ES256", "ES384", "ES512",
		"EdDSA":
		return true
	default:
		return false
	}
}

func registeredClaim(name string) bool {
	switch name {
	case "iss", "sub", "aud", "exp", "nbf", "iat", "jti":
		return true
	default:
		return false
	}
}

func validClaimName(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z',
			character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9',
			character == '_',
			character == '-',
			character == '.':
		default:
			return false
		}
	}
	return true
}

func validTokenValue(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > maxTokenValueBytes || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
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
