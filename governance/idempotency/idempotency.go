// Package idempotency provides transport-neutral, operation-scoped request
// deduplication with fenced ownership and bounded result replay.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/operation"
)

const (
	defaultBackendTimeout = 100 * time.Millisecond
	defaultMaxResultBytes = 1024 * 1024
	maximumBackendTimeout = 5 * time.Second
	maximumKeyBytes       = 256
	maximumNamespaceBytes = 128
	maximumOwnerBytes     = 64
	minimumProcessingTTL  = time.Second
	maximumProcessingTTL  = 15 * time.Minute
	minimumResultTTL      = time.Minute
	maximumResultTTL      = 24 * time.Hour
)

var (
	// ErrInvalidConfig reports an incomplete runtime, resolver, rule, or codec.
	ErrInvalidConfig = errors.New("idempotency: invalid config")
	// ErrInvalidRequest reports an absent or unsafe key/fingerprint.
	ErrInvalidRequest = kerrors.New(
		400,
		"IDEMPOTENCY_KEY_INVALID",
		"idempotency identity is invalid",
	)
	// ErrConflict reports reuse of a key for a different request fingerprint.
	ErrConflict = kerrors.New(
		409,
		"IDEMPOTENCY_KEY_CONFLICT",
		"idempotency key was already used for another request",
	)
	// ErrInProgress reports another active owner for the same request.
	ErrInProgress = kerrors.New(
		409,
		"IDEMPOTENCY_IN_PROGRESS",
		"idempotent request is already in progress",
	)
	// ErrUnavailable reports a fail-closed Store failure.
	ErrUnavailable = kerrors.New(
		503,
		"IDEMPOTENCY_UNAVAILABLE",
		"idempotency service is unavailable",
	)
	// ErrResultInvalid reports an unsafe, oversized, or undecodable replay.
	ErrResultInvalid = kerrors.New(
		500,
		"IDEMPOTENCY_RESULT_INVALID",
		"idempotency result could not be recorded or replayed",
	)
	// ErrInvalidDecision reports a malformed Store response.
	ErrInvalidDecision = errors.New("idempotency: invalid store decision")
	// ErrStaleOwner reports a Complete or Abandon after ownership changed.
	ErrStaleOwner = errors.New("idempotency: stale owner")
)

// ClaimState is one atomic Store decision.
type ClaimState string

const (
	// ClaimAcquired grants the caller ownership of a new request.
	ClaimAcquired ClaimState = "acquired"
	// ClaimInProgress reports a request currently owned by another caller.
	ClaimInProgress ClaimState = "in-progress"
	// ClaimReplayed returns a previously completed result.
	ClaimReplayed ClaimState = "replayed"
	// ClaimConflict reports an incompatible reuse of an idempotency key.
	ClaimConflict ClaimState = "conflict"
)

// RequestIdentity binds a caller-selected key to exact request content.
type RequestIdentity struct {
	Key         string
	Fingerprint string
}

// RequestFunc derives one bounded identity from transport-neutral facts.
type RequestFunc func(
	context.Context,
	operation.Operation,
	any,
) (RequestIdentity, error)

// Codec encodes and reconstructs one Operation-specific result type.
type Codec interface {
	Encode(any) ([]byte, error)
	Decode([]byte) (any, error)
}

// CodecFuncs adapts functions to Codec.
type CodecFuncs struct {
	EncodeFunc func(any) ([]byte, error)
	DecodeFunc func([]byte) (any, error)
}

// Encode implements Codec.
func (codec CodecFuncs) Encode(value any) ([]byte, error) {
	if codec.EncodeFunc == nil {
		return nil, fmt.Errorf("%w: encode function is nil", ErrInvalidConfig)
	}
	return codec.EncodeFunc(value)
}

// Decode implements Codec.
func (codec CodecFuncs) Decode(encoded []byte) (any, error) {
	if codec.DecodeFunc == nil {
		return nil, fmt.Errorf("%w: decode function is nil", ErrInvalidConfig)
	}
	return codec.DecodeFunc(encoded)
}

// Rule is the complete contract for one explicitly idempotent Operation.
type Rule struct {
	Namespace     string
	ProcessingTTL time.Duration
	ResultTTL     time.Duration
	Request       RequestFunc
	Codec         Codec
}

// Resolver returns an exact Rule for a unary Operation.
type Resolver interface {
	Resolve(operation.Operation) (Rule, bool)
}

// ClaimRequest attempts to own one request while its Handler executes.
type ClaimRequest struct {
	Operation     string
	Namespace     string
	Key           string
	Fingerprint   string
	Owner         string
	ProcessingTTL time.Duration
}

// Lease is the complete fence required to mutate a processing claim.
type Lease struct {
	Operation   string
	Namespace   string
	Key         string
	Fingerprint string
	Owner       string
}

// Claim is an atomic Store decision. Result is set only for ClaimReplayed.
type Claim struct {
	State      ClaimState
	Result     []byte
	RetryAfter time.Duration
}

// Completion atomically publishes a successful result for one owner.
type Completion struct {
	Lease     Lease
	Result    []byte
	ResultTTL time.Duration
}

// Store provides atomic claim, fenced completion, and fenced abandon.
type Store interface {
	Claim(context.Context, ClaimRequest) (Claim, error)
	Complete(context.Context, Completion) error
	Abandon(context.Context, Lease) error
}

// Fingerprint returns a lowercase SHA-256 identity for deterministic bytes.
func Fingerprint(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

// ValidateRule validates construction-time Rule bounds.
func ValidateRule(rule Rule) error {
	if !validNamespace(rule.Namespace) {
		return fmt.Errorf("%w: namespace", ErrInvalidConfig)
	}
	if rule.ProcessingTTL < minimumProcessingTTL ||
		rule.ProcessingTTL > maximumProcessingTTL ||
		rule.ResultTTL < minimumResultTTL ||
		rule.ResultTTL > maximumResultTTL {
		return fmt.Errorf("%w: rule TTL", ErrInvalidConfig)
	}
	if rule.Request == nil || isNil(rule.Codec) {
		return fmt.Errorf("%w: request and codec are required", ErrInvalidConfig)
	}
	return nil
}

// ValidateClaimRequest validates the complete Store claim input.
func ValidateClaimRequest(request ClaimRequest) error {
	if !validOperation(request.Operation) ||
		!validNamespace(request.Namespace) ||
		!validKey(request.Key) ||
		!validFingerprint(request.Fingerprint) ||
		!validOwner(request.Owner) ||
		request.ProcessingTTL < minimumProcessingTTL ||
		request.ProcessingTTL > maximumProcessingTTL {
		return fmt.Errorf("%w: claim request", ErrInvalidConfig)
	}
	return nil
}

// ValidateLease validates a mutation fence.
func ValidateLease(lease Lease) error {
	return ValidateClaimRequest(ClaimRequest{
		Operation:     lease.Operation,
		Namespace:     lease.Namespace,
		Key:           lease.Key,
		Fingerprint:   lease.Fingerprint,
		Owner:         lease.Owner,
		ProcessingTTL: minimumProcessingTTL,
	})
}

// ValidateCompletion validates a result publication request.
func ValidateCompletion(completion Completion, maximumBytes int) error {
	if err := ValidateLease(completion.Lease); err != nil {
		return err
	}
	if maximumBytes <= 0 || maximumBytes > defaultMaxResultBytes ||
		len(completion.Result) > maximumBytes ||
		completion.ResultTTL < minimumResultTTL ||
		completion.ResultTTL > maximumResultTTL {
		return fmt.Errorf("%w: completion", ErrInvalidConfig)
	}
	return nil
}

func validateIdentity(identity RequestIdentity) error {
	if !validKey(identity.Key) || !validFingerprint(identity.Fingerprint) {
		return ErrInvalidRequest
	}
	return nil
}

func validateClaim(claim Claim, maximumBytes int) error {
	switch claim.State {
	case ClaimAcquired, ClaimConflict:
		if len(claim.Result) != 0 || claim.RetryAfter != 0 {
			return ErrInvalidDecision
		}
	case ClaimInProgress:
		if len(claim.Result) != 0 || claim.RetryAfter < 0 ||
			claim.RetryAfter > maximumProcessingTTL {
			return ErrInvalidDecision
		}
	case ClaimReplayed:
		if len(claim.Result) > maximumBytes || claim.RetryAfter != 0 {
			return ErrInvalidDecision
		}
	default:
		return ErrInvalidDecision
	}
	return nil
}

func validOperation(value string) bool {
	return value != "" && len(value) <= 1024 && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && !containsControl(value)
}

func validNamespace(value string) bool {
	if value == "" || len(value) > maximumNamespaceBytes {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.' || r == '/':
		default:
			return false
		}
	}
	return true
}

func validKey(value string) bool {
	return value != "" && len(value) <= maximumKeyBytes &&
		utf8.ValidString(value) && strings.TrimSpace(value) == value &&
		!containsControl(value)
}

func validFingerprint(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func validOwner(value string) bool {
	if value == "" || len(value) > maximumOwnerBytes {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

func containsControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
