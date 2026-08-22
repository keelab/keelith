package idempotency

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"time"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// Config constructs one instance-scoped idempotency Runtime.
type Config struct {
	Store          Store
	Resolver       Resolver
	BackendTimeout time.Duration
	MaxResultBytes int
	Random         io.Reader
}

// Description is a value-free, low-cardinality operational snapshot.
type Description struct {
	Acquired         uint64
	Replayed         uint64
	InProgress       uint64
	Conflicts        uint64
	HandlerFailures  uint64
	BackendErrors    uint64
	CompletionErrors uint64
	AbandonErrors    uint64
	CodecErrors      uint64
}

// Runtime owns middleware state but not Store lifecycle.
type Runtime struct {
	store          Store
	resolver       Resolver
	backendTimeout time.Duration
	maxResultBytes int
	random         io.Reader

	mu               sync.Mutex
	acquired         uint64
	replayed         uint64
	inProgress       uint64
	conflicts        uint64
	handlerFailures  uint64
	backendErrors    uint64
	completionErrors uint64
	abandonErrors    uint64
	codecErrors      uint64
}

// New constructs a dormant Runtime.
func New(config Config) (*Runtime, error) {
	if isNil(config.Store) || isNil(config.Resolver) {
		return nil, fmt.Errorf("%w: store and resolver are required", ErrInvalidConfig)
	}
	backendTimeout := config.BackendTimeout
	if backendTimeout == 0 {
		backendTimeout = defaultBackendTimeout
	}
	if backendTimeout <= 0 || backendTimeout > maximumBackendTimeout {
		return nil, fmt.Errorf("%w: backend timeout", ErrInvalidConfig)
	}
	maxResultBytes := config.MaxResultBytes
	if maxResultBytes == 0 {
		maxResultBytes = defaultMaxResultBytes
	}
	if maxResultBytes <= 0 || maxResultBytes > defaultMaxResultBytes {
		return nil, fmt.Errorf("%w: max result bytes", ErrInvalidConfig)
	}
	random := config.Random
	if random == nil {
		random = rand.Reader
	}
	return &Runtime{
		store:          config.Store,
		resolver:       config.Resolver,
		backendTimeout: backendTimeout,
		maxResultBytes: maxResultBytes,
		random:         random,
	}, nil
}

// Middleware returns the transport-neutral unary middleware.
func (r *Runtime) Middleware() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		if r == nil || next == nil {
			return func(context.Context, any) (any, error) {
				return nil, fmt.Errorf("%w: runtime or handler is nil", ErrInvalidConfig)
			}
		}
		return func(ctx context.Context, request any) (any, error) {
			return r.invoke(ctx, request, next)
		}
	}
}

func (r *Runtime) invoke(ctx context.Context, request any, next middleware.Handler) (any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	target, ok := operation.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: operation is missing", ErrInvalidConfig)
	}
	rule, enabled := r.resolver.Resolve(target)
	if !enabled {
		return next(ctx, request)
	}
	if target.Kind() != operation.KindUnary {
		return nil, fmt.Errorf("%w: operation must be unary", ErrInvalidConfig)
	}
	if err := ValidateRule(rule); err != nil {
		return nil, err
	}
	identity, err := rule.Request(ctx, target, request)
	if err != nil {
		return nil, ErrInvalidRequest
	}
	if err := validateIdentity(identity); err != nil {
		return nil, err
	}
	owner, err := r.owner()
	if err != nil {
		return nil, kerrors.Wrap(err, ErrUnavailable.Code(), ErrUnavailable.Reason(), ErrUnavailable.Message())
	}
	claimRequest := ClaimRequest{
		Operation:     storageOperation(target),
		Namespace:     rule.Namespace,
		Key:           identity.Key,
		Fingerprint:   identity.Fingerprint,
		Owner:         owner,
		ProcessingTTL: rule.ProcessingTTL,
	}
	claimContext, cancel := context.WithTimeout(ctx, r.backendTimeout)
	claim, err := r.store.Claim(claimContext, claimRequest)
	cancel()
	if err != nil {
		r.recordBackendError()
		return nil, unavailable(err)
	}
	if err := validateClaim(claim, r.maxResultBytes); err != nil {
		r.recordBackendError()
		return nil, unavailable(err)
	}
	lease := Lease{
		Operation:   claimRequest.Operation,
		Namespace:   claimRequest.Namespace,
		Key:         claimRequest.Key,
		Fingerprint: claimRequest.Fingerprint,
		Owner:       claimRequest.Owner,
	}
	switch claim.State {
	case ClaimAcquired:
		return r.execute(ctx, request, next, rule, lease)
	case ClaimReplayed:
		response, decodeErr := rule.Codec.Decode(append([]byte(nil), claim.Result...))
		if decodeErr != nil {
			r.recordCodecError()
			return nil, resultInvalid(decodeErr)
		}
		r.mu.Lock()
		r.replayed++
		r.mu.Unlock()
		return response, nil
	case ClaimInProgress:
		r.mu.Lock()
		r.inProgress++
		r.mu.Unlock()
		retryMilliseconds := claim.RetryAfter.Milliseconds()
		if retryMilliseconds < 1 {
			retryMilliseconds = 1
		}
		return nil, ErrInProgress.Clone(kerrors.WithMetadata(map[string]string{
			"retry-after-ms": strconv.FormatInt(retryMilliseconds, 10),
		}))
	case ClaimConflict:
		r.mu.Lock()
		r.conflicts++
		r.mu.Unlock()
		return nil, ErrConflict
	default:
		r.recordBackendError()
		return nil, unavailable(ErrInvalidDecision)
	}
}

// storageOperation deliberately omits transport so HTTP and gRPC adapters for
// the same logical unary method compete for one claim and replay one result.
func storageOperation(target operation.Operation) string {
	return url.PathEscape(target.Service()) + "/" +
		url.PathEscape(target.Method()) + "/" +
		url.PathEscape(string(target.Kind()))
}

func (r *Runtime) execute(ctx context.Context, request any, next middleware.Handler, rule Rule, lease Lease) (any, error) {
	r.mu.Lock()
	r.acquired++
	r.mu.Unlock()
	response, handlerErr := next(ctx, request)
	if handlerErr != nil {
		r.mu.Lock()
		r.handlerFailures++
		r.mu.Unlock()
		r.abandon(ctx, lease)
		return nil, handlerErr
	}
	encoded, err := rule.Codec.Encode(response)
	if err != nil || len(encoded) > r.maxResultBytes {
		r.recordCodecError()
		r.abandon(ctx, lease)
		if err == nil {
			err = fmt.Errorf("result has %d bytes", len(encoded))
		}
		return nil, resultInvalid(err)
	}
	completion := Completion{
		Lease:     lease,
		Result:    append([]byte(nil), encoded...),
		ResultTTL: rule.ResultTTL,
	}
	completeContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		r.backendTimeout,
	)
	err = r.store.Complete(completeContext, completion)
	cancel()
	if err != nil {
		r.mu.Lock()
		r.completionErrors++
		r.mu.Unlock()
		return nil, unavailable(err)
	}
	return response, nil
}

func (r *Runtime) abandon(ctx context.Context, lease Lease) {
	abandonContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.backendTimeout)
	err := r.store.Abandon(abandonContext, lease)
	cancel()
	if err == nil {
		return
	}
	r.mu.Lock()
	r.abandonErrors++
	r.mu.Unlock()
}

func (r *Runtime) owner() (string, error) {
	content := make([]byte, 16)
	if _, err := io.ReadFull(r.random, content); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(content), nil
}

// Describe returns aggregate state without request identities or results.
func (r *Runtime) Describe() Description {
	if r == nil {
		return Description{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return Description{
		Acquired:         r.acquired,
		Replayed:         r.replayed,
		InProgress:       r.inProgress,
		Conflicts:        r.conflicts,
		HandlerFailures:  r.handlerFailures,
		BackendErrors:    r.backendErrors,
		CompletionErrors: r.completionErrors,
		AbandonErrors:    r.abandonErrors,
		CodecErrors:      r.codecErrors,
	}
}

func (r *Runtime) recordBackendError() {
	r.mu.Lock()
	r.backendErrors++
	r.mu.Unlock()
}

func (r *Runtime) recordCodecError() {
	r.mu.Lock()
	r.codecErrors++
	r.mu.Unlock()
}

func unavailable(cause error) error {
	return kerrors.Wrap(
		cause,
		ErrUnavailable.Code(),
		ErrUnavailable.Reason(),
		ErrUnavailable.Message(),
	)
}

func resultInvalid(cause error) error {
	return kerrors.Wrap(
		cause,
		ErrResultInvalid.Code(),
		ErrResultInvalid.Reason(),
		ErrResultInvalid.Message(),
	)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
