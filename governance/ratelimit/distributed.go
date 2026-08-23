package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/operation"
)

const (
	defaultBackendTimeout = 50 * time.Millisecond
	maxSubjectBytes       = 256
)

var (
	// ErrUnavailable reports a fail-closed shared quota backend failure.
	ErrUnavailable = kerrors.New(
		503,
		"RATE_LIMIT_UNAVAILABLE",
		"distributed rate limit is unavailable",
	)
	// ErrInvalidSubject reports an unsafe quota key or cost.
	ErrInvalidSubject = errors.New("ratelimit: invalid distributed subject")
	// ErrInvalidDecision reports a malformed backend result.
	ErrInvalidDecision = errors.New("ratelimit: invalid distributed decision")
)

// FailureMode defines behavior when the shared quota backend fails.
type FailureMode uint8

const (
	// FailClosed rejects requests when the backend cannot decide.
	FailClosed FailureMode = iota
	// FailOpen allows requests with only local concurrency protection.
	FailOpen
	// LocalFallback applies the complete policy using a per-process limiter.
	LocalFallback
)

// Degradation identifies an admitted request whose global quota was not
// enforced.
type Degradation string

const (
	// DegradationNone means the shared quota backend decided successfully.
	DegradationNone Degradation = ""
	// DegradationFailOpen means the backend failed and the call was admitted.
	DegradationFailOpen Degradation = "fail-open"
	// DegradationLocalFallback means a per-process token bucket replaced the
	// shared quota.
	DegradationLocalFallback Degradation = "local-fallback"
)

// Subject is one explicit quota identity and weighted request cost.
type Subject struct {
	Key  string
	Cost int
}

// SubjectFunc derives a quota subject from transport-neutral call facts.
type SubjectFunc func(
	context.Context,
	operation.Operation,
	any,
) (Subject, error)

// Quota is one backend-neutral token bucket request.
type Quota struct {
	Key   string
	Rate  float64
	Burst int
	Cost  int
}

// Decision is one shared quota result.
type Decision struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration
}

// DistributedBackend atomically checks and consumes shared quota.
type DistributedBackend interface {
	Allow(context.Context, Quota) (Decision, error)
}

// DistributedConfig configures shared quota and explicit failure behavior.
type DistributedConfig struct {
	Backend        DistributedBackend
	Mode           FailureMode
	LocalPool      *Pool
	Subject        SubjectFunc
	BackendTimeout time.Duration
}

// DistributedDescription is a low-cardinality operational snapshot.
type DistributedDescription struct {
	Mode           FailureMode
	Accepted       uint64
	Rejected       uint64
	BackendErrors  uint64
	FailOpen       uint64
	LocalFallback  uint64
	LastError      string
	BackendTimeout time.Duration
}

// DistributedLimiter combines a shared rate with local concurrency.
type DistributedLimiter struct {
	backend        DistributedBackend
	mode           FailureMode
	local          *Pool
	subject        SubjectFunc
	backendTimeout time.Duration

	mu            sync.Mutex
	accepted      uint64
	rejected      uint64
	backendErrors uint64
	failOpen      uint64
	localFallback uint64
	lastError     string
}

// DistributedPermit owns one local concurrency slot.
type DistributedPermit struct {
	local       *Permit
	decision    Decision
	degradation Degradation
}

// NewDistributedLimiter constructs an instance-scoped shared quota
// controller.
func NewDistributedLimiter(config DistributedConfig) (*DistributedLimiter, error) {
	if isNil(config.Backend) {
		return nil, fmt.Errorf("%w: distributed backend is nil", ErrInvalidOption)
	}
	if config.Mode != FailClosed && config.Mode != FailOpen && config.Mode != LocalFallback {
		return nil, fmt.Errorf("%w: failure mode %d", ErrInvalidOption, config.Mode)
	}
	local := config.LocalPool
	if local == nil {
		local = NewPool(nil)
	}
	subject := config.Subject
	if subject == nil {
		subject = func(_ context.Context, target operation.Operation, _ any) (Subject, error) {
			return Subject{Key: target.PolicyKey(), Cost: 1}, nil
		}
	}
	backendTimeout := config.BackendTimeout
	if backendTimeout == 0 {
		backendTimeout = defaultBackendTimeout
	}
	if backendTimeout <= 0 {
		return nil, fmt.Errorf("%w: backend timeout must be positive", ErrInvalidOption)
	}
	return &DistributedLimiter{
		backend:        config.Backend,
		mode:           config.Mode,
		local:          local,
		subject:        subject,
		backendTimeout: backendTimeout,
	}, nil
}

// Acquire checks shared rate and obtains a local concurrency permit.
func (l *DistributedLimiter) Acquire(ctx context.Context, target operation.Operation, request any, config policy.RateLimitPolicy) (*DistributedPermit, error) {
	if l == nil || ctx == nil {
		return nil, fmt.Errorf("%w: limiter or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if target.Transport() == "" || target.Service() == "" || target.Method() == "" {
		return nil, fmt.Errorf("%w: operation is invalid", ErrInvalidSubject)
	}
	if !config.Enabled {
		return &DistributedPermit{}, nil
	}
	if err := validate(config); err != nil {
		return nil, err
	}
	hasRate := config.RequestsPerSecond > 0
	if !hasRate {
		return l.acquireLocal(target.PolicyKey(), concurrencyPolicy(config), Decision{Allowed: true}, DegradationNone)
	}

	subject, err := l.subject(ctx, target, request)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSubject, err)
	}
	if err := validateSubject(subject); err != nil {
		return nil, err
	}
	if subject.Cost > config.Burst {
		l.recordRejected()
		return nil, ErrLimited
	}
	quota := Quota{
		Key:   subject.Key,
		Rate:  config.RequestsPerSecond,
		Burst: config.Burst,
		Cost:  subject.Cost,
	}
	backendCtx, cancel := context.WithTimeout(ctx, l.backendTimeout)
	decision, backendErr := l.backend.Allow(backendCtx, quota)
	cancel()
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if backendErr == nil {
		if decisionErr := validateDecision(decision); decisionErr != nil {
			backendErr = decisionErr
		} else {
			l.clearLastError()
			if !decision.Allowed {
				l.recordRejected()
				return nil, limitedError(decision)
			}
			return l.acquireLocal(target.PolicyKey(), concurrencyPolicy(config), decision, DegradationNone)
		}
	}

	l.recordBackendError(backendErr)
	switch l.mode {
	case FailClosed:
		l.recordRejected()
		return nil, kerrors.Wrap(backendErr, ErrUnavailable.Code(), ErrUnavailable.Reason(), ErrUnavailable.Message())
	case FailOpen:
		l.recordFailOpen()
		return l.acquireLocal(target.PolicyKey(), concurrencyPolicy(config), Decision{Allowed: true}, DegradationFailOpen)
	case LocalFallback:
		l.recordLocalFallback()
		return l.acquireLocal(target.PolicyKey(), config, Decision{Allowed: true}, DegradationLocalFallback)
	default:
		return nil, fmt.Errorf("%w: failure mode %d", ErrInvalidOption, l.mode)
	}
}

// Release returns the local concurrency slot exactly once.
func (p *DistributedPermit) Release() {
	if p == nil {
		return
	}
	p.local.Release()
}

// Decision returns the shared backend decision, or the admitted degraded
// decision.
func (p *DistributedPermit) Decision() Decision {
	if p == nil {
		return Decision{}
	}
	return p.decision
}

// Degraded reports whether shared quota enforcement was bypassed.
func (p *DistributedPermit) Degraded() bool {
	return p != nil && p.degradation != DegradationNone
}

// Degradation returns the explicit fallback path.
func (p *DistributedPermit) Degradation() Degradation {
	if p == nil {
		return DegradationNone
	}
	return p.degradation
}

// Describe returns low-cardinality counters without Subject keys.
func (l *DistributedLimiter) Describe() DistributedDescription {
	if l == nil {
		return DistributedDescription{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return DistributedDescription{
		Mode:           l.mode,
		Accepted:       l.accepted,
		Rejected:       l.rejected,
		BackendErrors:  l.backendErrors,
		FailOpen:       l.failOpen,
		LocalFallback:  l.localFallback,
		LastError:      l.lastError,
		BackendTimeout: l.backendTimeout,
	}
}

// LocalDescriptions returns sorted local concurrency/fallback state.
func (l *DistributedLimiter) LocalDescriptions() []Description {
	if l == nil {
		return nil
	}
	return l.local.Describe()
}

func (l *DistributedLimiter) acquireLocal(key string, config policy.RateLimitPolicy, decision Decision, degradation Degradation) (*DistributedPermit, error) {
	if !config.Enabled {
		l.recordAccepted()
		return &DistributedPermit{
			decision:    decision,
			degradation: degradation,
		}, nil
	}
	local, err := l.local.Get(key).Acquire(config)
	if err != nil {
		l.recordRejected()
		return nil, err
	}
	l.recordAccepted()
	return &DistributedPermit{
		local:       local,
		decision:    decision,
		degradation: degradation,
	}, nil
}

func (l *DistributedLimiter) recordAccepted() {
	l.mu.Lock()
	l.accepted++
	l.mu.Unlock()
}

func (l *DistributedLimiter) recordRejected() {
	l.mu.Lock()
	l.rejected++
	l.mu.Unlock()
}

func (l *DistributedLimiter) recordBackendError(err error) {
	l.mu.Lock()
	l.backendErrors++
	l.lastError = boundedError(err, 512)
	l.mu.Unlock()
}

func (l *DistributedLimiter) recordFailOpen() {
	l.mu.Lock()
	l.failOpen++
	l.mu.Unlock()
}

func (l *DistributedLimiter) recordLocalFallback() {
	l.mu.Lock()
	l.localFallback++
	l.mu.Unlock()
}

func (l *DistributedLimiter) clearLastError() {
	l.mu.Lock()
	l.lastError = ""
	l.mu.Unlock()
}

func concurrencyPolicy(config policy.RateLimitPolicy) policy.RateLimitPolicy {
	if config.MaxConcurrency <= 0 {
		return policy.RateLimitPolicy{}
	}
	return policy.RateLimitPolicy{
		Enabled:        true,
		MaxConcurrency: config.MaxConcurrency,
	}
}

func validateSubject(subject Subject) error {
	if strings.TrimSpace(subject.Key) != subject.Key ||
		subject.Key == "" ||
		len(subject.Key) > maxSubjectBytes ||
		!utf8.ValidString(subject.Key) ||
		subject.Cost <= 0 {
		return fmt.Errorf("%w: key or cost is invalid", ErrInvalidSubject)
	}
	for _, r := range subject.Key {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: key contains control characters", ErrInvalidSubject)
		}
	}
	return nil
}

func validateDecision(decision Decision) error {
	if decision.Remaining < 0 ||
		math.IsNaN(decision.Remaining) ||
		math.IsInf(decision.Remaining, 0) ||
		decision.RetryAfter < 0 {
		return fmt.Errorf("%w: negative or non-finite value", ErrInvalidDecision)
	}
	return nil
}

func limitedError(decision Decision) error {
	if decision.RetryAfter <= 0 {
		return ErrLimited
	}
	milliseconds := decision.RetryAfter.Milliseconds()
	if milliseconds <= 0 {
		milliseconds = 1
	}
	return ErrLimited.Clone(kerrors.WithMetadata(map[string]string{
		"retry-after-ms": strconv.FormatInt(milliseconds, 10),
	}))
}

func boundedError(err error, limit int) string {
	if err == nil {
		return ""
	}
	value := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, err.Error())
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
