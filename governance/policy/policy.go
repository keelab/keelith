// Package policy defines immutable, transport-neutral method policy snapshots.
package policy

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	// ErrInvalidPolicy means a resolved policy violates its invariants.
	ErrInvalidPolicy = errors.New("policy: invalid policy")
	// ErrInvalidDefinition means a policy definition is missing an identity or
	// contains conflicting rules.
	ErrInvalidDefinition = errors.New("policy: invalid definition")
)

const (
	minRetryAttempts = 2
	maxRetryAttempts = 10
	minHedgeAttempts = 2
	maxHedgeAttempts = 5
)

// RetryPolicy configures bounded retry behavior. It does not execute retries.
type RetryPolicy struct {
	Enabled     bool
	Idempotent  bool
	MaxAttempts int
	BackoffMin  time.Duration
	BackoffMax  time.Duration
	BudgetRatio float64
}

// HedgingPolicy configures bounded parallel hedging. It does not execute
// hedged requests.
type HedgingPolicy struct {
	Enabled     bool
	Idempotent  bool
	MaxAttempts int
	Delay       time.Duration
}

// BreakerPolicy configures circuit-breaker thresholds. It does not maintain
// breaker state.
type BreakerPolicy struct {
	Enabled        bool
	FailureRatio   float64
	Window         time.Duration
	MinRequests    int
	OpenTimeout    time.Duration
	HalfOpenProbes int
}

// BulkheadPolicy bounds concurrent logical calls to one dependency.
//
// MaxQueue zero means fail fast. A positive queue must always have a bounded
// QueueTimeout so callers without a deadline cannot wait forever.
type BulkheadPolicy struct {
	Enabled        bool
	MaxConcurrency int
	MaxQueue       int
	QueueTimeout   time.Duration
}

// RateLimitPolicy configures local rate and concurrency limits.
type RateLimitPolicy struct {
	Enabled           bool
	RequestsPerSecond float64
	Burst             int
	MaxConcurrency    int
}

// LoadSheddingPolicy configures local overload thresholds.
type LoadSheddingPolicy struct {
	Enabled        bool
	MaxConcurrency int
	CPUThreshold   float64
}

// StreamPolicy configures per-stream message quotas and shared message limits.
type StreamPolicy struct {
	Enabled            bool
	MaxSendMessages    int
	MaxReceiveMessages int
	MessagesPerSecond  float64
	Burst              int
	MaxConcurrency     int
}

// Policy is the fully resolved policy for an operation.
type Policy struct {
	Timeout      time.Duration
	Retry        RetryPolicy
	Hedging      HedgingPolicy
	Breaker      BreakerPolicy
	Bulkhead     BulkheadPolicy
	RateLimit    RateLimitPolicy
	LoadShedding LoadSheddingPolicy
	Stream       StreamPolicy
}

// Default returns Keelith's safe baseline policy.
func Default() Policy {
	return Policy{
		Timeout: 3 * time.Second,
		Retry: RetryPolicy{
			MaxAttempts: 3,
			BackoffMin:  10 * time.Millisecond,
			BackoffMax:  time.Second,
			BudgetRatio: 0.1,
		},
		Hedging: HedgingPolicy{
			MaxAttempts: 2,
			Delay:       50 * time.Millisecond,
		},
		Breaker: BreakerPolicy{
			FailureRatio:   0.5,
			Window:         30 * time.Second,
			MinRequests:    20,
			OpenTimeout:    10 * time.Second,
			HalfOpenProbes: 2,
		},
		Bulkhead: BulkheadPolicy{
			MaxConcurrency: 100,
		},
	}
}

// Validate checks a fully resolved policy.
func Validate(value Policy) error {
	if value.Timeout <= 0 {
		return invalidPolicy("timeout must be positive")
	}
	if err := validateRetry(value.Retry); err != nil {
		return err
	}
	if err := validateHedging(value.Hedging); err != nil {
		return err
	}
	if value.Retry.Enabled && value.Hedging.Enabled {
		return invalidPolicy("retry and hedging are mutually exclusive")
	}
	if err := validateBreaker(value.Breaker); err != nil {
		return err
	}
	if err := validateBulkhead(value.Bulkhead); err != nil {
		return err
	}
	if err := validateRateLimit(value.RateLimit); err != nil {
		return err
	}
	if err := validateLoadShedding(value.LoadShedding); err != nil {
		return err
	}
	if err := ValidateStream(value.Stream); err != nil {
		return err
	}
	return nil
}

// Optional distinguishes an omitted patch field from an explicit zero value.
type Optional[T any] struct {
	value T
	set   bool
}

// Some returns an Optional containing value.
func Some[T any](value T) Optional[T] {
	return Optional[T]{value: value, set: true}
}

// Get returns the contained value and whether it was explicitly set.
func (optional Optional[T]) Get() (T, bool) {
	return optional.value, optional.set
}

// Patch selectively replaces fields in a Policy.
type Patch struct {
	Timeout      Optional[time.Duration]
	Retry        Optional[RetryPolicy]
	Hedging      Optional[HedgingPolicy]
	Breaker      Optional[BreakerPolicy]
	Bulkhead     Optional[BulkheadPolicy]
	RateLimit    Optional[RateLimitPolicy]
	LoadShedding Optional[LoadSheddingPolicy]
	Stream       Optional[StreamPolicy]
}

func (patch Patch) apply(base Policy) Policy {
	if value, ok := patch.Timeout.Get(); ok {
		base.Timeout = value
	}
	if value, ok := patch.Retry.Get(); ok {
		base.Retry = value
	}
	if value, ok := patch.Hedging.Get(); ok {
		base.Hedging = value
	}
	if value, ok := patch.Breaker.Get(); ok {
		base.Breaker = value
	}
	if value, ok := patch.Bulkhead.Get(); ok {
		base.Bulkhead = value
	}
	if value, ok := patch.RateLimit.Get(); ok {
		base.RateLimit = value
	}
	if value, ok := patch.LoadShedding.Get(); ok {
		base.LoadShedding = value
	}
	if value, ok := patch.Stream.Get(); ok {
		base.Stream = value
	}
	return base
}

func validateRetry(value RetryPolicy) error {
	if value.MaxAttempts < 0 ||
		value.BackoffMin < 0 ||
		value.BackoffMax < 0 ||
		invalidRatio(value.BudgetRatio) {
		return invalidPolicy("retry contains a negative or non-finite value")
	}
	if !value.Enabled {
		return nil
	}
	if !value.Idempotent {
		return invalidPolicy("retry requires an idempotent operation")
	}
	if value.MaxAttempts < minRetryAttempts || value.MaxAttempts > maxRetryAttempts {
		return invalidPolicy("retry max attempts must be between %d and %d", minRetryAttempts, maxRetryAttempts)
	}
	if value.BackoffMin <= 0 || value.BackoffMax < value.BackoffMin {
		return invalidPolicy("retry backoff range is invalid")
	}
	if value.BudgetRatio <= 0 || value.BudgetRatio > 1 {
		return invalidPolicy("retry budget ratio must be in (0, 1]")
	}
	return nil
}

func validateHedging(value HedgingPolicy) error {
	if value.MaxAttempts < 0 || value.Delay < 0 {
		return invalidPolicy("hedging contains a negative value")
	}
	if !value.Enabled {
		return nil
	}
	if !value.Idempotent {
		return invalidPolicy("hedging requires an idempotent operation")
	}
	if value.MaxAttempts < minHedgeAttempts || value.MaxAttempts > maxHedgeAttempts {
		return invalidPolicy("hedging max attempts must be between %d and %d", minHedgeAttempts, maxHedgeAttempts)
	}
	if value.Delay <= 0 {
		return invalidPolicy("hedging delay must be positive")
	}
	return nil
}

func validateBreaker(value BreakerPolicy) error {
	if invalidRatio(value.FailureRatio) ||
		value.Window < 0 ||
		value.MinRequests < 0 ||
		value.OpenTimeout < 0 ||
		value.HalfOpenProbes < 0 {
		return invalidPolicy("breaker contains a negative or non-finite value")
	}
	if !value.Enabled {
		return nil
	}
	if value.FailureRatio <= 0 || value.FailureRatio > 1 {
		return invalidPolicy("breaker failure ratio must be in (0, 1]")
	}
	if value.Window <= 0 ||
		value.MinRequests <= 0 ||
		value.OpenTimeout <= 0 ||
		value.HalfOpenProbes <= 0 {
		return invalidPolicy("breaker thresholds must be positive")
	}
	return nil
}

func validateBulkhead(value BulkheadPolicy) error {
	if value.MaxConcurrency < 0 ||
		value.MaxQueue < 0 ||
		value.QueueTimeout < 0 {
		return invalidPolicy("bulkhead contains a negative value")
	}
	if !value.Enabled {
		return nil
	}
	if value.MaxConcurrency <= 0 {
		return invalidPolicy("bulkhead max concurrency must be positive")
	}
	if value.MaxQueue > 0 && value.QueueTimeout <= 0 {
		return invalidPolicy("bulkhead queue requires a positive timeout")
	}
	if value.MaxQueue == 0 && value.QueueTimeout > 0 {
		return invalidPolicy("bulkhead queue timeout requires a queue")
	}
	return nil
}

func validateRateLimit(value RateLimitPolicy) error {
	if math.IsNaN(value.RequestsPerSecond) ||
		math.IsInf(value.RequestsPerSecond, 0) ||
		value.RequestsPerSecond < 0 ||
		value.Burst < 0 ||
		value.MaxConcurrency < 0 {
		return invalidPolicy("rate limit contains a negative or non-finite value")
	}
	if !value.Enabled {
		return nil
	}
	hasRate := value.RequestsPerSecond > 0 || value.Burst > 0
	if hasRate && (value.RequestsPerSecond <= 0 || value.Burst <= 0) {
		return invalidPolicy("rate limit requires both requests per second and burst")
	}
	if !hasRate && value.MaxConcurrency <= 0 {
		return invalidPolicy("rate limit requires a rate or concurrency limit")
	}
	return nil
}

func validateLoadShedding(value LoadSheddingPolicy) error {
	if value.MaxConcurrency < 0 ||
		math.IsNaN(value.CPUThreshold) ||
		math.IsInf(value.CPUThreshold, 0) ||
		value.CPUThreshold < 0 ||
		value.CPUThreshold > 1 {
		return invalidPolicy("load shedding contains an invalid threshold")
	}
	if value.Enabled && value.MaxConcurrency <= 0 && value.CPUThreshold <= 0 {
		return invalidPolicy("load shedding requires a concurrency or CPU threshold")
	}
	return nil
}

// ValidateStream checks one resolved stream message policy.
func ValidateStream(value StreamPolicy) error {
	if value.MaxSendMessages < 0 ||
		value.MaxReceiveMessages < 0 ||
		math.IsNaN(value.MessagesPerSecond) ||
		math.IsInf(value.MessagesPerSecond, 0) ||
		value.MessagesPerSecond < 0 ||
		value.Burst < 0 ||
		value.MaxConcurrency < 0 {
		return invalidPolicy("stream policy contains an invalid limit")
	}
	if !value.Enabled {
		return nil
	}
	hasRate := value.MessagesPerSecond > 0 || value.Burst > 0
	if hasRate && (value.MessagesPerSecond <= 0 || value.Burst <= 0) {
		return invalidPolicy(
			"stream message rate requires both messages per second and burst",
		)
	}
	if value.MaxSendMessages <= 0 &&
		value.MaxReceiveMessages <= 0 &&
		!hasRate &&
		value.MaxConcurrency <= 0 {
		return invalidPolicy("stream policy requires at least one limit")
	}
	return nil
}

func invalidRatio(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0) || value < 0
}

func invalidPolicy(format string, values ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPolicy, fmt.Sprintf(format, values...))
}
