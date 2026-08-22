// Package ratelimit provides per-Operation token and concurrency limits.
package ratelimit

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/policy"
)

var (
	// ErrLimited reports that a rate or concurrency limit rejected a call.
	ErrLimited = kerrors.New(429, "RATE_LIMITED", "request rate is limited")
	// ErrInvalidOption reports an invalid limiter dependency or policy.
	ErrInvalidOption = errors.New("ratelimit: invalid option")
)

// Clock supplies deterministic token refill time.
type Clock interface {
	Now() time.Time
}

// Description is a low-cardinality limiter snapshot.
type Description struct {
	Key      string
	Tokens   float64
	Inflight int
	Accepted uint64
	Rejected uint64
}

// Limiter combines a token bucket with a concurrency gate.
type Limiter struct {
	clock Clock

	mu          sync.Mutex
	initialized bool
	rate        float64
	burst       int
	last        time.Time
	tokens      float64
	inflight    int
	accepted    uint64
	rejected    uint64
}

// Permit releases one concurrency slot.
type Permit struct {
	limiter *Limiter
	once    sync.Once
}

// NewLimiter creates an isolated Limiter.
func NewLimiter(clock Clock) *Limiter {
	if isNil(clock) {
		clock = systemClock{}
	}
	return &Limiter{clock: clock}
}

// Acquire consumes one token and concurrency slot.
func (l *Limiter) Acquire(config policy.RateLimitPolicy) (*Permit, error) {
	if l == nil {
		return nil, fmt.Errorf("%w: limiter is nil", ErrInvalidOption)
	}
	if !config.Enabled {
		return &Permit{}, nil
	}
	if err := validate(config); err != nil {
		return nil, err
	}
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.configure(now, config)
	l.refill(now)
	if config.MaxConcurrency > 0 && l.inflight >= config.MaxConcurrency {
		l.rejected++
		return nil, ErrLimited
	}
	if config.RequestsPerSecond > 0 && l.tokens < 1 {
		l.rejected++
		return nil, ErrLimited
	}
	if config.RequestsPerSecond > 0 {
		l.tokens--
	}
	l.inflight++
	l.accepted++
	return &Permit{limiter: l}, nil
}

// Release returns one concurrency slot. Repeated calls are ignored.
func (p *Permit) Release() {
	if p == nil || p.limiter == nil {
		return
	}
	p.once.Do(func() {
		p.limiter.mu.Lock()
		if p.limiter.inflight > 0 {
			p.limiter.inflight--
		}
		p.limiter.mu.Unlock()
	})
}

// Describe returns a debug snapshot.
func (l *Limiter) Describe() Description {
	if l == nil {
		return Description{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return Description{
		Tokens:   l.tokens,
		Inflight: l.inflight,
		Accepted: l.accepted,
		Rejected: l.rejected,
	}
}

func (l *Limiter) configure(now time.Time, config policy.RateLimitPolicy) {
	if l.initialized && l.rate == config.RequestsPerSecond && l.burst == config.Burst {
		return
	}
	l.initialized = true
	l.rate = config.RequestsPerSecond
	l.burst = config.Burst
	l.last = now
	l.tokens = float64(config.Burst)
}

func (l *Limiter) refill(now time.Time) {
	if l.rate <= 0 || now.Before(l.last) {
		l.last = now
		return
	}
	elapsed := now.Sub(l.last).Seconds()
	l.tokens = math.Min(
		float64(l.burst),
		l.tokens+elapsed*l.rate,
	)
	l.last = now
}

func validate(config policy.RateLimitPolicy) error {
	hasRate := config.RequestsPerSecond > 0 || config.Burst > 0
	if config.RequestsPerSecond < 0 ||
		math.IsNaN(config.RequestsPerSecond) ||
		math.IsInf(config.RequestsPerSecond, 0) ||
		config.Burst < 0 ||
		config.MaxConcurrency < 0 ||
		hasRate && (config.RequestsPerSecond <= 0 || config.Burst <= 0) ||
		!hasRate && config.MaxConcurrency <= 0 {
		return fmt.Errorf("%w: resolved rate-limit policy is invalid", policy.ErrInvalidPolicy)
	}
	return nil
}

// Pool owns limiter state by stable Operation policy key.
type Pool struct {
	clock Clock

	mu       sync.Mutex
	limiters map[string]*Limiter
}

// NewPool creates an isolated limiter pool.
func NewPool(clock Clock) *Pool {
	if isNil(clock) {
		clock = systemClock{}
	}
	return &Pool{clock: clock, limiters: make(map[string]*Limiter)}
}

// Get returns the stable limiter for key.
func (p *Pool) Get(key string) *Limiter {
	if p == nil {
		return nil
	}
	normalized := strings.TrimSpace(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	subject := p.limiters[normalized]
	if subject == nil {
		subject = NewLimiter(p.clock)
		p.limiters[normalized] = subject
	}
	return subject
}

// Describe returns sorted low-cardinality snapshots.
func (p *Pool) Describe() []Description {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	entries := make(map[string]*Limiter, len(p.limiters))
	for key, limiter := range p.limiters {
		entries[key] = limiter
	}
	p.mu.Unlock()
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Description, 0, len(keys))

	for _, key := range keys {
		description := entries[key].Describe()
		description.Key = key
		result = append(result, description)
	}
	return result
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
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
