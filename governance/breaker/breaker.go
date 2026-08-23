// Package breaker provides explainable service and instance circuit breakers.
package breaker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/governance/policy"
)

var (
	// ErrOpen is non-retryable so rejections cannot create retry storms.
	ErrOpen = kerrors.New(503, "CIRCUIT_OPEN", "circuit breaker is open")
	// ErrInvalidOption means a breaker dependency or scope is invalid.
	ErrInvalidOption = errors.New("breaker: invalid option")
)

// State is the stable circuit state.
type State string

const (
	// StateClosed accepts invocations and records outcomes.
	StateClosed State = "closed"
	// StateOpen rejects invocations until the open deadline.
	StateOpen State = "open"
	// StateHalfOpen admits bounded probes before recovery.
	StateHalfOpen State = "half-open"
)

// Scope separates aggregate service health from one selected instance.
type Scope string

const (
	// ScopeService aggregates failures across a logical service.
	ScopeService Scope = "service"
	// ScopeInstance isolates failures for one selected instance.
	ScopeInstance Scope = "instance"
)

// Clock supplies deterministic state-machine time.
type Clock interface {
	Now() time.Time
}

// Description is a low-cardinality debug snapshot.
type Description struct {
	Scope          Scope
	Key            string
	State          State
	Requests       uint64
	Failures       uint64
	OpenUntil      time.Time
	ProbesInFlight int
	ProbeSuccesses int
}

// Breaker is one isolated state machine.
type Breaker struct {
	clock Clock

	mu             sync.Mutex
	state          State
	windowStart    time.Time
	requests       uint64
	failures       uint64
	openUntil      time.Time
	probesInFlight int
	probeSuccesses int
	generation     uint64
}

// Permit must be completed exactly once after an allowed invocation.
type Permit struct {
	breaker    *Breaker
	config     policy.BreakerPolicy
	state      State
	generation uint64
	once       sync.Once
}

// NewBreaker creates one closed state machine.
func NewBreaker(clock Clock) *Breaker {
	if isNil(clock) {
		clock = systemClock{}
	}
	return &Breaker{clock: clock, state: StateClosed}
}

// Allow advances timed transitions and returns a completion Permit.
func (b *Breaker) Allow(config policy.BreakerPolicy) (*Permit, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: breaker is nil", ErrInvalidOption)
	}
	if !config.Enabled {
		return &Permit{}, nil
	}
	if err := validate(config); err != nil {
		return nil, err
	}

	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		b.rollWindow(now, config.Window)
	case StateOpen:
		if now.Before(b.openUntil) {
			return nil, ErrOpen
		}
		b.state = StateHalfOpen
		b.probesInFlight = 0
		b.probeSuccesses = 0
	case StateHalfOpen:
	default:
		b.state = StateClosed
		b.rollWindow(now, config.Window)
	}
	if b.state == StateHalfOpen {
		if b.probesInFlight >= config.HalfOpenProbes {
			return nil, ErrOpen
		}
		b.probesInFlight++
	}
	return &Permit{
		breaker:    b,
		config:     config,
		state:      b.state,
		generation: b.generation,
	}, nil
}

// Done records one allowed invocation result. Repeated calls are ignored.
func (permit *Permit) Done(err error) {
	if permit == nil || permit.breaker == nil {
		return
	}
	permit.once.Do(func() {
		permit.breaker.complete(
			permit.config,
			permit.state,
			permit.generation,
			err,
		)
	})
}

// Probe reports whether this permit was admitted in half-open state.
func (permit *Permit) Probe() bool {
	return permit != nil &&
		permit.breaker != nil &&
		permit.state == StateHalfOpen
}

// Describe returns an immutable state snapshot.
func (b *Breaker) Describe() Description {
	if b == nil {
		return Description{State: StateOpen}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return Description{
		State:          b.state,
		Requests:       b.requests,
		Failures:       b.failures,
		OpenUntil:      b.openUntil,
		ProbesInFlight: b.probesInFlight,
		ProbeSuccesses: b.probeSuccesses,
	}
}

func (b *Breaker) complete(config policy.BreakerPolicy, permittedState State, generation uint64, err error) {
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation != b.generation || permittedState != b.state {
		return
	}
	failed := instanceFailure(err)
	switch b.state {
	case StateClosed:
		b.rollWindow(now, config.Window)
		b.requests++
		if failed {
			b.failures++
		}
		if b.requests >= uint64(config.MinRequests) &&
			float64(b.failures)/float64(b.requests) >= config.FailureRatio {
			b.open(now, config.OpenTimeout)
		}
	case StateHalfOpen:
		if b.probesInFlight > 0 {
			b.probesInFlight--
		}
		if failed {
			b.open(now, config.OpenTimeout)
			return
		}
		b.probeSuccesses++
		if b.probeSuccesses >= config.HalfOpenProbes {
			b.close(now)
		}
	}
}

func (b *Breaker) rollWindow(now time.Time, window time.Duration) {
	if b.windowStart.IsZero() ||
		now.Sub(b.windowStart) >= window ||
		now.Before(b.windowStart) {
		b.windowStart = now
		b.requests = 0
		b.failures = 0
	}
}

func (b *Breaker) open(now time.Time, duration time.Duration) {
	b.state = StateOpen
	b.openUntil = now.Add(duration)
	b.probesInFlight = 0
	b.probeSuccesses = 0
	b.generation++
}

func (b *Breaker) close(now time.Time) {
	b.state = StateClosed
	b.windowStart = now
	b.requests = 0
	b.failures = 0
	b.openUntil = time.Time{}
	b.probesInFlight = 0
	b.probeSuccesses = 0
	b.generation++
}

func instanceFailure(err error) bool {
	switch failure.Classify(err) {
	case failure.Transport, failure.Timeout:
		return true
	default:
		return false
	}
}

func validate(config policy.BreakerPolicy) error {
	if config.FailureRatio <= 0 ||
		config.FailureRatio > 1 ||
		config.Window <= 0 ||
		config.MinRequests <= 0 ||
		config.OpenTimeout <= 0 ||
		config.HalfOpenProbes <= 0 {
		return fmt.Errorf("%w: resolved breaker policy is invalid", policy.ErrInvalidPolicy)
	}
	return nil
}

// Pool owns breaker state by explicit low-cardinality scope and key.
type Pool struct {
	clock Clock

	mu       sync.Mutex
	breakers map[poolKey]*Breaker
}

type poolKey struct {
	scope Scope
	key   string
}

// NewPool creates an isolated breaker pool.
func NewPool(clock Clock) *Pool {
	if isNil(clock) {
		clock = systemClock{}
	}
	return &Pool{clock: clock, breakers: make(map[poolKey]*Breaker)}
}

// Get returns the stable breaker for scope/key.
func (p *Pool) Get(scope Scope, key string) *Breaker {
	if p == nil {
		return nil
	}
	normalized := strings.TrimSpace(key)
	identity := poolKey{scope: scope, key: normalized}
	p.mu.Lock()
	defer p.mu.Unlock()
	subject := p.breakers[identity]
	if subject == nil {
		subject = NewBreaker(p.clock)
		p.breakers[identity] = subject
	}
	return subject
}

// Describe returns sorted low-cardinality snapshots.
func (p *Pool) Describe() []Description {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	entries := make([]struct {
		identity poolKey
		breaker  *Breaker
	}, 0, len(p.breakers))
	for identity, subject := range p.breakers {
		entries = append(entries, struct {
			identity poolKey
			breaker  *Breaker
		}{identity: identity, breaker: subject})
	}
	p.mu.Unlock()

	result := make([]Description, 0, len(entries))
	for _, entry := range entries {
		description := entry.breaker.Describe()
		description.Scope = entry.identity.scope
		description.Key = entry.identity.key
		result = append(result, description)
	}
	sort.Slice(result, func(first, second int) bool {
		if result[first].Scope == result[second].Scope {
			return result[first].Key < result[second].Key
		}
		return result[first].Scope < result[second].Scope
	})
	return result
}

type instanceContextKey struct{}

// WithInstance records the selected stable instance identity.
func WithInstance(ctx context.Context, instance string) context.Context {
	return context.WithValue(ctx, instanceContextKey{}, strings.TrimSpace(instance))
}

func instanceFromContext(ctx context.Context) (string, bool) {
	instance, ok := ctx.Value(instanceContextKey{}).(string)
	return instance, ok && instance != ""
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
