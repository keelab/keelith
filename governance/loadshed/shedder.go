// Package loadshed provides CPU-aware BBR-like concurrency shedding.
package loadshed

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

const (
	sampleWindow = time.Second
	bbrGain      = 2.0
)

var (
	// ErrOverloaded reports that the shedder rejected an invocation.
	ErrOverloaded = kerrors.New(503, "OVERLOADED", "instance is overloaded")
	// ErrInvalidOption reports an invalid shedder dependency or policy.
	ErrInvalidOption = errors.New("loadshed: invalid option")
)

// Clock supplies deterministic latency/window time.
type Clock interface {
	Now() time.Time
}

// CPU reports normalized process or host CPU usage in [0, 1].
type CPU interface {
	Usage() float64
}

// Description is a low-cardinality shedder snapshot.
type Description struct {
	Key           string
	Inflight      int
	AdaptiveLimit int
	CPUUsage      float64
	Accepted      uint64
	Rejected      uint64
	MinLatency    time.Duration
}

// Shedder combines a hard concurrency cap with a CPU-gated BBR-like estimate.
type Shedder struct {
	clock Clock
	cpu   CPU

	mu            sync.Mutex
	initialized   bool
	windowStart   time.Time
	completed     uint64
	minLatency    time.Duration
	adaptiveLimit int
	inflight      int
	accepted      uint64
	rejected      uint64
}

// Permit records completion latency and releases one inflight slot.
type Permit struct {
	shedder *Shedder
	start   time.Time
	once    sync.Once
}

// NewShedder creates an isolated Shedder.
func NewShedder(clock Clock, cpu CPU) *Shedder {
	if isNil(clock) {
		clock = systemClock{}
	}
	if isNil(cpu) {
		cpu = zeroCPU{}
	}
	return &Shedder{clock: clock, cpu: cpu}
}

// Acquire enforces hard and adaptive limits.
func (s *Shedder) Acquire(
	config policy.LoadSheddingPolicy,
) (*Permit, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: shedder is nil", ErrInvalidOption)
	}
	if !config.Enabled {
		return &Permit{}, nil
	}
	if err := validate(config); err != nil {
		return nil, err
	}
	now := s.clock.Now()
	usage := s.cpu.Usage()
	if math.IsNaN(usage) || math.IsInf(usage, 0) {
		usage = 1
	}
	usage = math.Max(0, math.Min(1, usage))

	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollWindow(now, config)
	hardLimited := config.MaxConcurrency > 0 &&
		s.inflight >= config.MaxConcurrency
	adaptiveLimited := config.CPUThreshold > 0 &&
		usage >= config.CPUThreshold &&
		s.inflight >= s.adaptiveLimit
	if hardLimited || adaptiveLimited {
		s.rejected++
		return nil, ErrOverloaded
	}
	s.inflight++
	s.accepted++
	return &Permit{shedder: s, start: now}, nil
}

// Release records completion and returns one inflight slot.
func (permit *Permit) Release() {
	if permit == nil || permit.shedder == nil {
		return
	}
	permit.once.Do(func() {
		now := permit.shedder.clock.Now()
		latency := now.Sub(permit.start)
		if latency <= 0 {
			latency = time.Nanosecond
		}
		permit.shedder.mu.Lock()
		if permit.shedder.inflight > 0 {
			permit.shedder.inflight--
		}
		permit.shedder.completed++
		if permit.shedder.minLatency == 0 ||
			latency < permit.shedder.minLatency {
			permit.shedder.minLatency = latency
		}
		permit.shedder.mu.Unlock()
	})
}

// Describe returns a debug snapshot.
func (s *Shedder) Describe() Description {
	if s == nil {
		return Description{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return Description{
		Inflight:      s.inflight,
		AdaptiveLimit: s.adaptiveLimit,
		CPUUsage:      s.cpu.Usage(),
		Accepted:      s.accepted,
		Rejected:      s.rejected,
		MinLatency:    s.minLatency,
	}
}

func (s *Shedder) rollWindow(now time.Time, config policy.LoadSheddingPolicy) {
	if !s.initialized {
		s.initialized = true
		s.windowStart = now
		s.adaptiveLimit = config.MaxConcurrency
		if s.adaptiveLimit <= 0 {
			s.adaptiveLimit = 1
		}
		return
	}
	elapsed := now.Sub(s.windowStart)
	if elapsed < sampleWindow && elapsed >= 0 {
		return
	}
	if s.completed > 0 && s.minLatency > 0 && elapsed > 0 {
		throughput := float64(s.completed) / elapsed.Seconds()
		estimate := int(math.Ceil(
			throughput * s.minLatency.Seconds() * bbrGain,
		))
		if estimate < 1 {
			estimate = 1
		}
		if config.MaxConcurrency > 0 && estimate > config.MaxConcurrency {
			estimate = config.MaxConcurrency
		}
		s.adaptiveLimit = estimate
	}
	s.windowStart = now
	s.completed = 0
	s.minLatency = 0
}

func validate(config policy.LoadSheddingPolicy) error {
	if config.MaxConcurrency < 0 ||
		config.CPUThreshold < 0 ||
		config.CPUThreshold > 1 ||
		math.IsNaN(config.CPUThreshold) ||
		math.IsInf(config.CPUThreshold, 0) ||
		config.MaxConcurrency <= 0 && config.CPUThreshold <= 0 {
		return fmt.Errorf("%w: resolved load-shed policy is invalid", policy.ErrInvalidPolicy)
	}
	return nil
}

// Pool owns shedder state by stable Operation policy key.
type Pool struct {
	clock Clock
	cpu   CPU

	mu       sync.Mutex
	shedders map[string]*Shedder
}

// NewPool creates an isolated shedder pool.
func NewPool(clock Clock, cpu CPU) *Pool {
	if isNil(clock) {
		clock = systemClock{}
	}
	if isNil(cpu) {
		cpu = zeroCPU{}
	}
	return &Pool{
		clock:    clock,
		cpu:      cpu,
		shedders: make(map[string]*Shedder),
	}
}

// Get returns the stable shedder for key.
func (p *Pool) Get(key string) *Shedder {
	if p == nil {
		return nil
	}
	normalized := strings.TrimSpace(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	subject := p.shedders[normalized]
	if subject == nil {
		subject = NewShedder(p.clock, p.cpu)
		p.shedders[normalized] = subject
	}
	return subject
}

// Describe returns sorted low-cardinality snapshots.
func (p *Pool) Describe() []Description {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	entries := make(map[string]*Shedder, len(p.shedders))
	for key, shedder := range p.shedders {
		entries[key] = shedder
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

type zeroCPU struct{}

func (zeroCPU) Usage() float64 {
	return 0
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
