package loadshed

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/metrics"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	defaultCPUComponentName  = "keelith.governance.runtime-cpu"
	defaultCPUSampleInterval = 250 * time.Millisecond
	minCPUSampleInterval     = 50 * time.Millisecond
	maxCPUSampleInterval     = 10 * time.Second
	totalCPUSecondsMetric    = "/cpu/classes/total:cpu-seconds"
	idleCPUSecondsMetric     = "/cpu/classes/idle:cpu-seconds"
)

var (
	// ErrCPUAlreadyStarted reports repeated RuntimeCPU Start.
	ErrCPUAlreadyStarted = errors.New("loadshed: runtime CPU already started")
	// ErrCPUUnsupported reports missing or malformed Go runtime CPU metrics.
	ErrCPUUnsupported = errors.New("loadshed: runtime CPU metrics unsupported")
)

// CPUState is the observable sampler lifecycle.
type CPUState string

const (
	// CPUStateNew means sampling has not started.
	CPUStateNew CPUState = "new"
	// CPUStateRunning means runtime CPU sampling is active.
	CPUStateRunning CPUState = "running"
	// CPUStateStopping means sampler shutdown is in progress.
	CPUStateStopping CPUState = "stopping"
	// CPUStateStopped means the sampler has exited.
	CPUStateStopped CPUState = "stopped"
)

// RuntimeCPUConfig configures a lock-free CPU provider backed by runtime
// scheduler metrics.
type RuntimeCPUConfig struct {
	Name     string
	Interval time.Duration
}

// CPUDescription is a value-free RuntimeCPU diagnostic.
type CPUDescription struct {
	State    CPUState
	Running  bool
	Usage    float64
	Samples  uint64
	Failures uint64
	Failed   bool
}

type cumulativeCPUReader func() (total float64, idle float64, err error)

// RuntimeCPU periodically converts cumulative Go runtime CPU classes into a
// normalized busy ratio. Usage performs only one atomic load.
//
// RuntimeCPU implements app.Component structurally.
type RuntimeCPU struct {
	name     string
	interval time.Duration
	read     cumulativeCPUReader

	usage    atomic.Uint64
	samples  atomic.Uint64
	failures atomic.Uint64
	failed   atomic.Bool

	mu          sync.Mutex
	state       CPUState
	cancel      context.CancelFunc
	done        chan struct{}
	initialized bool
	total       float64
	idle        float64
}

// NewRuntimeCPU constructs a sampler without starting a goroutine.
func NewRuntimeCPU(config RuntimeCPUConfig) (*RuntimeCPU, error) {
	rawName := config.Name
	name := strings.TrimSpace(config.Name)
	if name == "" {
		name = defaultCPUComponentName
	}
	if rawName != "" && rawName != name || !validCPUName(name) {
		return nil, fmt.Errorf("%w: CPU component name is invalid", ErrInvalidOption)
	}
	interval := config.Interval
	if interval == 0 {
		interval = defaultCPUSampleInterval
	}
	if interval < minCPUSampleInterval || interval > maxCPUSampleInterval {
		return nil, fmt.Errorf("%w: CPU sample interval must be between %s and %s", ErrInvalidOption, minCPUSampleInterval, maxCPUSampleInterval)
	}
	return &RuntimeCPU{
		name:     name,
		interval: interval,
		read:     readRuntimeCPU,
		state:    CPUStateNew,
		done:     make(chan struct{}),
	}, nil
}

// Name returns the stable App component name.
func (r *RuntimeCPU) Name() string {
	if r == nil {
		return ""
	}
	return r.name
}

// Dependencies returns no component prerequisites.
func (*RuntimeCPU) Dependencies() []string {
	return nil
}

// Start establishes a cumulative baseline before reporting Running.
func (r *RuntimeCPU) Start(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("%w: runtime CPU is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	r.mu.Lock()
	if r.state != CPUStateNew {
		r.mu.Unlock()
		return ErrCPUAlreadyStarted
	}
	total, idle, err := r.read()
	if err != nil {
		r.failures.Add(1)
		r.failed.Store(true)
		r.mu.Unlock()
		return err
	}
	r.failed.Store(false)
	runtimeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.total = total
	r.idle = idle
	r.initialized = true
	r.cancel = cancel
	r.state = CPUStateRunning
	done := r.done
	interval := r.interval
	r.mu.Unlock()

	go r.run(runtimeCtx, interval, done)
	return nil
}

// Stop terminates sampling and waits for the goroutine to exit.
func (r *RuntimeCPU) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	r.mu.Lock()
	switch r.state {
	case CPUStateNew:
		r.mu.Unlock()
		return nil
	case CPUStateRunning:
		r.state = CPUStateStopping
		cancel := r.cancel
		done := r.done
		r.mu.Unlock()
		cancel()
		return waitCPU(ctx, done)
	case CPUStateStopping:
		done := r.done
		r.mu.Unlock()
		return waitCPU(ctx, done)
	case CPUStateStopped:
		r.mu.Unlock()
		return nil
	default:
		r.mu.Unlock()
		return nil
	}
}

// Usage returns the last normalized busy ratio in [0, 1].
func (r *RuntimeCPU) Usage() float64 {
	if r == nil {
		return 0
	}
	return math.Float64frombits(r.usage.Load())
}

// Describe returns sampler lifecycle and aggregate health.
func (r *RuntimeCPU) Describe() CPUDescription {
	if r == nil {
		return CPUDescription{State: CPUStateStopped}
	}
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	return CPUDescription{
		State:    state,
		Running:  state == CPUStateRunning,
		Usage:    r.Usage(),
		Samples:  r.samples.Load(),
		Failures: r.failures.Load(),
		Failed:   r.failed.Load(),
	}
}

func (r *RuntimeCPU) run(
	ctx context.Context,
	interval time.Duration,
	done chan struct{},
) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.cancel = nil
			r.state = CPUStateStopped
			r.mu.Unlock()
			return
		case <-ticker.C:
			r.sample()
		}
	}
}

func (r *RuntimeCPU) sample() {
	total, idle, err := r.read()
	if err != nil {
		r.failures.Add(1)
		r.failed.Store(true)
		return
	}
	r.mu.Lock()
	if !r.initialized {
		r.total = total
		r.idle = idle
		r.initialized = true
		r.mu.Unlock()
		return
	}
	totalDelta := total - r.total
	idleDelta := idle - r.idle
	r.total = total
	r.idle = idle
	r.mu.Unlock()
	if totalDelta <= 0 ||
		math.IsNaN(totalDelta) ||
		math.IsInf(totalDelta, 0) {
		r.failures.Add(1)
		r.failed.Store(true)
		return
	}
	busyDelta := totalDelta - idleDelta
	usage := busyDelta / totalDelta
	if math.IsNaN(usage) || math.IsInf(usage, 0) {
		r.failures.Add(1)
		r.failed.Store(true)
		return
	}
	usage = math.Max(0, math.Min(1, usage))
	r.usage.Store(math.Float64bits(usage))
	r.samples.Add(1)
	r.failed.Store(false)
}

func readRuntimeCPU() (float64, float64, error) {
	samples := []metrics.Sample{
		{Name: totalCPUSecondsMetric},
		{Name: idleCPUSecondsMetric},
	}
	metrics.Read(samples)
	if samples[0].Value.Kind() != metrics.KindFloat64 ||
		samples[1].Value.Kind() != metrics.KindFloat64 {
		return 0, 0, ErrCPUUnsupported
	}
	total := samples[0].Value.Float64()
	idle := samples[1].Value.Float64()
	if total < 0 ||
		idle < 0 ||
		math.IsNaN(total) ||
		math.IsInf(total, 0) ||
		math.IsNaN(idle) ||
		math.IsInf(idle, 0) {
		return 0, 0, ErrCPUUnsupported
	}
	return total, idle, nil
}

func waitCPU(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func validCPUName(value string) bool {
	if value == "" ||
		len(value) > 256 ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
