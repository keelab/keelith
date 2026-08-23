package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/keelab/keelith/programmable/topology"
)

const (
	defaultApplyTimeout = 30 * time.Second
	defaultDrainTimeout = 30 * time.Second
	defaultMinBackoff   = 100 * time.Millisecond
	defaultMaxBackoff   = 10 * time.Second
)

var (
	// ErrInvalidController reports missing dependencies or unsafe bounds.
	ErrInvalidController = errors.New("topology control: invalid controller")
	// ErrRevisionRollback rejects a control revision older than one observed.
	ErrRevisionRollback = errors.New("topology control: revision rollback")
	// ErrRevisionConflict rejects different content at one revision.
	ErrRevisionConflict = errors.New("topology control: revision conflict")
	// ErrEpochRollback rejects a new revision that does not advance runtime epoch.
	ErrEpochRollback = errors.New("topology control: epoch rollback")
)

// Target owns the process-local Stage, Ready and Drain lifecycle.
type Target interface {
	Stage(context.Context, topology.Snapshot) error
	Ready(context.Context, uint64) (uint64, error)
	Drain(context.Context, uint64) error
}

// DrainPlanner lets weighted targets retain Ready epochs with non-zero
// traffic and return only zero-weight epochs eligible for cleanup.
type DrainPlanner interface {
	Drainable(context.Context) ([]uint64, error)
}

// Sleeper provides cancellable bounded backoff and permits deterministic tests.
type Sleeper func(context.Context, time.Duration) error

// EventKind is one low-cardinality control-plane transition.
type EventKind string

const (
	// EventObserved records a structurally valid candidate.
	EventObserved EventKind = "observed"
	// EventApplied records a Ready candidate.
	EventApplied EventKind = "applied"
	// EventRejected records a candidate kept away from the target.
	EventRejected EventKind = "rejected"
	// EventReconnect records a source watcher restart.
	EventReconnect EventKind = "reconnect"
)

// FailureClass is a fixed, payload-free operational failure identity.
type FailureClass string

const (
	// FailureNone reports a healthy controller.
	FailureNone FailureClass = ""
	// FailureSource reports a load, watch or reconnect failure.
	FailureSource FailureClass = "source"
	// FailureSignature reports an unauthenticated candidate.
	FailureSignature FailureClass = "signature"
	// FailureRevision reports a rollback or conflicting revision.
	FailureRevision FailureClass = "revision"
	// FailureEpoch reports a runtime epoch rollback.
	FailureEpoch FailureClass = "epoch"
	// FailureStage reports a candidate construction failure.
	FailureStage FailureClass = "stage"
	// FailureReady reports a failed atomic readiness transition.
	FailureReady FailureClass = "ready"
	// FailureDrain reports a failed old-epoch drain.
	FailureDrain FailureClass = "drain"
)

// Event contains no plan body, signature, component identity or raw error.
type Event struct {
	Kind         EventKind
	Revision     uint64
	Epoch        uint64
	FailureClass FailureClass
}

// Observer receives bounded control-plane lifecycle observations.
type Observer interface{ Observe(context.Context, Event) }

// ObserverFunc adapts an observation function.
type ObserverFunc func(context.Context, Event)

// Observe invokes the adapted observer.
func (fn ObserverFunc) Observe(ctx context.Context, event Event) {
	fn(ctx, event)
}

// ControllerConfig configures serial candidate application and watch recovery.
type ControllerConfig struct {
	Source        Source
	Target        Target
	Verifier      Verifier
	AllowUnsigned bool
	ApplyTimeout  time.Duration
	DrainTimeout  time.Duration
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	Sleep         Sleeper
	Observer      Observer
}

// Status is one payload-free immutable Controller snapshot.
type Status struct {
	ObservedRevision uint64
	AppliedRevision  uint64
	Epoch            uint64
	Hash             string
	Reconnects       uint64
	Degraded         bool
	FailureClass     FailureClass
}

// Controller serializes source updates around one last-known-good target.
type Controller struct {
	source        Source
	target        Target
	verifier      Verifier
	allowUnsigned bool
	applyTimeout  time.Duration
	drainTimeout  time.Duration
	minBackoff    time.Duration
	maxBackoff    time.Duration
	sleep         Sleeper
	observer      Observer

	apply    sync.Mutex
	mu       sync.RWMutex
	seenHash string
	status   Status
}

// NewController validates and snapshots one topology control loop.
func NewController(config ControllerConfig) (*Controller, error) {
	if isNilControlValue(config.Source) || isNilControlValue(config.Target) ||
		config.AllowUnsigned == !isNilControlValue(config.Verifier) ||
		config.Observer != nil && isNilControlValue(config.Observer) {
		return nil, ErrInvalidController
	}
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = defaultApplyTimeout
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = defaultDrainTimeout
	}
	if config.MinBackoff == 0 {
		config.MinBackoff = defaultMinBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaultMaxBackoff
	}
	if config.Sleep == nil {
		config.Sleep = sleepContext
	}
	if config.ApplyTimeout <= 0 || config.ApplyTimeout > 10*time.Minute ||
		config.DrainTimeout <= 0 || config.DrainTimeout > time.Hour ||
		config.MinBackoff <= 0 || config.MaxBackoff < config.MinBackoff ||
		config.MaxBackoff > time.Minute {
		return nil, ErrInvalidController
	}
	return &Controller{
		source: config.Source, target: config.Target, verifier: config.Verifier,
		allowUnsigned: config.AllowUnsigned,
		applyTimeout:  config.ApplyTimeout, drainTimeout: config.DrainTimeout,
		minBackoff: config.MinBackoff, maxBackoff: config.MaxBackoff,
		sleep: config.Sleep, observer: config.Observer,
	}, nil
}

// Apply validates and serially publishes one complete candidate.
func (c *Controller) Apply(ctx context.Context, candidate Candidate) error {
	if c == nil || ctx == nil || candidate.Revision() == 0 ||
		candidate.Hash() == "" {
		return ErrInvalidController
	}
	c.apply.Lock()
	defer c.apply.Unlock()
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if !c.allowUnsigned {
		if err := c.verifier.Verify(ctx, candidate); err != nil {
			c.reject(ctx, candidate, FailureSignature)
			return errors.Join(ErrInvalidSignature, err)
		}
	}
	c.mu.RLock()
	status := c.status
	seenHash := c.seenHash
	c.mu.RUnlock()
	if candidate.Revision() < status.ObservedRevision {
		c.reject(ctx, candidate, FailureRevision)
		return ErrRevisionRollback
	}
	if candidate.Revision() == status.ObservedRevision {
		if candidate.Hash() == seenHash {
			return nil
		}
		c.reject(ctx, candidate, FailureRevision)
		return ErrRevisionConflict
	}
	if status.Epoch != 0 && candidate.Epoch() <= status.Epoch {
		c.reject(ctx, candidate, FailureEpoch)
		return ErrEpochRollback
	}
	c.mu.Lock()
	c.status.ObservedRevision = candidate.Revision()
	c.seenHash = candidate.Hash()
	c.mu.Unlock()
	c.observe(ctx, Event{
		Kind: EventObserved, Revision: candidate.Revision(), Epoch: candidate.Epoch(),
	})

	applyCtx, cancel := context.WithTimeout(ctx, c.applyTimeout)
	err := c.target.Stage(applyCtx, candidate.Snapshot())
	cancel()
	if err != nil {
		c.fail(ctx, candidate, FailureStage)
		return fmt.Errorf("topology control: stage: %w", err)
	}
	readyCtx, cancel := context.WithTimeout(ctx, c.applyTimeout)
	previous, err := c.target.Ready(readyCtx, candidate.Epoch())
	cancel()
	if err != nil {
		c.fail(ctx, candidate, FailureReady)
		return fmt.Errorf("topology control: ready: %w", err)
	}
	c.mu.Lock()
	c.status.AppliedRevision = candidate.Revision()
	c.status.Epoch = candidate.Epoch()
	c.status.Hash = candidate.Hash()
	c.status.Degraded = false
	c.status.FailureClass = FailureNone
	c.mu.Unlock()
	c.observe(ctx, Event{
		Kind: EventApplied, Revision: candidate.Revision(), Epoch: candidate.Epoch(),
	})
	drainEpochs := make([]uint64, 0, 1)
	if previous != 0 && previous != candidate.Epoch() {
		drainEpochs = append(drainEpochs, previous)
	}
	if planner, supported := c.target.(DrainPlanner); supported {
		drainCtx, drainCancel := context.WithTimeout(ctx, c.drainTimeout)
		drainEpochs, err = planner.Drainable(drainCtx)
		drainCancel()
		if err != nil {
			c.fail(ctx, candidate, FailureDrain)
			return fmt.Errorf("topology control: plan drain: %w", err)
		}
	}
	for _, drainEpoch := range drainEpochs {
		drainCtx, drainCancel := context.WithTimeout(ctx, c.drainTimeout)
		err = c.target.Drain(drainCtx, drainEpoch)
		drainCancel()
		if err != nil {
			c.fail(ctx, candidate, FailureDrain)
			return fmt.Errorf("topology control: drain epoch %d: %w", drainEpoch, err)
		}
	}
	return nil
}

// Sync loads and applies one current candidate before a process becomes ready.
func (c *Controller) Sync(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidController
	}
	candidate, err := c.source.Load(ctx)
	if err != nil {
		c.sourceFailure(ctx)
		return fmt.Errorf("topology control: load: %w", err)
	}
	return c.Apply(ctx, candidate)
}

// Run loads the current candidate and reconnects watchers with bounded backoff.
func (c *Controller) Run(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidController
	}
	backoff := c.minBackoff
	for {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		candidate, err := c.source.Load(ctx)
		if err != nil {
			c.sourceFailure(ctx)
			if sleepErr := c.sleep(ctx, backoff); sleepErr != nil {
				return sleepErr
			}
			backoff = nextBackoff(backoff, c.maxBackoff)
			continue
		}
		_ = c.Apply(ctx, candidate)
		watcher, err := c.source.Watch(ctx)
		if err != nil {
			c.sourceFailure(ctx)
			if sleepErr := c.sleep(ctx, backoff); sleepErr != nil {
				return sleepErr
			}
			backoff = nextBackoff(backoff, c.maxBackoff)
			continue
		}
		candidate, err = c.source.Load(ctx)
		if err != nil {
			_ = watcher.Close()
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			c.sourceFailure(ctx)
			if sleepErr := c.sleep(ctx, backoff); sleepErr != nil {
				return sleepErr
			}
			backoff = nextBackoff(backoff, c.maxBackoff)
			continue
		}
		_ = c.Apply(ctx, candidate)
		backoff = c.minBackoff
		for {
			candidate, err = watcher.Next(ctx)
			if err != nil {
				_ = watcher.Close()
				if cause := context.Cause(ctx); cause != nil {
					return cause
				}
				c.sourceFailure(ctx)
				break
			}
			_ = c.Apply(ctx, candidate)
		}
		if sleepErr := c.sleep(ctx, backoff); sleepErr != nil {
			return sleepErr
		}
		backoff = nextBackoff(backoff, c.maxBackoff)
	}
}

// Status returns a payload-free immutable last-seen/last-good view.
func (c *Controller) Status() Status {
	if c == nil {
		return Status{Degraded: true, FailureClass: FailureSource}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

func (c *Controller) reject(ctx context.Context, candidate Candidate, class FailureClass) {
	c.mu.Lock()
	c.status.Degraded = true
	c.status.FailureClass = class
	c.mu.Unlock()
	c.observe(ctx, Event{
		Kind: EventRejected, Revision: candidate.Revision(), Epoch: candidate.Epoch(),
		FailureClass: class,
	})
}

func (c *Controller) fail(ctx context.Context, candidate Candidate, class FailureClass) {
	c.reject(ctx, candidate, class)
}

func (c *Controller) sourceFailure(ctx context.Context) {
	c.mu.Lock()
	c.status.Degraded = true
	c.status.FailureClass = FailureSource
	c.status.Reconnects++
	c.mu.Unlock()
	c.observe(ctx, Event{Kind: EventReconnect, FailureClass: FailureSource})
}

func (c *Controller) observe(ctx context.Context, event Event) {
	if isNilControlValue(c.observer) {
		return
	}
	defer func() { _ = recover() }()
	c.observer.Observe(ctx, event)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current time.Duration, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}
