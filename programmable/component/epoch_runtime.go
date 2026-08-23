package component

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/keelab/keelith/programmable/topology"
)

const defaultEpochRuntimeName = "keelith.component.epochs"

var (
	// ErrInvalidEpochRuntime reports an incomplete builder, name, snapshot, or
	// returned Runtime.
	ErrInvalidEpochRuntime = errors.New("component: invalid epoch runtime")
	// ErrEpochRuntimeClosed reports staging, promotion, or acquisition after
	// shutdown has begun.
	ErrEpochRuntimeClosed = errors.New("component: epoch runtime closed")
	// ErrEpochNotStaged reports promotion or draining of an unknown epoch.
	ErrEpochNotStaged = errors.New("component: epoch not staged")
)

// RuntimeBuilder creates one mutable Runtime registration set for snapshot.
//
// EpochRuntime calls Runtime.Activate after Build returns. A builder must not
// freeze or activate the returned Runtime itself.
type RuntimeBuilder func(
	context.Context,
	topology.Snapshot,
) (*Runtime, error)

// EpochRuntimeConfig configures an epoch manager and optional App component.
type EpochRuntimeConfig struct {
	Name         string
	Dependencies []string
	Initial      topology.Snapshot
	Build        RuntimeBuilder
	Observer     EpochObserver
}

type epochEntry struct {
	snapshot  topology.Snapshot
	runtime   *Runtime
	closeOnce sync.Once
	closeErr  error
}

// EpochRuntime owns one frozen component Runtime per topology epoch.
//
// Stage constructs without changing traffic. Ready atomically changes the
// epoch used by new Acquire calls. Drain waits for pinned leases before
// releasing factory-owned providers.
type EpochRuntime struct {
	manager      *topology.Manager
	build        RuntimeBuilder
	initial      topology.Snapshot
	name         string
	dependencies []string
	observer     EpochObserver

	transitions sync.Mutex
	mu          sync.RWMutex
	entries     map[uint64]*epochEntry
	active      uint64
	closed      bool
}

// NewEpochRuntime validates and constructs an empty epoch runtime.
func NewEpochRuntime(config EpochRuntimeConfig) (*EpochRuntime, error) {
	if config.Build == nil || isNilEpochObserver(config.Observer) &&
		config.Observer != nil {
		return nil, ErrInvalidEpochRuntime
	}
	name := strings.TrimSpace(config.Name)
	if name == "" {
		name = defaultEpochRuntimeName
	}
	if name != config.Name && config.Name != "" {
		return nil, ErrInvalidEpochRuntime
	}
	if config.Initial.Epoch() != 0 || config.Initial.Hash() != "" {
		if _, err := config.Initial.Bindings(); err != nil {
			return nil, fmt.Errorf(
				"%w: initial snapshot: %w",
				ErrInvalidEpochRuntime,
				err,
			)
		}
	}
	dependencies := append([]string(nil), config.Dependencies...)
	for _, dependency := range dependencies {
		if strings.TrimSpace(dependency) != dependency || dependency == "" {
			return nil, ErrInvalidEpochRuntime
		}
	}
	return &EpochRuntime{
		manager:      topology.NewManager(),
		build:        config.Build,
		initial:      config.Initial,
		name:         name,
		dependencies: dependencies,
		observer:     config.Observer,
		entries:      make(map[uint64]*epochEntry),
	}, nil
}

// Name implements the App Component contract.
func (er *EpochRuntime) Name() string {
	if er == nil {
		return ""
	}
	return er.name
}

// Dependencies implements the App DependencyProvider contract.
func (er *EpochRuntime) Dependencies() []string {
	if er == nil {
		return nil
	}
	return append([]string(nil), er.dependencies...)
}

// Start activates the configured initial snapshot for App startup.
func (er *EpochRuntime) Start(ctx context.Context) error {
	if er == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	if _, exists := er.Active(); exists {
		return nil
	}
	if er.initial.Epoch() == 0 || er.initial.Hash() == "" {
		return fmt.Errorf(
			"%w: initial snapshot is required by Start",
			ErrInvalidEpochRuntime,
		)
	}
	return er.Activate(ctx, er.initial)
}

// Stage constructs and activates one immutable Runtime without changing the
// active traffic epoch.
func (er *EpochRuntime) Stage(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if er == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	er.transitions.Lock()
	defer er.transitions.Unlock()
	return er.stageLocked(ctx, snapshot)
}

func (er *EpochRuntime) stageLocked(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if _, err := snapshot.Bindings(); err != nil {
		return fmt.Errorf("%w: snapshot: %w", ErrInvalidEpochRuntime, err)
	}
	er.mu.RLock()
	closed := er.closed
	_, duplicate := er.entries[snapshot.Epoch()]
	er.mu.RUnlock()
	if closed {
		return ErrEpochRuntimeClosed
	}
	if duplicate {
		return fmt.Errorf(
			"%w: epoch %d",
			topology.ErrInvalidEpoch,
			snapshot.Epoch(),
		)
	}

	candidate, buildErr := er.build(ctx, snapshot)
	if buildErr != nil || candidate == nil {
		cause := buildErr
		if cause == nil {
			cause = ErrInvalidEpochRuntime
		}
		closeErr := closeCandidate(ctx, candidate)
		observeEpoch(ctx, er.observer, EpochEvent{
			Kind:   EpochEventStage,
			Epoch:  snapshot.Epoch(),
			State:  topology.EpochStaging,
			Failed: true,
		})
		return errors.Join(
			fmt.Errorf("%w: build epoch %d: %w",
				ErrInvalidEpochRuntime,
				snapshot.Epoch(),
				cause,
			),
			closeErr,
		)
	}
	if err := candidate.Activate(ctx, snapshot); err != nil {
		closeErr := closeCandidate(ctx, candidate)
		observeEpoch(ctx, er.observer, EpochEvent{
			Kind:   EpochEventStage,
			Epoch:  snapshot.Epoch(),
			State:  topology.EpochStaging,
			Failed: true,
		})
		return errors.Join(err, closeErr)
	}
	if err := er.manager.Stage(snapshot); err != nil {
		closeErr := closeCandidate(ctx, candidate)
		observeEpoch(ctx, er.observer, EpochEvent{
			Kind:   EpochEventStage,
			Epoch:  snapshot.Epoch(),
			State:  topology.EpochStaging,
			Failed: true,
		})
		return errors.Join(err, closeErr)
	}
	er.mu.Lock()
	er.entries[snapshot.Epoch()] = &epochEntry{
		snapshot: snapshot,
		runtime:  candidate,
	}
	er.mu.Unlock()
	observeEpoch(ctx, er.observer, EpochEvent{
		Kind:  EpochEventStage,
		Epoch: snapshot.Epoch(),
		State: topology.EpochStaging,
	})
	return nil
}

// Ready promotes one staged epoch and returns the previously active epoch.
func (er *EpochRuntime) Ready(epoch uint64) (uint64, error) {
	return er.ReadyContext(context.Background(), epoch)
}

// ReadyContext promotes one staged epoch while honoring caller cancellation.
func (er *EpochRuntime) ReadyContext(
	ctx context.Context,
	epoch uint64,
) (uint64, error) {
	if er == nil {
		return 0, ErrInvalidEpochRuntime
	}
	if ctx == nil {
		return 0, ErrInvalidEpochRuntime
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	er.transitions.Lock()
	defer er.transitions.Unlock()
	return er.readyLocked(ctx, epoch)
}

func (er *EpochRuntime) readyLocked(
	ctx context.Context,
	epoch uint64,
) (uint64, error) {
	er.mu.RLock()
	closed := er.closed
	_, exists := er.entries[epoch]
	previous := er.active
	er.mu.RUnlock()
	if closed {
		return 0, ErrEpochRuntimeClosed
	}
	if !exists {
		return 0, fmt.Errorf("%w: epoch %d", ErrEpochNotStaged, epoch)
	}
	if err := er.manager.Ready(epoch); err != nil {
		observeEpoch(ctx, er.observer, EpochEvent{
			Kind:   EpochEventReady,
			Epoch:  epoch,
			State:  topology.EpochStaging,
			Failed: true,
		})
		return 0, err
	}
	er.mu.Lock()
	er.active = epoch
	er.mu.Unlock()
	observeEpoch(ctx, er.observer, EpochEvent{
		Kind:  EpochEventReady,
		Epoch: epoch,
		State: topology.EpochReady,
	})
	return previous, nil
}

// Activate constructs, stages, and promotes one snapshot atomically with
// respect to other epoch lifecycle changes.
func (er *EpochRuntime) Activate(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if er == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	er.transitions.Lock()
	defer er.transitions.Unlock()
	if err := er.stageLocked(ctx, snapshot); err != nil {
		return err
	}
	if _, err := er.readyLocked(ctx, snapshot.Epoch()); err != nil {
		return errors.Join(err, er.discardStaged(ctx, snapshot.Epoch()))
	}
	return nil
}

// Drain prevents new leases for an old Ready epoch, waits for every pinned
// lease, and closes its factory-owned providers.
func (er *EpochRuntime) Drain(
	ctx context.Context,
	epoch uint64,
) error {
	if er == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	er.transitions.Lock()
	entry := er.entry(epoch)
	if entry == nil {
		er.transitions.Unlock()
		return fmt.Errorf("%w: epoch %d", ErrEpochNotStaged, epoch)
	}
	state, exists := er.manager.State(epoch)
	if !exists {
		er.transitions.Unlock()
		return fmt.Errorf("%w: epoch %d", ErrEpochNotStaged, epoch)
	}
	switch state {
	case topology.EpochReady:
		if err := er.manager.Drain(epoch); err != nil {
			er.transitions.Unlock()
			return err
		}
		observeEpoch(ctx, er.observer, EpochEvent{
			Kind:  EpochEventDrain,
			Epoch: epoch,
			State: topology.EpochDraining,
		})
	case topology.EpochDraining, topology.EpochStopped:
	case topology.EpochStaging:
		er.transitions.Unlock()
		return fmt.Errorf(
			"%w: epoch %d is staging",
			topology.ErrInvalidEpochTransition,
			epoch,
		)
	default:
		er.transitions.Unlock()
		return topology.ErrInvalidEpochTransition
	}
	er.transitions.Unlock()

	if state != topology.EpochStopped {
		if err := er.manager.Stop(ctx, epoch); err != nil {
			return err
		}
	}
	return er.closeEntry(ctx, epoch, entry)
}

// Acquire pins the current Ready Runtime for one logical call.
func (er *EpochRuntime) Acquire(
	ctx context.Context,
) (*EpochLease, error) {
	return er.acquire(ctx, "")
}

// AcquireKey pins the Runtime chosen for a stable weighted routing key.
func (er *EpochRuntime) AcquireKey(
	ctx context.Context,
	routingKey string,
) (*EpochLease, error) {
	return er.acquire(ctx, routingKey)
}

func (er *EpochRuntime) acquire(
	ctx context.Context,
	routingKey string,
) (*EpochLease, error) {
	if er == nil || ctx == nil {
		return nil, ErrInvalidEpochRuntime
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	er.mu.RLock()
	closed := er.closed
	er.mu.RUnlock()
	if closed {
		return nil, ErrEpochRuntimeClosed
	}
	var lease *topology.Lease
	var err error
	if routingKey == "" {
		lease, err = er.manager.Acquire()
	} else {
		lease, err = er.manager.AcquireKey(routingKey)
	}
	if err != nil {
		return nil, err
	}
	entry := er.entry(lease.Epoch())
	if entry == nil {
		lease.Release()
		return nil, ErrEpochNotStaged
	}
	observeEpoch(ctx, er.observer, EpochEvent{
		Kind:  EpochEventAcquire,
		Epoch: lease.Epoch(),
		State: topology.EpochReady,
	})
	return &EpochLease{
		lease:    lease,
		runtime:  entry.runtime,
		observer: er.observer,
	}, nil
}

// Drainable returns zero-weight Ready epochs eligible for rollout cleanup.
func (er *EpochRuntime) Drainable() []uint64 {
	if er == nil {
		return nil
	}
	return er.manager.Drainable()
}

// Active returns the epoch currently used by new Acquire calls.
func (er *EpochRuntime) Active() (uint64, bool) {
	if er == nil {
		return 0, false
	}
	er.mu.RLock()
	defer er.mu.RUnlock()
	return er.active, er.active != 0
}

// State returns the topology lifecycle state for one epoch.
func (er *EpochRuntime) State(
	epoch uint64,
) (topology.EpochState, bool) {
	if er == nil {
		return "", false
	}
	return er.manager.State(epoch)
}

// Stop implements the App Component contract and is safely retryable after a
// context timeout. It rejects new work, drains every Ready epoch, discards
// staging epochs, and closes stopped runtimes newest-first.
func (er *EpochRuntime) Stop(ctx context.Context) error {
	if er == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	er.transitions.Lock()
	er.mu.Lock()
	er.closed = true
	active := er.active
	er.active = 0
	epochs := make([]uint64, 0, len(er.entries))
	for epoch := range er.entries {
		epochs = append(epochs, epoch)
	}
	er.mu.Unlock()
	sort.Slice(epochs, func(first, second int) bool {
		return epochs[first] < epochs[second]
	})

	failures := make([]error, 0)
	for _, epoch := range epochs {
		if epoch == active {
			continue
		}
		state, exists := er.manager.State(epoch)
		if !exists {
			continue
		}
		switch state {
		case topology.EpochStaging:
			if err := er.manager.Discard(epoch); err != nil {
				failures = append(failures, err)
			}
		case topology.EpochReady:
			if err := er.manager.DrainForShutdown(epoch); err != nil {
				failures = append(failures, err)
			} else {
				observeEpoch(ctx, er.observer, EpochEvent{
					Kind:  EpochEventDrain,
					Epoch: epoch,
					State: topology.EpochDraining,
				})
			}
		}
	}
	if active != 0 {
		state, exists := er.manager.State(active)
		if exists && state == topology.EpochReady {
			if err := er.manager.DrainActive(active); err != nil {
				failures = append(failures, err)
			} else {
				observeEpoch(ctx, er.observer, EpochEvent{
					Kind:  EpochEventDrain,
					Epoch: active,
					State: topology.EpochDraining,
				})
			}
		}
	}
	er.transitions.Unlock()

	sort.Slice(epochs, func(first, second int) bool {
		return epochs[first] > epochs[second]
	})
	for _, epoch := range epochs {
		entry := er.entry(epoch)
		if entry == nil {
			continue
		}
		state, exists := er.manager.State(epoch)
		if !exists {
			continue
		}
		if state == topology.EpochDraining {
			if err := er.manager.Stop(ctx, epoch); err != nil {
				failures = append(failures, err)
				continue
			}
			state = topology.EpochStopped
		}
		if state == topology.EpochStopped {
			if err := er.closeEntry(ctx, epoch, entry); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func (er *EpochRuntime) discardStaged(
	ctx context.Context,
	epoch uint64,
) error {
	entry := er.entry(epoch)
	if entry == nil {
		return nil
	}
	discardErr := er.manager.Discard(epoch)
	closeErr := er.closeEntry(ctx, epoch, entry)
	return errors.Join(discardErr, closeErr)
}

func (er *EpochRuntime) entry(epoch uint64) *epochEntry {
	er.mu.RLock()
	defer er.mu.RUnlock()
	return er.entries[epoch]
}

func (er *EpochRuntime) closeEntry(
	ctx context.Context,
	epoch uint64,
	entry *epochEntry,
) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	entry.closeOnce.Do(func() {
		entry.closeErr = entry.runtime.Close(ctx)
		observeEpoch(ctx, er.observer, EpochEvent{
			Kind:   EpochEventClose,
			Epoch:  epoch,
			State:  topology.EpochStopped,
			Failed: entry.closeErr != nil,
		})
	})
	return entry.closeErr
}

func closeCandidate(ctx context.Context, candidate *Runtime) error {
	if candidate == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		defaultFactoryRollbackTimeout,
	)
	defer cancel()
	return candidate.Close(cleanupCtx)
}

// EpochLease pins one Runtime and Snapshot until Release.
type EpochLease struct {
	lease    *topology.Lease
	runtime  *Runtime
	observer EpochObserver
	once     sync.Once
}

// Epoch returns the fixed topology epoch.
func (lease *EpochLease) Epoch() uint64 {
	if lease == nil || lease.lease == nil {
		return 0
	}
	return lease.lease.Epoch()
}

// Snapshot returns the fixed topology snapshot.
func (lease *EpochLease) Snapshot() topology.Snapshot {
	if lease == nil || lease.lease == nil {
		return topology.Snapshot{}
	}
	return lease.lease.Snapshot()
}

// Runtime returns the fixed component Runtime.
func (lease *EpochLease) Runtime() *Runtime {
	if lease == nil {
		return nil
	}
	return lease.runtime
}

// Release idempotently releases the pinned epoch.
func (lease *EpochLease) Release() {
	if lease == nil || lease.lease == nil {
		return
	}
	lease.once.Do(func() {
		epoch := lease.lease.Epoch()
		lease.lease.Release()
		observeEpoch(context.Background(), lease.observer, EpochEvent{
			Kind:  EpochEventRelease,
			Epoch: epoch,
			State: topology.EpochReady,
		})
	})
}
