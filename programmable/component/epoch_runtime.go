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
func (runtime *EpochRuntime) Name() string {
	if runtime == nil {
		return ""
	}
	return runtime.name
}

// Dependencies implements the App DependencyProvider contract.
func (runtime *EpochRuntime) Dependencies() []string {
	if runtime == nil {
		return nil
	}
	return append([]string(nil), runtime.dependencies...)
}

// Start activates the configured initial snapshot for App startup.
func (runtime *EpochRuntime) Start(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	if _, exists := runtime.Active(); exists {
		return nil
	}
	if runtime.initial.Epoch() == 0 || runtime.initial.Hash() == "" {
		return fmt.Errorf(
			"%w: initial snapshot is required by Start",
			ErrInvalidEpochRuntime,
		)
	}
	return runtime.Activate(ctx, runtime.initial)
}

// Stage constructs and activates one immutable Runtime without changing the
// active traffic epoch.
func (runtime *EpochRuntime) Stage(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	runtime.transitions.Lock()
	defer runtime.transitions.Unlock()
	return runtime.stageLocked(ctx, snapshot)
}

func (runtime *EpochRuntime) stageLocked(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if _, err := snapshot.Bindings(); err != nil {
		return fmt.Errorf("%w: snapshot: %w", ErrInvalidEpochRuntime, err)
	}
	runtime.mu.RLock()
	closed := runtime.closed
	_, duplicate := runtime.entries[snapshot.Epoch()]
	runtime.mu.RUnlock()
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

	candidate, buildErr := runtime.build(ctx, snapshot)
	if buildErr != nil || candidate == nil {
		cause := buildErr
		if cause == nil {
			cause = ErrInvalidEpochRuntime
		}
		closeErr := closeCandidate(ctx, candidate)
		observeEpoch(ctx, runtime.observer, EpochEvent{
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
		observeEpoch(ctx, runtime.observer, EpochEvent{
			Kind:   EpochEventStage,
			Epoch:  snapshot.Epoch(),
			State:  topology.EpochStaging,
			Failed: true,
		})
		return errors.Join(err, closeErr)
	}
	if err := runtime.manager.Stage(snapshot); err != nil {
		closeErr := closeCandidate(ctx, candidate)
		observeEpoch(ctx, runtime.observer, EpochEvent{
			Kind:   EpochEventStage,
			Epoch:  snapshot.Epoch(),
			State:  topology.EpochStaging,
			Failed: true,
		})
		return errors.Join(err, closeErr)
	}
	runtime.mu.Lock()
	runtime.entries[snapshot.Epoch()] = &epochEntry{
		snapshot: snapshot,
		runtime:  candidate,
	}
	runtime.mu.Unlock()
	observeEpoch(ctx, runtime.observer, EpochEvent{
		Kind:  EpochEventStage,
		Epoch: snapshot.Epoch(),
		State: topology.EpochStaging,
	})
	return nil
}

// Ready promotes one staged epoch and returns the previously active epoch.
func (runtime *EpochRuntime) Ready(epoch uint64) (uint64, error) {
	return runtime.ReadyContext(context.Background(), epoch)
}

// ReadyContext promotes one staged epoch while honoring caller cancellation.
func (runtime *EpochRuntime) ReadyContext(
	ctx context.Context,
	epoch uint64,
) (uint64, error) {
	if runtime == nil {
		return 0, ErrInvalidEpochRuntime
	}
	if ctx == nil {
		return 0, ErrInvalidEpochRuntime
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	runtime.transitions.Lock()
	defer runtime.transitions.Unlock()
	return runtime.readyLocked(ctx, epoch)
}

func (runtime *EpochRuntime) readyLocked(
	ctx context.Context,
	epoch uint64,
) (uint64, error) {
	runtime.mu.RLock()
	closed := runtime.closed
	_, exists := runtime.entries[epoch]
	previous := runtime.active
	runtime.mu.RUnlock()
	if closed {
		return 0, ErrEpochRuntimeClosed
	}
	if !exists {
		return 0, fmt.Errorf("%w: epoch %d", ErrEpochNotStaged, epoch)
	}
	if err := runtime.manager.Ready(epoch); err != nil {
		observeEpoch(ctx, runtime.observer, EpochEvent{
			Kind:   EpochEventReady,
			Epoch:  epoch,
			State:  topology.EpochStaging,
			Failed: true,
		})
		return 0, err
	}
	runtime.mu.Lock()
	runtime.active = epoch
	runtime.mu.Unlock()
	observeEpoch(ctx, runtime.observer, EpochEvent{
		Kind:  EpochEventReady,
		Epoch: epoch,
		State: topology.EpochReady,
	})
	return previous, nil
}

// Activate constructs, stages, and promotes one snapshot atomically with
// respect to other epoch lifecycle changes.
func (runtime *EpochRuntime) Activate(
	ctx context.Context,
	snapshot topology.Snapshot,
) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	runtime.transitions.Lock()
	defer runtime.transitions.Unlock()
	if err := runtime.stageLocked(ctx, snapshot); err != nil {
		return err
	}
	if _, err := runtime.readyLocked(ctx, snapshot.Epoch()); err != nil {
		return errors.Join(err, runtime.discardStaged(ctx, snapshot.Epoch()))
	}
	return nil
}

// Drain prevents new leases for an old Ready epoch, waits for every pinned
// lease, and closes its factory-owned providers.
func (runtime *EpochRuntime) Drain(
	ctx context.Context,
	epoch uint64,
) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	runtime.transitions.Lock()
	entry := runtime.entry(epoch)
	if entry == nil {
		runtime.transitions.Unlock()
		return fmt.Errorf("%w: epoch %d", ErrEpochNotStaged, epoch)
	}
	state, exists := runtime.manager.State(epoch)
	if !exists {
		runtime.transitions.Unlock()
		return fmt.Errorf("%w: epoch %d", ErrEpochNotStaged, epoch)
	}
	switch state {
	case topology.EpochReady:
		if err := runtime.manager.Drain(epoch); err != nil {
			runtime.transitions.Unlock()
			return err
		}
		observeEpoch(ctx, runtime.observer, EpochEvent{
			Kind:  EpochEventDrain,
			Epoch: epoch,
			State: topology.EpochDraining,
		})
	case topology.EpochDraining, topology.EpochStopped:
	case topology.EpochStaging:
		runtime.transitions.Unlock()
		return fmt.Errorf(
			"%w: epoch %d is staging",
			topology.ErrInvalidEpochTransition,
			epoch,
		)
	default:
		runtime.transitions.Unlock()
		return topology.ErrInvalidEpochTransition
	}
	runtime.transitions.Unlock()

	if state != topology.EpochStopped {
		if err := runtime.manager.Stop(ctx, epoch); err != nil {
			return err
		}
	}
	return runtime.closeEntry(ctx, epoch, entry)
}

// Acquire pins the current Ready Runtime for one logical call.
func (runtime *EpochRuntime) Acquire(
	ctx context.Context,
) (*EpochLease, error) {
	return runtime.acquire(ctx, "")
}

// AcquireKey pins the Runtime chosen for a stable weighted routing key.
func (runtime *EpochRuntime) AcquireKey(
	ctx context.Context,
	routingKey string,
) (*EpochLease, error) {
	return runtime.acquire(ctx, routingKey)
}

func (runtime *EpochRuntime) acquire(
	ctx context.Context,
	routingKey string,
) (*EpochLease, error) {
	if runtime == nil || ctx == nil {
		return nil, ErrInvalidEpochRuntime
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	runtime.mu.RLock()
	closed := runtime.closed
	runtime.mu.RUnlock()
	if closed {
		return nil, ErrEpochRuntimeClosed
	}
	var lease *topology.Lease
	var err error
	if routingKey == "" {
		lease, err = runtime.manager.Acquire()
	} else {
		lease, err = runtime.manager.AcquireKey(routingKey)
	}
	if err != nil {
		return nil, err
	}
	entry := runtime.entry(lease.Epoch())
	if entry == nil {
		lease.Release()
		return nil, ErrEpochNotStaged
	}
	observeEpoch(ctx, runtime.observer, EpochEvent{
		Kind:  EpochEventAcquire,
		Epoch: lease.Epoch(),
		State: topology.EpochReady,
	})
	return &EpochLease{
		lease:    lease,
		runtime:  entry.runtime,
		observer: runtime.observer,
	}, nil
}

// Drainable returns zero-weight Ready epochs eligible for rollout cleanup.
func (runtime *EpochRuntime) Drainable() []uint64 {
	if runtime == nil {
		return nil
	}
	return runtime.manager.Drainable()
}

// Active returns the epoch currently used by new Acquire calls.
func (runtime *EpochRuntime) Active() (uint64, bool) {
	if runtime == nil {
		return 0, false
	}
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	return runtime.active, runtime.active != 0
}

// State returns the topology lifecycle state for one epoch.
func (runtime *EpochRuntime) State(
	epoch uint64,
) (topology.EpochState, bool) {
	if runtime == nil {
		return "", false
	}
	return runtime.manager.State(epoch)
}

// Stop implements the App Component contract and is safely retryable after a
// context timeout. It rejects new work, drains every Ready epoch, discards
// staging epochs, and closes stopped runtimes newest-first.
func (runtime *EpochRuntime) Stop(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidEpochRuntime
	}
	runtime.transitions.Lock()
	runtime.mu.Lock()
	runtime.closed = true
	active := runtime.active
	runtime.active = 0
	epochs := make([]uint64, 0, len(runtime.entries))
	for epoch := range runtime.entries {
		epochs = append(epochs, epoch)
	}
	runtime.mu.Unlock()
	sort.Slice(epochs, func(first, second int) bool {
		return epochs[first] < epochs[second]
	})

	failures := make([]error, 0)
	for _, epoch := range epochs {
		if epoch == active {
			continue
		}
		state, exists := runtime.manager.State(epoch)
		if !exists {
			continue
		}
		switch state {
		case topology.EpochStaging:
			if err := runtime.manager.Discard(epoch); err != nil {
				failures = append(failures, err)
			}
		case topology.EpochReady:
			if err := runtime.manager.DrainForShutdown(epoch); err != nil {
				failures = append(failures, err)
			} else {
				observeEpoch(ctx, runtime.observer, EpochEvent{
					Kind:  EpochEventDrain,
					Epoch: epoch,
					State: topology.EpochDraining,
				})
			}
		}
	}
	if active != 0 {
		state, exists := runtime.manager.State(active)
		if exists && state == topology.EpochReady {
			if err := runtime.manager.DrainActive(active); err != nil {
				failures = append(failures, err)
			} else {
				observeEpoch(ctx, runtime.observer, EpochEvent{
					Kind:  EpochEventDrain,
					Epoch: active,
					State: topology.EpochDraining,
				})
			}
		}
	}
	runtime.transitions.Unlock()

	sort.Slice(epochs, func(first, second int) bool {
		return epochs[first] > epochs[second]
	})
	for _, epoch := range epochs {
		entry := runtime.entry(epoch)
		if entry == nil {
			continue
		}
		state, exists := runtime.manager.State(epoch)
		if !exists {
			continue
		}
		if state == topology.EpochDraining {
			if err := runtime.manager.Stop(ctx, epoch); err != nil {
				failures = append(failures, err)
				continue
			}
			state = topology.EpochStopped
		}
		if state == topology.EpochStopped {
			if err := runtime.closeEntry(ctx, epoch, entry); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func (runtime *EpochRuntime) discardStaged(
	ctx context.Context,
	epoch uint64,
) error {
	entry := runtime.entry(epoch)
	if entry == nil {
		return nil
	}
	discardErr := runtime.manager.Discard(epoch)
	closeErr := runtime.closeEntry(ctx, epoch, entry)
	return errors.Join(discardErr, closeErr)
}

func (runtime *EpochRuntime) entry(epoch uint64) *epochEntry {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	return runtime.entries[epoch]
}

func (runtime *EpochRuntime) closeEntry(
	ctx context.Context,
	epoch uint64,
	entry *epochEntry,
) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	entry.closeOnce.Do(func() {
		entry.closeErr = entry.runtime.Close(ctx)
		observeEpoch(ctx, runtime.observer, EpochEvent{
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
