package topology

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	// ErrInvalidEpoch reports a zero, duplicate, or non-monotonic epoch.
	ErrInvalidEpoch = errors.New("topology: invalid epoch")
	// ErrEpochNotFound reports an epoch absent from the Manager.
	ErrEpochNotFound = errors.New("topology: epoch not found")
	// ErrInvalidEpochTransition reports a transition from the wrong state.
	ErrInvalidEpochTransition = errors.New("topology: invalid epoch transition")
	// ErrSuccessorNotReady prevents draining the active epoch too early.
	ErrSuccessorNotReady = errors.New("topology: successor not ready")
	// ErrNoReadyEpoch reports Acquire before any epoch becomes Ready.
	ErrNoReadyEpoch = errors.New("topology: no ready epoch")
	// ErrEpochHasTraffic prevents draining an epoch still selected by new calls.
	ErrEpochHasTraffic = errors.New("topology: epoch still has traffic")
)

// EpochState is the lifecycle of one immutable process plan epoch.
type EpochState string

const (
	// EpochStaging is constructed but not eligible for calls.
	EpochStaging EpochState = "staging"
	// EpochReady is eligible for new call leases.
	EpochReady EpochState = "ready"
	// EpochDraining rejects new leases and waits for existing leases.
	EpochDraining EpochState = "draining"
	// EpochStopped has released every call lease.
	EpochStopped EpochState = "stopped"
)

type managedEpoch struct {
	snapshot Snapshot
	state    EpochState
	leases   uint64
	idle     chan struct{}
}

// Manager coordinates immutable epoch readiness, draining, and call leases.
//
// Manager deliberately does not own component Runtime instances and cannot
// replace the frozen Snapshot of an existing process runtime.
type Manager struct {
	mu      sync.Mutex
	epochs  map[uint64]*managedEpoch
	active  uint64
	maximum uint64
	traffic *TrafficSelector
}

// NewManager creates an empty epoch manager.
func NewManager() *Manager {
	return &Manager{epochs: make(map[uint64]*managedEpoch)}
}

// Stage adds one activated, strictly newer epoch.
func (manager *Manager) Stage(snapshot Snapshot) error {
	if manager == nil {
		return fmt.Errorf("%w: manager is nil", ErrInvalidEpoch)
	}
	epoch := snapshot.Epoch()
	if epoch == 0 || snapshot.Hash() == "" {
		return fmt.Errorf("%w: snapshot is not activated", ErrInvalidEpoch)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if epoch <= manager.maximum {
		return fmt.Errorf(
			"%w: epoch %d is not newer than %d",
			ErrInvalidEpoch,
			epoch,
			manager.maximum,
		)
	}
	idle := make(chan struct{})
	close(idle)
	manager.epochs[epoch] = &managedEpoch{
		snapshot: snapshot,
		state:    EpochStaging,
		idle:     idle,
	}
	manager.maximum = epoch
	return nil
}

// Ready promotes a staging epoch to receive new call leases.
func (manager *Manager) Ready(epoch uint64) error {
	if manager == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state != EpochStaging || epoch <= manager.active {
		return fmt.Errorf(
			"%w: epoch %d cannot become ready from %q",
			ErrInvalidEpochTransition,
			epoch,
			entry.state,
		)
	}
	selector, err := NewTrafficSelector(entry.snapshot.Traffic())
	if err != nil {
		return err
	}
	for _, weight := range selector.Weights() {
		if weight.BasisPoints == 0 || weight.Epoch == epoch {
			continue
		}
		referenced, available := manager.epochs[weight.Epoch]
		if !available || referenced.state != EpochReady {
			return fmt.Errorf(
				"%w: traffic epoch %d is not ready",
				ErrInvalidEpochTransition,
				weight.Epoch,
			)
		}
	}
	entry.state = EpochReady
	manager.active = epoch
	manager.traffic = selector
	return nil
}

// Drain moves an old Ready epoch to Draining only after a newer epoch is Ready.
func (manager *Manager) Drain(epoch uint64) error {
	if manager == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state != EpochReady {
		return fmt.Errorf(
			"%w: epoch %d cannot drain from %q",
			ErrInvalidEpochTransition,
			epoch,
			entry.state,
		)
	}
	successor, successorExists := manager.epochs[manager.active]
	if !successorExists ||
		manager.active <= epoch ||
		successor.state != EpochReady {
		return fmt.Errorf(
			"%w: epoch %d has no newer ready epoch",
			ErrSuccessorNotReady,
			epoch,
		)
	}
	if manager.traffic != nil && manager.traffic.BasisPoints(epoch) != 0 {
		return fmt.Errorf(
			"%w: epoch %d has %d basis points",
			ErrEpochHasTraffic,
			epoch,
			manager.traffic.BasisPoints(epoch),
		)
	}
	entry.state = EpochDraining
	return nil
}

// Acquire leases the current Ready epoch for one logical call.
func (manager *Manager) Acquire() (*Lease, error) {
	return manager.acquire("")
}

// AcquireKey leases the Ready epoch selected by stable weighted rendezvous.
func (manager *Manager) AcquireKey(routingKey string) (*Lease, error) {
	return manager.acquire(routingKey)
}

func (manager *Manager) acquire(routingKey string) (*Lease, error) {
	if manager == nil {
		return nil, fmt.Errorf("%w: manager is nil", ErrNoReadyEpoch)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.traffic == nil {
		return nil, ErrNoReadyEpoch
	}
	epoch, err := manager.traffic.Select(routingKey)
	if err != nil {
		return nil, errors.Join(ErrNoReadyEpoch, err)
	}
	entry, exists := manager.epochs[epoch]
	if !exists || entry.state != EpochReady {
		return nil, ErrNoReadyEpoch
	}
	if entry.leases == 0 {
		entry.idle = make(chan struct{})
	}
	entry.leases++
	return &Lease{
		manager:  manager,
		epoch:    epoch,
		snapshot: entry.snapshot,
	}, nil
}

// Drainable returns every non-active Ready epoch with zero current weight.
func (manager *Manager) Drainable() []uint64 {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	epochs := make([]uint64, 0)
	for epoch, entry := range manager.epochs {
		if epoch == manager.active || entry.state != EpochReady ||
			manager.traffic != nil && manager.traffic.BasisPoints(epoch) != 0 {
			continue
		}
		epochs = append(epochs, epoch)
	}
	slices.Sort(epochs)
	return epochs
}

// DrainForShutdown removes any Ready epoch after its owning runtime has
// stopped accepting calls. Unlike Drain, it is not a rollout operation.
func (manager *Manager) DrainForShutdown(epoch uint64) error {
	if manager == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state != EpochReady {
		return fmt.Errorf("%w: epoch %d cannot drain for shutdown from %q", ErrInvalidEpochTransition, epoch, entry.state)
	}
	entry.state = EpochDraining
	if manager.active == epoch {
		manager.active = 0
		manager.traffic = nil
	}
	return nil
}

// Stop waits for a Draining epoch's leases and marks it Stopped.
func (manager *Manager) Stop(ctx context.Context, epoch uint64) error {
	if manager == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidEpochTransition)
	}
	manager.mu.Lock()
	entry, exists := manager.epochs[epoch]
	if !exists {
		manager.mu.Unlock()
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	switch entry.state {
	case EpochStopped:
		manager.mu.Unlock()
		return nil
	case EpochDraining:
	default:
		state := entry.state
		manager.mu.Unlock()
		return fmt.Errorf("%w: epoch %d cannot stop from %q", ErrInvalidEpochTransition, epoch, state)
	}
	idle := entry.idle
	manager.mu.Unlock()

	select {
	case <-idle:
	case <-ctx.Done():
		return context.Cause(ctx)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry = manager.epochs[epoch]
	if entry.state == EpochStopped {
		return nil
	}
	if entry.state != EpochDraining || entry.leases != 0 {
		return fmt.Errorf("%w: epoch %d changed while stopping", ErrInvalidEpochTransition, epoch)
	}
	entry.state = EpochStopped
	return nil
}

// State returns the current lifecycle state for one epoch.
func (manager *Manager) State(epoch uint64) (EpochState, bool) {
	if manager == nil {
		return "", false
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[epoch]
	if !exists {
		return "", false
	}
	return entry.state, true
}

// Lease pins one call to an immutable epoch Snapshot until Release.
type Lease struct {
	manager  *Manager
	epoch    uint64
	snapshot Snapshot
	once     sync.Once
}

// Epoch returns the fixed call epoch.
func (lease *Lease) Epoch() uint64 {
	if lease == nil {
		return 0
	}
	return lease.epoch
}

// Snapshot returns the fixed call Snapshot.
func (lease *Lease) Snapshot() Snapshot {
	if lease == nil {
		return Snapshot{}
	}
	return lease.snapshot
}

// Release idempotently releases this call's epoch lease.
func (lease *Lease) Release() {
	if lease == nil {
		return
	}
	lease.once.Do(func() {
		lease.manager.release(lease.epoch)
	})
}

func (manager *Manager) release(epoch uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[epoch]
	if !exists || entry.leases == 0 {
		return
	}
	entry.leases--
	if entry.leases == 0 {
		close(entry.idle)
	}
}
