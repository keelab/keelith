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
func (m *Manager) Stage(snapshot Snapshot) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrInvalidEpoch)
	}
	epoch := snapshot.Epoch()
	if epoch == 0 || snapshot.Hash() == "" {
		return fmt.Errorf("%w: snapshot is not activated", ErrInvalidEpoch)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if epoch <= m.maximum {
		return fmt.Errorf(
			"%w: epoch %d is not newer than %d",
			ErrInvalidEpoch,
			epoch,
			m.maximum,
		)
	}
	idle := make(chan struct{})
	close(idle)
	m.epochs[epoch] = &managedEpoch{
		snapshot: snapshot,
		state:    EpochStaging,
		idle:     idle,
	}
	m.maximum = epoch
	return nil
}

// Ready promotes a staging epoch to receive new call leases.
func (m *Manager) Ready(epoch uint64) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state != EpochStaging || epoch <= m.active {
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
		referenced, available := m.epochs[weight.Epoch]
		if !available || referenced.state != EpochReady {
			return fmt.Errorf(
				"%w: traffic epoch %d is not ready",
				ErrInvalidEpochTransition,
				weight.Epoch,
			)
		}
	}
	entry.state = EpochReady
	m.active = epoch
	m.traffic = selector
	return nil
}

// Drain moves an old Ready epoch to Draining only after a newer epoch is Ready.
func (m *Manager) Drain(epoch uint64) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[epoch]
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
	successor, successorExists := m.epochs[m.active]
	if !successorExists ||
		m.active <= epoch ||
		successor.state != EpochReady {
		return fmt.Errorf(
			"%w: epoch %d has no newer ready epoch",
			ErrSuccessorNotReady,
			epoch,
		)
	}
	if m.traffic != nil && m.traffic.BasisPoints(epoch) != 0 {
		return fmt.Errorf(
			"%w: epoch %d has %d basis points",
			ErrEpochHasTraffic,
			epoch,
			m.traffic.BasisPoints(epoch),
		)
	}
	entry.state = EpochDraining
	return nil
}

// Acquire leases the current Ready epoch for one logical call.
func (m *Manager) Acquire() (*Lease, error) {
	return m.acquire("")
}

// AcquireKey leases the Ready epoch selected by stable weighted rendezvous.
func (m *Manager) AcquireKey(routingKey string) (*Lease, error) {
	return m.acquire(routingKey)
}

func (m *Manager) acquire(routingKey string) (*Lease, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: manager is nil", ErrNoReadyEpoch)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.traffic == nil {
		return nil, ErrNoReadyEpoch
	}
	epoch, err := m.traffic.Select(routingKey)
	if err != nil {
		return nil, errors.Join(ErrNoReadyEpoch, err)
	}
	entry, exists := m.epochs[epoch]
	if !exists || entry.state != EpochReady {
		return nil, ErrNoReadyEpoch
	}
	if entry.leases == 0 {
		entry.idle = make(chan struct{})
	}
	entry.leases++
	return &Lease{
		manager:  m,
		epoch:    epoch,
		snapshot: entry.snapshot,
	}, nil
}

// Drainable returns every non-active Ready epoch with zero current weight.
func (m *Manager) Drainable() []uint64 {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	epochs := make([]uint64, 0)
	for epoch, entry := range m.epochs {
		if epoch == m.active || entry.state != EpochReady ||
			m.traffic != nil && m.traffic.BasisPoints(epoch) != 0 {
			continue
		}
		epochs = append(epochs, epoch)
	}
	slices.Sort(epochs)
	return epochs
}

// DrainForShutdown removes any Ready epoch after its owning runtime has
// stopped accepting calls. Unlike Drain, it is not a rollout operation.
func (m *Manager) DrainForShutdown(epoch uint64) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state != EpochReady {
		return fmt.Errorf("%w: epoch %d cannot drain for shutdown from %q", ErrInvalidEpochTransition, epoch, entry.state)
	}
	entry.state = EpochDraining
	if m.active == epoch {
		m.active = 0
		m.traffic = nil
	}
	return nil
}

// Stop waits for a Draining epoch's leases and marks it Stopped.
func (m *Manager) Stop(ctx context.Context, epoch uint64) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidEpochTransition)
	}
	m.mu.Lock()
	entry, exists := m.epochs[epoch]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	switch entry.state {
	case EpochStopped:
		m.mu.Unlock()
		return nil
	case EpochDraining:
	default:
		state := entry.state
		m.mu.Unlock()
		return fmt.Errorf("%w: epoch %d cannot stop from %q", ErrInvalidEpochTransition, epoch, state)
	}
	idle := entry.idle
	m.mu.Unlock()

	select {
	case <-idle:
	case <-ctx.Done():
		return context.Cause(ctx)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	entry = m.epochs[epoch]
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
func (m *Manager) State(epoch uint64) (EpochState, bool) {
	if m == nil {
		return "", false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[epoch]
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

func (m *Manager) release(epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[epoch]
	if !exists || entry.leases == 0 {
		return
	}
	entry.leases--
	if entry.leases == 0 {
		close(entry.idle)
	}
}
