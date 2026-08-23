package topology

import "fmt"

// Active returns the current Ready snapshot.
func (m *Manager) Active() (Snapshot, bool) {
	if m == nil {
		return Snapshot{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[m.active]
	if !exists || entry.state != EpochReady {
		return Snapshot{}, false
	}
	return entry.snapshot, true
}

// DrainActive moves the current Ready epoch to Draining without requiring a
// successor. It is reserved for whole-runtime shutdown, when no new leases
// will be accepted.
func (m *Manager) DrainActive(epoch uint64) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state != EpochReady || m.active != epoch {
		return fmt.Errorf(
			"%w: epoch %d cannot drain as active from %q",
			ErrInvalidEpochTransition,
			epoch,
			entry.state,
		)
	}
	entry.state = EpochDraining
	m.active = 0
	m.traffic = nil
	return nil
}

// Discard marks one never-ready staging epoch Stopped.
func (m *Manager) Discard(epoch uint64) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state == EpochStopped {
		return nil
	}
	if entry.state != EpochStaging || entry.leases != 0 {
		return fmt.Errorf(
			"%w: epoch %d cannot be discarded from %q",
			ErrInvalidEpochTransition,
			epoch,
			entry.state,
		)
	}
	entry.state = EpochStopped
	return nil
}
