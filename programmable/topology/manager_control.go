package topology

import "fmt"

// Active returns the current Ready snapshot.
func (manager *Manager) Active() (Snapshot, bool) {
	if manager == nil {
		return Snapshot{}, false
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[manager.active]
	if !exists || entry.state != EpochReady {
		return Snapshot{}, false
	}
	return entry.snapshot, true
}

// DrainActive moves the current Ready epoch to Draining without requiring a
// successor. It is reserved for whole-runtime shutdown, when no new leases
// will be accepted.
func (manager *Manager) DrainActive(epoch uint64) error {
	if manager == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[epoch]
	if !exists {
		return fmt.Errorf("%w: epoch %d", ErrEpochNotFound, epoch)
	}
	if entry.state != EpochReady || manager.active != epoch {
		return fmt.Errorf(
			"%w: epoch %d cannot drain as active from %q",
			ErrInvalidEpochTransition,
			epoch,
			entry.state,
		)
	}
	entry.state = EpochDraining
	manager.active = 0
	manager.traffic = nil
	return nil
}

// Discard marks one never-ready staging epoch Stopped.
func (manager *Manager) Discard(epoch uint64) error {
	if manager == nil {
		return fmt.Errorf("%w: manager is nil", ErrEpochNotFound)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.epochs[epoch]
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
