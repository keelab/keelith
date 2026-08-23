package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/keelab/keelith/registry"
)

var (
	// ErrNodeChangeWatcherClosed reports a closed Router node-change watcher.
	ErrNodeChangeWatcherClosed = errors.New("client: node change watcher closed")
)

// NodeUpdate describes one instance whose endpoints or metadata changed.
//
// Previous and Current return immutable registry values.
type NodeUpdate struct {
	previous registry.Instance
	current  registry.Instance
}

// Previous returns the instance before the accepted snapshot.
func (u NodeUpdate) Previous() registry.Instance {
	return u.previous.Clone()
}

// Current returns the instance in the accepted snapshot.
func (u NodeUpdate) Current() registry.Instance {
	return u.current.Clone()
}

// NodeChange is an immutable, revisioned Router topology update.
//
// Current always contains the complete accepted instance set. Consumers may
// therefore coalesce pending updates without losing the final topology.
type NodeChange struct {
	service          string
	previousRevision string
	revision         string
	current          []registry.Instance
	added            []registry.Instance
	updated          []NodeUpdate
	removed          []registry.Instance
}

// NewNodeChange validates two snapshots and derives one immutable topology
// change. A zero-valued previous snapshot represents an initial full state.
func NewNodeChange(previous registry.Snapshot, current registry.Snapshot) (NodeChange, error) {
	if err := current.Validate(); err != nil {
		return NodeChange{}, fmt.Errorf("client: current snapshot: %w", err)
	}
	previousIsZero := previous.Service() == "" &&
		previous.Revision() == "" &&
		len(previous.Instances()) == 0
	if !previousIsZero {
		if err := previous.Validate(); err != nil {
			return NodeChange{}, fmt.Errorf("client: previous snapshot: %w", err)
		}
		if previous.Service() != current.Service() {
			return NodeChange{}, fmt.Errorf("%w: snapshot services differ", ErrInvalidOption)
		}
	}
	return newNodeChange(previous, current), nil
}

// Service returns the logical service whose topology changed.
func (n NodeChange) Service() string {
	return n.service
}

// PreviousRevision returns the previously accepted discovery revision.
func (n NodeChange) PreviousRevision() string {
	return n.previousRevision
}

// Revision returns the newly accepted discovery revision.
func (n NodeChange) Revision() string {
	return n.revision
}

// Current returns the complete accepted instance set.
func (n NodeChange) Current() []registry.Instance {
	return cloneInstances(n.current)
}

// Added returns instances absent from the previous accepted snapshot.
func (n NodeChange) Added() []registry.Instance {
	return cloneInstances(n.added)
}

// Updated returns instances whose endpoints or metadata changed.
func (n NodeChange) Updated() []NodeUpdate {
	result := make([]NodeUpdate, len(n.updated))
	for index, u := range n.updated {
		result[index] = NodeUpdate{
			previous: u.previous.Clone(),
			current:  u.current.Clone(),
		}
	}
	return result
}

// Removed returns instances absent from the newly accepted snapshot.
func (n NodeChange) Removed() []registry.Instance {
	return cloneInstances(n.removed)
}

// NodeChangeSource creates service-scoped topology watchers.
type NodeChangeSource interface {
	WatchNodeChanges(context.Context) (NodeChangeWatcher, error)
}

// NodeChangeWatcher returns latest-wins full-state topology changes.
type NodeChangeWatcher interface {
	Next(context.Context) (NodeChange, error)
	Close() error
}

type routerNodeChangeWatcher struct {
	router   *Router
	parent   context.Context
	updates  chan NodeChange
	done     chan struct{}
	stopMu   sync.Mutex
	stop     func() bool
	closeOne sync.Once
}

func (w *routerNodeChangeWatcher) Next(ctx context.Context) (NodeChange, error) {
	if ctx == nil {
		return NodeChange{}, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	select {
	case change := <-w.updates:
		return change.clone(), nil
	case <-w.done:
		return NodeChange{}, ErrNodeChangeWatcherClosed
	case <-w.parent.Done():
		return NodeChange{}, context.Cause(w.parent)
	case <-ctx.Done():
		return NodeChange{}, context.Cause(ctx)
	}
}

func (w *routerNodeChangeWatcher) Close() error {
	if w == nil {
		return nil
	}
	w.closeOne.Do(func() {
		w.stopMu.Lock()
		stop := w.stop
		w.stopMu.Unlock()
		if stop != nil {
			stop()
		}
		if w.router != nil {
			w.router.removeNodeChangeWatcher(w)
		}
		close(w.done)
	})
	return nil
}

func (w *routerNodeChangeWatcher) setStop(stop func() bool) {
	w.stopMu.Lock()
	w.stop = stop
	w.stopMu.Unlock()
	select {
	case <-w.done:
		stop()
	default:
	}
}

func (w *routerNodeChangeWatcher) publish(change NodeChange) {
	select {
	case <-w.done:
		return
	default:
	}
	select {
	case <-w.updates:
	default:
	}
	select {
	case w.updates <- change.clone():
	case <-w.done:
	default:
	}
}

func (n NodeChange) clone() NodeChange {
	return NodeChange{
		service:          n.service,
		previousRevision: n.previousRevision,
		revision:         n.revision,
		current:          cloneInstances(n.current),
		added:            cloneInstances(n.added),
		updated:          n.Updated(),
		removed:          cloneInstances(n.removed),
	}
}

func newNodeChange(previous registry.Snapshot, current registry.Snapshot) NodeChange {
	previousByID := make(map[string]registry.Instance)
	for _, instance := range previous.Instances() {
		previousByID[instance.ID()] = instance
	}
	currentInstances := current.Instances()
	currentByID := make(map[string]registry.Instance, len(currentInstances))
	added := make([]registry.Instance, 0)
	updated := make([]NodeUpdate, 0)

	for _, instance := range currentInstances {
		currentByID[instance.ID()] = instance
		former, exists := previousByID[instance.ID()]
		switch {
		case !exists:
			added = append(added, instance)
		case !former.Equal(instance):
			updated = append(updated, NodeUpdate{
				previous: former,
				current:  instance,
			})
		}
	}
	removed := make([]registry.Instance, 0)

	for _, instance := range previous.Instances() {
		if _, exists := currentByID[instance.ID()]; !exists {
			removed = append(removed, instance)
		}
	}

	return NodeChange{
		service:          current.Service(),
		previousRevision: previous.Revision(),
		revision:         current.Revision(),
		current:          currentInstances,
		added:            added,
		updated:          updated,
		removed:          removed,
	}
}

func cloneInstances(source []registry.Instance) []registry.Instance {
	result := make([]registry.Instance, len(source))
	for index, instance := range source {
		result[index] = instance.Clone()
	}
	return result
}

var (
	_ NodeChangeSource  = (*Router)(nil)
	_ NodeChangeWatcher = (*routerNodeChangeWatcher)(nil)
)
