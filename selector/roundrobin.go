package selector

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
)

// RoundRobin selects eligible Nodes in stable rotation.
type RoundRobin struct {
	scheme   string
	settings settings

	mu       sync.RWMutex
	service  string
	revision string
	nodes    []Node
	next     atomic.Uint64
}

// NewRoundRobin constructs a RoundRobin selector.
func NewRoundRobin(scheme string, options ...Option) (*RoundRobin, error) {
	normalizedScheme, err := validateScheme(scheme)
	if err != nil {
		return nil, err
	}
	settings, err := newSettings(options)
	if err != nil {
		return nil, err
	}
	return &RoundRobin{scheme: normalizedScheme, settings: settings}, nil
}

// PreferenceTierCount reports the configured bounded preference depth.
func (rr *RoundRobin) PreferenceTierCount() int {
	if rr == nil {
		return 0
	}
	return rr.settings.preferenceTierCount()
}

// Update atomically replaces the current full Snapshot.
func (rr *RoundRobin) Update(snapshot registry.Snapshot) error {
	nodes, err := nodesFromSnapshot(snapshot, rr.scheme)
	if err != nil {
		return err
	}
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if rr.service == snapshot.Service() &&
		rr.revision == snapshot.Revision() {
		return nil
	}
	rr.service = snapshot.Service()
	rr.revision = snapshot.Revision()
	rr.nodes = nodes
	return nil
}

// Select returns the next eligible Node.
func (rr *RoundRobin) Select(
	ctx context.Context,
	operationID operation.Operation,
) (Node, Done, error) {
	rr.mu.RLock()
	service := rr.service
	nodes := append([]Node(nil), rr.nodes...)
	rr.mu.RUnlock()

	if service != "" && operationID.Service() != service {
		return Node{}, nil, ErrServiceMismatch
	}
	eligible, err := eligibleNodes(ctx, operationID, nodes, rr.settings)
	if err != nil {
		return Node{}, nil, err
	}
	index := (rr.next.Add(1) - 1) % uint64(len(eligible))
	node := eligible[index]
	return node, idempotentDone(func(result Result) {
		observeResult(rr.settings, operationID, node, result)
	}), nil
}
