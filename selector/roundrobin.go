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
func (roundRobin *RoundRobin) PreferenceTierCount() int {
	if roundRobin == nil {
		return 0
	}
	return roundRobin.settings.preferenceTierCount()
}

// Update atomically replaces the current full Snapshot.
func (roundRobin *RoundRobin) Update(snapshot registry.Snapshot) error {
	nodes, err := nodesFromSnapshot(snapshot, roundRobin.scheme)
	if err != nil {
		return err
	}
	roundRobin.mu.Lock()
	defer roundRobin.mu.Unlock()
	if roundRobin.service == snapshot.Service() &&
		roundRobin.revision == snapshot.Revision() {
		return nil
	}
	roundRobin.service = snapshot.Service()
	roundRobin.revision = snapshot.Revision()
	roundRobin.nodes = nodes
	return nil
}

// Select returns the next eligible Node.
func (roundRobin *RoundRobin) Select(
	ctx context.Context,
	operationID operation.Operation,
) (Node, Done, error) {
	roundRobin.mu.RLock()
	service := roundRobin.service
	nodes := append([]Node(nil), roundRobin.nodes...)
	roundRobin.mu.RUnlock()

	if service != "" && operationID.Service() != service {
		return Node{}, nil, ErrServiceMismatch
	}
	eligible, err := eligibleNodes(ctx, operationID, nodes, roundRobin.settings)
	if err != nil {
		return Node{}, nil, err
	}
	index := (roundRobin.next.Add(1) - 1) % uint64(len(eligible))
	node := eligible[index]
	return node, idempotentDone(func(result Result) {
		observeResult(roundRobin.settings, operationID, node, result)
	}), nil
}
