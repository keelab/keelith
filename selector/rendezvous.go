package selector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
)

// HashKey extracts the stable affinity key for one selection.
//
// RoutingHintHashKey is the standard request-scoped implementation.
type HashKey func(context.Context, operation.Operation) (string, error)

// Rendezvous implements highest-random-weight consistent hashing.
type Rendezvous struct {
	scheme   string
	key      HashKey
	settings settings

	mu       sync.RWMutex
	service  string
	revision string
	nodes    []Node
}

// NewRendezvous constructs a consistent-hash selector.
func NewRendezvous(
	scheme string,
	key HashKey,
	options ...Option,
) (*Rendezvous, error) {
	normalizedScheme, err := validateScheme(scheme)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, fmt.Errorf("%w: extractor is nil", ErrHashKey)
	}
	settings, err := newSettings(options)
	if err != nil {
		return nil, err
	}
	return &Rendezvous{
		scheme:   normalizedScheme,
		key:      key,
		settings: settings,
	}, nil
}

// PreferenceTierCount reports the configured bounded preference depth.
func (selector *Rendezvous) PreferenceTierCount() int {
	if selector == nil {
		return 0
	}
	return selector.settings.preferenceTierCount()
}

// Update atomically replaces the current full Snapshot.
func (selector *Rendezvous) Update(snapshot registry.Snapshot) error {
	nodes, err := nodesFromSnapshot(snapshot, selector.scheme)
	if err != nil {
		return err
	}
	selector.mu.Lock()
	defer selector.mu.Unlock()
	if selector.service == snapshot.Service() &&
		selector.revision == snapshot.Revision() {
		return nil
	}
	selector.service = snapshot.Service()
	selector.revision = snapshot.Revision()
	selector.nodes = nodes
	return nil
}

// Select returns the highest-scoring eligible node for the extracted key.
func (selector *Rendezvous) Select(
	ctx context.Context,
	operationID operation.Operation,
) (Node, Done, error) {
	selector.mu.RLock()
	service := selector.service
	nodes := append([]Node(nil), selector.nodes...)
	selector.mu.RUnlock()

	if service != "" && operationID.Service() != service {
		return Node{}, nil, ErrServiceMismatch
	}
	eligible, err := eligibleNodes(ctx, operationID, nodes, selector.settings)
	if err != nil {
		return Node{}, nil, err
	}
	key, err := selector.key(ctx, operationID)
	if err != nil {
		return Node{}, nil, fmt.Errorf("%w: %w", ErrHashKey, err)
	}
	if strings.TrimSpace(key) == "" {
		return Node{}, nil, fmt.Errorf("%w: extractor returned an empty key", ErrHashKey)
	}

	chosen := eligible[0]
	best := rendezvousScore(key, chosen)
	for _, candidate := range eligible[1:] {
		score := rendezvousScore(key, candidate)
		if bytes.Compare(score[:], best[:]) > 0 {
			chosen = candidate
			best = score
		}
	}
	return chosen, idempotentDone(func(result Result) {
		observeResult(selector.settings, operationID, chosen, result)
	}), nil
}

func rendezvousScore(key string, node Node) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(key))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(node.key()))
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
