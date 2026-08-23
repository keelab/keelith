package selector

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
)

const maximumNodeWeight = 1_000_000

type weightedState struct {
	node    Node
	weight  int64
	current int64
}

// WeightedRoundRobin implements smooth weighted round-robin selection.
//
// A missing metadata weight defaults to one. Invalid, zero, negative, or
// excessively large weights reject the complete discovery snapshot.
type WeightedRoundRobin struct {
	scheme    string
	weightKey string
	settings  settings

	mu       sync.Mutex
	service  string
	revision string
	nodes    map[string]*weightedState
}

// NewWeightedRoundRobin constructs a smooth weighted round-robin selector.
func NewWeightedRoundRobin(
	scheme string,
	weightKey string,
	options ...Option,
) (*WeightedRoundRobin, error) {
	normalizedScheme, err := validateScheme(scheme)
	if err != nil {
		return nil, err
	}
	weightKey = strings.TrimSpace(weightKey)
	if weightKey == "" {
		return nil, fmt.Errorf("%w: weight metadata key is empty", ErrInvalidOption)
	}
	settings, err := newSettings(options)
	if err != nil {
		return nil, err
	}
	return &WeightedRoundRobin{
		scheme:    normalizedScheme,
		weightKey: weightKey,
		settings:  settings,
		nodes:     make(map[string]*weightedState),
	}, nil
}

// PreferenceTierCount reports the configured bounded preference depth.
func (w *WeightedRoundRobin) PreferenceTierCount() int {
	if w == nil {
		return 0
	}
	return w.settings.preferenceTierCount()
}

// Update replaces nodes while preserving smooth scheduling state.
func (w *WeightedRoundRobin) Update(snapshot registry.Snapshot) error {
	nodes, err := nodesFromSnapshot(snapshot, w.scheme)
	if err != nil {
		return err
	}

	weights := make(map[string]int64, len(nodes))
	for _, node := range nodes {
		weight, parseErr := nodeWeight(node, w.weightKey)
		if parseErr != nil {
			return parseErr
		}
		weights[node.key()] = weight
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.service == snapshot.Service() &&
		w.revision == snapshot.Revision() {
		return nil
	}
	next := make(map[string]*weightedState, len(nodes))
	for _, node := range nodes {
		key := node.key()
		state := w.nodes[key]
		if state == nil {
			state = &weightedState{}
		}
		state.node = node
		state.weight = weights[key]
		next[key] = state
	}
	w.service = snapshot.Service()
	w.revision = snapshot.Revision()
	w.nodes = next
	return nil
}

// Select returns a node according to its advertised relative capacity.
func (w *WeightedRoundRobin) Select(
	ctx context.Context,
	operationID operation.Operation,
) (Node, Done, error) {
	w.mu.Lock()
	service := w.service
	nodes := make([]Node, 0, len(w.nodes))
	for _, state := range w.nodes {
		nodes = append(nodes, state.node)
	}
	w.mu.Unlock()
	sort.Slice(nodes, func(first, second int) bool {
		return nodes[first].key() < nodes[second].key()
	})

	if service != "" && operationID.Service() != service {
		return Node{}, nil, ErrServiceMismatch
	}
	eligible, err := eligibleNodes(ctx, operationID, nodes, w.settings)
	if err != nil {
		return Node{}, nil, err
	}

	w.mu.Lock()
	var chosen *weightedState
	var total int64
	for _, node := range eligible {
		state := w.nodes[node.key()]
		if state == nil {
			continue
		}
		state.current += state.weight
		total += state.weight
		if chosen == nil ||
			state.current > chosen.current ||
			state.current == chosen.current && state.node.key() < chosen.node.key() {
			chosen = state
		}
	}
	if chosen == nil {
		w.mu.Unlock()
		return Node{}, nil, ErrNoNodes
	}
	chosen.current -= total
	node := chosen.node
	w.mu.Unlock()

	return node, idempotentDone(func(result Result) {
		observeResult(w.settings, operationID, node, result)
	}), nil
}

func nodeWeight(node Node, key string) (int64, error) {
	raw, exists := node.metadata[key]
	if !exists {
		return 1, nil
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value <= 0 || value > maximumNodeWeight {
		return 0, fmt.Errorf(
			"%w: node %q metadata %q=%q",
			ErrInvalidWeight,
			node.ID(),
			key,
			raw,
		)
	}
	return value, nil
}
