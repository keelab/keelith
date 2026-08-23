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
func (selector *WeightedRoundRobin) PreferenceTierCount() int {
	if selector == nil {
		return 0
	}
	return selector.settings.preferenceTierCount()
}

// Update replaces nodes while preserving smooth scheduling state.
func (selector *WeightedRoundRobin) Update(snapshot registry.Snapshot) error {
	nodes, err := nodesFromSnapshot(snapshot, selector.scheme)
	if err != nil {
		return err
	}

	weights := make(map[string]int64, len(nodes))
	for _, node := range nodes {
		weight, parseErr := nodeWeight(node, selector.weightKey)
		if parseErr != nil {
			return parseErr
		}
		weights[node.key()] = weight
	}

	selector.mu.Lock()
	defer selector.mu.Unlock()
	if selector.service == snapshot.Service() &&
		selector.revision == snapshot.Revision() {
		return nil
	}
	next := make(map[string]*weightedState, len(nodes))
	for _, node := range nodes {
		key := node.key()
		state := selector.nodes[key]
		if state == nil {
			state = &weightedState{}
		}
		state.node = node
		state.weight = weights[key]
		next[key] = state
	}
	selector.service = snapshot.Service()
	selector.revision = snapshot.Revision()
	selector.nodes = next
	return nil
}

// Select returns a node according to its advertised relative capacity.
func (selector *WeightedRoundRobin) Select(
	ctx context.Context,
	operationID operation.Operation,
) (Node, Done, error) {
	selector.mu.Lock()
	service := selector.service
	nodes := make([]Node, 0, len(selector.nodes))
	for _, state := range selector.nodes {
		nodes = append(nodes, state.node)
	}
	selector.mu.Unlock()
	sort.Slice(nodes, func(first, second int) bool {
		return nodes[first].key() < nodes[second].key()
	})

	if service != "" && operationID.Service() != service {
		return Node{}, nil, ErrServiceMismatch
	}
	eligible, err := eligibleNodes(ctx, operationID, nodes, selector.settings)
	if err != nil {
		return Node{}, nil, err
	}

	selector.mu.Lock()
	var chosen *weightedState
	var total int64
	for _, node := range eligible {
		state := selector.nodes[node.key()]
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
		selector.mu.Unlock()
		return Node{}, nil, ErrNoNodes
	}
	chosen.current -= total
	node := chosen.node
	selector.mu.Unlock()

	return node, idempotentDone(func(result Result) {
		observeResult(selector.settings, operationID, node, result)
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
