package selector

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
)

const ewmaWeight = 0.2

type nodeState struct {
	node           Node
	ewmaLatency    time.Duration
	inflight       int64
	failurePenalty float64
}

// P2C implements power-of-two-choices with latency and failure feedback.
type P2C struct {
	schemes  []string
	settings settings

	mu       sync.Mutex
	service  string
	revision string
	nodes    map[string]*nodeState
	random   uint64
}

// NewP2C constructs a feedback-aware P2C selector.
func NewP2C(scheme string, options ...Option) (*P2C, error) {
	return NewP2CForSchemes([]string{scheme}, options...)
}

// NewP2CForSchemes constructs one feedback-aware selector over an explicit,
// bounded set of endpoint schemes. Scheme names are case-insensitive and
// duplicates are rejected instead of silently changing the candidate set.
func NewP2CForSchemes(
	schemes []string,
	options ...Option,
) (*P2C, error) {
	normalizedSchemes, err := normalizeP2CSchemes(schemes)
	if err != nil {
		return nil, err
	}
	settings, err := newSettings(options)
	if err != nil {
		return nil, err
	}
	return &P2C{
		schemes:  normalizedSchemes,
		settings: settings,
		nodes:    make(map[string]*nodeState),
		random:   settings.seed,
	}, nil
}

// PreferenceTierCount reports the configured bounded preference depth.
func (selector *P2C) PreferenceTierCount() int {
	if selector == nil {
		return 0
	}
	return selector.settings.preferenceTierCount()
}

// Update replaces Nodes while preserving feedback for stable node keys.
func (selector *P2C) Update(snapshot registry.Snapshot) error {
	nodes, err := p2cNodesFromSnapshot(snapshot, selector.schemes)
	if err != nil {
		return err
	}

	selector.mu.Lock()
	defer selector.mu.Unlock()
	if selector.service == snapshot.Service() &&
		selector.revision == snapshot.Revision() {
		return nil
	}
	next := make(map[string]*nodeState, len(nodes))
	for _, node := range nodes {
		key := node.key()
		state := selector.nodes[key]
		if state == nil {
			state = &nodeState{node: node}
		} else {
			state.node = node
		}
		next[key] = state
	}
	selector.service = snapshot.Service()
	selector.revision = snapshot.Revision()
	selector.nodes = next
	return nil
}

func normalizeP2CSchemes(schemes []string) ([]string, error) {
	if len(schemes) == 0 || len(schemes) > 8 {
		return nil, fmt.Errorf(
			"%w: endpoint scheme count must be within 1..8",
			ErrInvalidOption,
		)
	}
	result := make([]string, 0, len(schemes))
	seen := make(map[string]struct{}, len(schemes))
	for _, scheme := range schemes {
		normalized, err := validateScheme(scheme)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[normalized]; duplicate {
			return nil, fmt.Errorf(
				"%w: endpoint scheme %q is duplicated",
				ErrInvalidOption,
				normalized,
			)
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result, nil
}

func p2cNodesFromSnapshot(
	snapshot registry.Snapshot,
	schemes []string,
) ([]Node, error) {
	nodes := make([]Node, 0)
	for _, scheme := range schemes {
		selected, err := nodesFromSnapshot(snapshot, scheme)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, selected...)
	}
	sort.Slice(nodes, func(first, second int) bool {
		return nodes[first].key() < nodes[second].key()
	})
	return nodes, nil
}

// Select samples two eligible Nodes and returns the lower score.
func (selector *P2C) Select(
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

	if service != "" && operationID.Service() != service {
		return Node{}, nil, ErrServiceMismatch
	}
	eligible, err := eligibleNodes(ctx, operationID, nodes, selector.settings)
	if err != nil {
		return Node{}, nil, err
	}

	selector.mu.Lock()
	candidates := make([]*nodeState, 0, len(eligible))
	for _, node := range eligible {
		if state := selector.nodes[node.key()]; state != nil {
			candidates = append(candidates, state)
		}
	}
	if len(candidates) == 0 {
		selector.mu.Unlock()
		return Node{}, nil, ErrNoNodes
	}
	chosen := selector.choose(candidates)
	chosen.inflight++
	node := chosen.node
	started := time.Now()
	selector.mu.Unlock()

	done := idempotentDone(func(result Result) {
		selector.record(chosen, started, result)
		observeResult(selector.settings, operationID, node, result)
	})
	return node, done, nil
}

// Stats returns stable node diagnostics.
func (selector *P2C) Stats() []NodeStats {
	selector.mu.Lock()
	defer selector.mu.Unlock()
	stats := make([]NodeStats, 0, len(selector.nodes))
	for _, state := range selector.nodes {
		stats = append(stats, NodeStats{
			Node:           state.node,
			EWMALatency:    state.ewmaLatency,
			Inflight:       state.inflight,
			FailurePenalty: state.failurePenalty,
		})
	}
	sort.Slice(stats, func(first, second int) bool {
		return stats[first].Node.key() < stats[second].Node.key()
	})
	return stats
}

func (selector *P2C) choose(candidates []*nodeState) *nodeState {
	if len(candidates) == 1 {
		return candidates[0]
	}
	first := candidates[selector.nextRandom()%uint64(len(candidates))]
	second := candidates[selector.nextRandom()%uint64(len(candidates))]
	if first == second {
		for _, candidate := range candidates {
			if candidate != first {
				second = candidate
				break
			}
		}
	}
	if score(second) < score(first) {
		return second
	}
	return first
}

func (selector *P2C) nextRandom() uint64 {
	value := selector.random
	value ^= value << 13
	value ^= value >> 7
	value ^= value << 17
	selector.random = value
	return value
}

func score(state *nodeState) float64 {
	latency := state.ewmaLatency
	if latency <= 0 {
		latency = time.Millisecond
	}
	return float64(latency) *
		float64(state.inflight+1) *
		(1 + state.failurePenalty)
}

func (selector *P2C) record(
	state *nodeState,
	started time.Time,
	result Result,
) {
	latency := result.Latency
	if latency <= 0 {
		latency = time.Since(started)
	}

	selector.mu.Lock()
	defer selector.mu.Unlock()
	if state.inflight > 0 {
		state.inflight--
	}
	if state.ewmaLatency <= 0 {
		state.ewmaLatency = latency
	} else {
		state.ewmaLatency = time.Duration(
			(1-ewmaWeight)*float64(state.ewmaLatency) +
				ewmaWeight*float64(latency),
		)
	}
	if result.Error != nil && !result.Canceled {
		state.failurePenalty++
	} else {
		state.failurePenalty *= 0.5
	}
}
