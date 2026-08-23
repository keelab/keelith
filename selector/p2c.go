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
func (p *P2C) PreferenceTierCount() int {
	if p == nil {
		return 0
	}
	return p.settings.preferenceTierCount()
}

// Update replaces Nodes while preserving feedback for stable node keys.
func (p *P2C) Update(snapshot registry.Snapshot) error {
	nodes, err := p2cNodesFromSnapshot(snapshot, p.schemes)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.service == snapshot.Service() &&
		p.revision == snapshot.Revision() {
		return nil
	}
	next := make(map[string]*nodeState, len(nodes))
	for _, node := range nodes {
		key := node.key()
		state := p.nodes[key]
		if state == nil {
			state = &nodeState{node: node}
		} else {
			state.node = node
		}
		next[key] = state
	}
	p.service = snapshot.Service()
	p.revision = snapshot.Revision()
	p.nodes = next
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
func (p *P2C) Select(
	ctx context.Context,
	operationID operation.Operation,
) (Node, Done, error) {
	p.mu.Lock()
	service := p.service
	nodes := make([]Node, 0, len(p.nodes))
	for _, state := range p.nodes {
		nodes = append(nodes, state.node)
	}
	p.mu.Unlock()

	if service != "" && operationID.Service() != service {
		return Node{}, nil, ErrServiceMismatch
	}
	eligible, err := eligibleNodes(ctx, operationID, nodes, p.settings)
	if err != nil {
		return Node{}, nil, err
	}

	p.mu.Lock()
	candidates := make([]*nodeState, 0, len(eligible))
	for _, node := range eligible {
		if state := p.nodes[node.key()]; state != nil {
			candidates = append(candidates, state)
		}
	}
	if len(candidates) == 0 {
		p.mu.Unlock()
		return Node{}, nil, ErrNoNodes
	}
	chosen := p.choose(candidates)
	chosen.inflight++
	node := chosen.node
	started := time.Now()
	p.mu.Unlock()

	done := idempotentDone(func(result Result) {
		p.record(chosen, started, result)
		observeResult(p.settings, operationID, node, result)
	})
	return node, done, nil
}

// Stats returns stable node diagnostics.
func (p *P2C) Stats() []NodeStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := make([]NodeStats, 0, len(p.nodes))
	for _, state := range p.nodes {
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

func (p *P2C) choose(candidates []*nodeState) *nodeState {
	if len(candidates) == 1 {
		return candidates[0]
	}
	first := candidates[p.nextRandom()%uint64(len(candidates))]
	second := candidates[p.nextRandom()%uint64(len(candidates))]
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

func (p *P2C) nextRandom() uint64 {
	value := p.random
	value ^= value << 13
	value ^= value >> 7
	value ^= value << 17
	p.random = value
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

func (p *P2C) record(
	state *nodeState,
	started time.Time,
	result Result,
) {
	latency := result.Latency
	if latency <= 0 {
		latency = time.Since(started)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
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
