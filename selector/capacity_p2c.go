package selector

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
)

const maximumAdvertisedCapacity = float64(1<<32 - 1)

type capacityNodeState struct {
	node           Node
	capacity       float64
	ewmaLatency    time.Duration
	inflight       int64
	failurePenalty float64
}

// CapacityP2C combines an advertised relative capacity with local latency,
// inflight, failure, filter, preference, and observer feedback.
//
// Nodes without capacityKey use capacity 1. A present capacity value must be a
// finite decimal within (0, 2^32-1].
type CapacityP2C struct {
	schemes     []string
	capacityKey string
	settings    settings

	mu       sync.Mutex
	service  string
	revision string
	nodes    map[string]*capacityNodeState
	random   uint64
}

// CapacityNodeStats is an immutable capacity-aware P2C diagnostic snapshot.
type CapacityNodeStats struct {
	NodeStats
	Capacity float64
}

// NewCapacityP2C constructs a capacity-aware, feedback-driven selector.
//
// It intentionally uses an explicit metadata key so provider-specific
// projections can opt in without changing the default P2C contract.
func NewCapacityP2C(
	scheme string,
	capacityKey string,
	options ...Option,
) (*CapacityP2C, error) {
	return NewCapacityP2CForSchemes(
		[]string{scheme},
		capacityKey,
		options...,
	)
}

// NewCapacityP2CForSchemes constructs one capacity-aware selector over an
// explicit, bounded set of endpoint schemes.
func NewCapacityP2CForSchemes(
	schemes []string,
	capacityKey string,
	options ...Option,
) (*CapacityP2C, error) {
	normalizedSchemes, err := normalizeP2CSchemes(schemes)
	if err != nil {
		return nil, err
	}
	if !validPreferenceKey(capacityKey) {
		return nil, fmt.Errorf(
			"%w: capacity metadata key is malformed",
			ErrInvalidOption,
		)
	}
	settings, err := newSettings(options)
	if err != nil {
		return nil, err
	}
	return &CapacityP2C{
		schemes:     normalizedSchemes,
		capacityKey: capacityKey,
		settings:    settings,
		nodes:       make(map[string]*capacityNodeState),
		random:      settings.seed,
	}, nil
}

// PreferenceTierCount reports the configured bounded preference depth.
func (selector *CapacityP2C) PreferenceTierCount() int {
	if selector == nil {
		return 0
	}
	return selector.settings.preferenceTierCount()
}

// Update atomically replaces Nodes while preserving feedback for stable keys.
func (selector *CapacityP2C) Update(snapshot registry.Snapshot) error {
	nodes, err := p2cNodesFromSnapshot(snapshot, selector.schemes)
	if err != nil {
		return err
	}
	capacities := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		capacity, err := advertisedCapacity(node, selector.capacityKey)
		if err != nil {
			return err
		}
		capacities[node.key()] = capacity
	}

	selector.mu.Lock()
	defer selector.mu.Unlock()
	if selector.service == snapshot.Service() &&
		selector.revision == snapshot.Revision() {
		return nil
	}
	next := make(map[string]*capacityNodeState, len(nodes))
	for _, node := range nodes {
		key := node.key()
		state := selector.nodes[key]
		if state == nil {
			state = &capacityNodeState{node: node}
		} else {
			state.node = node
		}
		state.capacity = capacities[key]
		next[key] = state
	}
	selector.service = snapshot.Service()
	selector.revision = snapshot.Revision()
	selector.nodes = next
	return nil
}

// Select returns the lower normalized load of two eligible Nodes.
func (selector *CapacityP2C) Select(
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
	candidates := make([]*capacityNodeState, 0, len(eligible))
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

// Stats returns stable node diagnostics without exposing mutable state.
func (selector *CapacityP2C) Stats() []CapacityNodeStats {
	selector.mu.Lock()
	defer selector.mu.Unlock()
	stats := make([]CapacityNodeStats, 0, len(selector.nodes))
	for _, state := range selector.nodes {
		stats = append(stats, CapacityNodeStats{
			NodeStats: NodeStats{
				Node:           state.node,
				EWMALatency:    state.ewmaLatency,
				Inflight:       state.inflight,
				FailurePenalty: state.failurePenalty,
			},
			Capacity: state.capacity,
		})
	}
	sort.Slice(stats, func(first, second int) bool {
		return stats[first].Node.key() < stats[second].Node.key()
	})
	return stats
}

func (selector *CapacityP2C) choose(
	candidates []*capacityNodeState,
) *capacityNodeState {
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
	if capacityScore(second) < capacityScore(first) {
		return second
	}
	return first
}

func (selector *CapacityP2C) nextRandom() uint64 {
	value := selector.random
	value ^= value << 13
	value ^= value >> 7
	value ^= value << 17
	selector.random = value
	return value
}

func (selector *CapacityP2C) record(
	state *capacityNodeState,
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

func advertisedCapacity(node Node, key string) (float64, error) {
	raw, ok := node.metadata[key]
	if !ok {
		return 1, nil
	}
	if raw == "" || strings.TrimSpace(raw) != raw {
		return 0, fmt.Errorf(
			"%w: node %q capacity is malformed",
			ErrInvalidWeight,
			node.id,
		)
	}
	capacity, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(capacity) || math.IsInf(capacity, 0) ||
		capacity <= 0 || capacity > maximumAdvertisedCapacity {
		return 0, fmt.Errorf(
			"%w: node %q capacity is malformed",
			ErrInvalidWeight,
			node.id,
		)
	}
	return capacity, nil
}

func capacityScore(state *capacityNodeState) float64 {
	latency := state.ewmaLatency
	if latency <= 0 {
		latency = time.Millisecond
	}
	return float64(latency) *
		float64(state.inflight+1) *
		(1 + state.failurePenalty) /
		state.capacity
}
