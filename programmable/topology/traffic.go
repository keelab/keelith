package topology

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
)

// TotalBasisPoints is the exact traffic weight total for one revision.
const TotalBasisPoints uint16 = 10_000

const maximumTrafficEpochs = 64

var (
	// ErrInvalidTraffic reports incomplete, duplicate, or unbalanced weights.
	ErrInvalidTraffic = errors.New("topology: invalid traffic weights")
)

// EpochWeight assigns one immutable Ready epoch a share of new calls.
// Zero weight is retained explicitly so rollback revisions can audit which
// failed candidate was removed from traffic.
type EpochWeight struct {
	Epoch       uint64
	BasisPoints uint16
}

// TrafficSelector is an immutable weighted selection table. Routing keys use
// weighted rendezvous hashing; an empty key uses a bounded 10,000-slot cycle.
type TrafficSelector struct {
	weights  []EpochWeight
	rotation atomic.Uint64
}

// NewTrafficSelector validates, sorts, and freezes one weight table.
func NewTrafficSelector(weights []EpochWeight) (*TrafficSelector, error) {
	if len(weights) == 0 || len(weights) > maximumTrafficEpochs {
		return nil, ErrInvalidTraffic
	}
	cloned := append([]EpochWeight(nil), weights...)
	sort.Slice(cloned, func(first, second int) bool {
		return cloned[first].Epoch < cloned[second].Epoch
	})
	total := uint64(0)
	positive := false
	for index, weight := range cloned {
		if weight.Epoch == 0 || index > 0 && cloned[index-1].Epoch == weight.Epoch {
			return nil, ErrInvalidTraffic
		}
		total += uint64(weight.BasisPoints)
		positive = positive || weight.BasisPoints != 0
	}
	if !positive || total != uint64(TotalBasisPoints) {
		return nil, fmt.Errorf("%w: total is %d, want %d", ErrInvalidTraffic, total, TotalBasisPoints)
	}
	return &TrafficSelector{weights: cloned}, nil
}

// Select returns the epoch for one new call. A routing key is stable across
// callers and processes; an empty key advances through one bounded cycle.
func (selector *TrafficSelector) Select(routingKey string) (uint64, error) {
	if selector == nil || len(selector.weights) == 0 {
		return 0, ErrInvalidTraffic
	}
	if routingKey == "" {
		bucket := uint16(
			(selector.rotation.Add(1) - 1) % uint64(TotalBasisPoints),
		)
		return selector.selectBucket(bucket), nil
	}
	return selector.selectRendezvous(routingKey), nil
}

// Weights returns an independent, canonical copy of the table.
func (selector *TrafficSelector) Weights() []EpochWeight {
	if selector == nil {
		return nil
	}
	return append([]EpochWeight(nil), selector.weights...)
}

// BasisPoints returns the configured weight for one epoch.
func (selector *TrafficSelector) BasisPoints(epoch uint64) uint16 {
	if selector == nil {
		return 0
	}
	index := sort.Search(len(selector.weights), func(index int) bool {
		return selector.weights[index].Epoch >= epoch
	})
	if index == len(selector.weights) || selector.weights[index].Epoch != epoch {
		return 0
	}
	return selector.weights[index].BasisPoints
}

func (selector *TrafficSelector) selectBucket(bucket uint16) uint64 {
	cumulative := uint32(0)
	for _, weight := range selector.weights {
		cumulative += uint32(weight.BasisPoints)
		if uint32(bucket) < cumulative {
			return weight.Epoch
		}
	}
	return selector.weights[len(selector.weights)-1].Epoch
}

func (selector *TrafficSelector) selectRendezvous(key string) uint64 {
	selected := uint64(0)
	best := math.Inf(1)
	for _, weight := range selector.weights {
		if weight.BasisPoints == 0 {
			continue
		}
		unit := rendezvousUnit(key, weight.Epoch)
		score := -math.Log(unit) / float64(weight.BasisPoints)
		if score < best || score == best && weight.Epoch < selected {
			best = score
			selected = weight.Epoch
		}
	}
	return selected
}

func rendezvousUnit(key string, epoch uint64) float64 {
	const (
		offset64 = uint64(14695981039346656037)
		prime64  = uint64(1099511628211)
	)
	hash := offset64
	for index := range len(key) {
		hash ^= uint64(key[index])
		hash *= prime64
	}
	// Mix the epoch as a separate rendezvous node identity, then avalanche the
	// combined value so adjacent epoch numbers do not have correlated scores.
	hash ^= epoch * 0x9e3779b97f4a7c15
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	hash ^= hash >> 31
	// Use the stable high 53 bits and keep the result strictly inside (0, 1).
	const denominator = float64(uint64(1)<<53) + 1
	return float64((hash>>11)+1) / denominator
}

// Traffic returns the effective immutable routing table. Legacy snapshots
// without an explicit table route all new calls to their own epoch.
func (snapshot Snapshot) Traffic() []EpochWeight {
	if snapshot.epoch == 0 {
		return nil
	}
	if snapshot.traffic == nil {
		return []EpochWeight{{
			Epoch: snapshot.epoch, BasisPoints: TotalBasisPoints,
		}}
	}
	return append([]EpochWeight(nil), snapshot.traffic...)
}
