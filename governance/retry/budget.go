package retry

import (
	"math"
	"sort"
	"sync"
)

// BudgetDescription is one stable Operation budget snapshot.
type BudgetDescription struct {
	Operation       string
	Primary         uint64
	Retries         uint64
	RejectedRetries uint64
}

// Budget bounds retry amplification per stable Operation.
//
// burst grants a small startup allowance. Long-term additional retries are
// limited by Policy.BudgetRatio relative to primary requests.
type Budget struct {
	burst uint64

	mu      sync.Mutex
	buckets map[string]budgetBucket
}

type budgetBucket struct {
	primary  uint64
	retries  uint64
	rejected uint64
}

// NewBudget creates a ratio budget with burst initial retries per Operation.
func NewBudget(burst uint64) *Budget {
	return &Budget{
		burst:   burst,
		buckets: make(map[string]budgetBucket),
	}
}

func (b *Budget) begin(key string) {
	b.mu.Lock()
	bucket := b.buckets[key]
	bucket.primary++
	b.buckets[key] = bucket
	b.mu.Unlock()
}

func (b *Budget) take(key string, ratio float64) bool {
	if b == nil || ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bucket := b.buckets[key]
	ratioAllowance := uint64(math.Floor(float64(bucket.primary) * ratio))
	allowed := b.burst + ratioAllowance
	if bucket.retries >= allowed {
		bucket.rejected++
		b.buckets[key] = bucket
		return false
	}
	bucket.retries++
	b.buckets[key] = bucket
	return true
}

// Describe returns sorted, immutable low-cardinality budget state.
func (b *Budget) Describe() []BudgetDescription {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	result := make([]BudgetDescription, 0, len(b.buckets))
	for key, bucket := range b.buckets {
		result = append(result, BudgetDescription{
			Operation:       key,
			Primary:         bucket.primary,
			Retries:         bucket.retries,
			RejectedRetries: bucket.rejected,
		})
	}
	b.mu.Unlock()
	sort.Slice(result, func(first, second int) bool {
		return result[first].Operation < result[second].Operation
	})
	return result
}
