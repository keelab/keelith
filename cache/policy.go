package cache

import (
	"errors"
	"time"
)

// NegativeClassifier identifies safe-to-cache absent results.
type NegativeClassifier func(error) bool

// Policy defines read-through and expiration behavior.
type Policy struct {
	TTL           time.Duration      // TTL is the time-to-live for cache entries.
	NegativeTTL   time.Duration      // NegativeTTL is the time-to-live for negative cache entries.
	JitterRatio   float64            // JitterRatio is the ratio of TTL jitter to apply.
	FailOpen      bool               // FailOpen indicates whether to fail open on cache errors.
	IsNegative    NegativeClassifier // IsNegative is the negative classifier function.
	NegativeError error              // NegativeError is the error to return for negative cache entries.
}

// DefaultPolicy returns a safe read-through baseline.
func DefaultPolicy() Policy {
	return Policy{
		TTL:         5 * time.Minute,
		NegativeTTL: 30 * time.Second,
		JitterRatio: 0.1,
		FailOpen:    true,
		IsNegative: func(err error) bool {
			return errors.Is(err, ErrNotFound)
		},
		NegativeError: ErrNotFound,
	}
}
