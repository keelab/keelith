package cache

import (
	"errors"
	"time"
)

// NegativeClassifier identifies safe-to-cache absent results.
type NegativeClassifier func(error) bool

// Policy defines read-through and expiration behavior.
type Policy struct {
	TTL           time.Duration
	NegativeTTL   time.Duration
	JitterRatio   float64
	FailOpen      bool
	IsNegative    NegativeClassifier
	NegativeError error
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
