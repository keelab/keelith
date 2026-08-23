package memory

import (
	"context"
	"sync"

	"github.com/keelab/keelith/programmable/continuation"
)

// OwnershipStore keeps payload-free CallID ownership bindings for tests and
// single-process deployments.
type OwnershipStore struct {
	mu     sync.RWMutex
	values map[string]continuation.Ownership
}

// NewOwnershipStore constructs an empty concurrency-safe access store.
func NewOwnershipStore() *OwnershipStore {
	return &OwnershipStore{
		values: make(map[string]continuation.Ownership),
	}
}

// Bind idempotently records an exact resource and rejects another owner or
// operation for the same CallID.
func (s *OwnershipStore) Bind(
	ctx context.Context,
	ownership continuation.Ownership,
) error {
	if s == nil || ctx == nil || !ownership.Valid() {
		return continuation.ErrInvalidService
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ownership.CallID().String()
	current, exists := s.values[key]
	if exists {
		if current.Equal(ownership) {
			return nil
		}
		return continuation.ErrOwnershipConflict
	}
	s.values[key] = ownership
	return nil
}

// Load returns one immutable ownership binding.
func (s *OwnershipStore) Load(
	ctx context.Context,
	callID continuation.CallID,
) (continuation.Ownership, error) {
	if s == nil || ctx == nil || callID.String() == "" {
		return continuation.Ownership{}, continuation.ErrInvalidService
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Ownership{}, cause
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ownership, exists := s.values[callID.String()]
	if !exists {
		return continuation.Ownership{}, continuation.ErrOwnershipNotFound
	}
	return ownership, nil
}
