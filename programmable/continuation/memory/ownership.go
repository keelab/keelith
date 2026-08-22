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
func (store *OwnershipStore) Bind(
	ctx context.Context,
	ownership continuation.Ownership,
) error {
	if store == nil || ctx == nil || !ownership.Valid() {
		return continuation.ErrInvalidService
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := ownership.CallID().String()
	current, exists := store.values[key]
	if exists {
		if current.Equal(ownership) {
			return nil
		}
		return continuation.ErrOwnershipConflict
	}
	store.values[key] = ownership
	return nil
}

// Load returns one immutable ownership binding.
func (store *OwnershipStore) Load(
	ctx context.Context,
	callID continuation.CallID,
) (continuation.Ownership, error) {
	if store == nil || ctx == nil || callID.String() == "" {
		return continuation.Ownership{}, continuation.ErrInvalidService
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Ownership{}, cause
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	ownership, exists := store.values[callID.String()]
	if !exists {
		return continuation.Ownership{}, continuation.ErrOwnershipNotFound
	}
	return ownership, nil
}
