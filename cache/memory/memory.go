// Package memory provides an in-process implementation of cache.Backend.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/keelab/keelith/cache"
)

var _ cache.VersionedBackend = (*Store)(nil)

// Clock supplies deterministic expiration time.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

type entry struct {
	value     []byte // value is a defensive copy of the cached value.
	expiresAt time.Time
}

// Store is a concurrency-safe in-memory cache backend.
type Store struct {
	clock Clock

	mu       sync.Mutex
	entries  map[string]entry  // entries is the map of cached values.
	versions map[string]uint64 // versions is the map of version numbers.
}

// New creates an in-memory backend using the system clock.
func New() *Store {
	return NewWithClock(systemClock{})
}

// NewWithClock creates an in-memory backend with an injected clock.
func NewWithClock(clock Clock) *Store {
	if clock == nil {
		clock = systemClock{}
	}
	return &Store{
		clock:    clock,
		entries:  make(map[string]entry),
		versions: make(map[string]uint64),
	}
}

// Get returns a defensive value copy or cache.ErrMiss.
func (store *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	now := store.clock.Now()
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.entries[key]
	if !ok {
		return nil, cache.ErrMiss
	}
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		delete(store.entries, key)
		return nil, cache.ErrMiss
	}
	return append([]byte(nil), entry.value...), nil
}

// Set stores a defensive copy. A zero TTL means no expiration.
func (store *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = store.clock.Now().Add(ttl)
	}
	store.mu.Lock()
	store.entries[key] = entry{
		value:     append([]byte(nil), value...),
		expiresAt: expiresAt,
	}
	store.mu.Unlock()
	return nil
}

// Delete removes keys and reports how many existed.
func (store *Store) Delete(ctx context.Context, keys ...string) (int64, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var deleted int64
	for _, key := range keys {
		if _, ok := store.entries[key]; ok {
			delete(store.entries, key)
			deleted++
		}
	}
	return deleted, nil
}

// CurrentVersion returns the in-process invalidation watermark for key.
func (store *Store) CurrentVersion(ctx context.Context, key string) (uint64, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.versions[key], nil
}

// SetIfVersion stores only while the invalidation watermark is unchanged.
func (store *Store) SetIfVersion(ctx context.Context, key string, value []byte, ttl time.Duration, expected uint64) (bool, error) {
	if cause := context.Cause(ctx); cause != nil {
		return false, cause
	}
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = store.clock.Now().Add(ttl)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.versions[key] != expected {
		return false, nil
	}
	store.entries[key] = entry{
		value:     append([]byte(nil), value...),
		expiresAt: expiresAt,
	}
	return true, nil
}

// ApplyInvalidation advances a key watermark and atomically deletes its value.
func (store *Store) ApplyInvalidation(ctx context.Context, key string, version uint64) (cache.InvalidationState, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	if version == 0 {
		return 0, fmt.Errorf("%w: version is zero", cache.ErrInvalidOption)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.versions[key]
	switch {
	case version > current:
		store.versions[key] = version
		delete(store.entries, key)
		return cache.InvalidationApplied, nil
	case version == current:
		return cache.InvalidationCurrent, nil
	default:
		return cache.InvalidationStale, nil
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}
