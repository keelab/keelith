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
	Now() time.Time
}

type entry struct {
	value     []byte
	expiresAt time.Time
}

// Store is a concurrency-safe in-memory cache backend.
type Store struct {
	clock Clock

	mu       sync.Mutex
	entries  map[string]entry
	versions map[string]uint64
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
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	if !ok {
		return nil, cache.ErrMiss
	}
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		delete(s.entries, key)
		return nil, cache.ErrMiss
	}
	return append([]byte(nil), entry.value...), nil
}

// Set stores a defensive copy. A zero TTL means no expiration.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = s.clock.Now().Add(ttl)
	}
	s.mu.Lock()
	s.entries[key] = entry{
		value:     append([]byte(nil), value...),
		expiresAt: expiresAt,
	}
	s.mu.Unlock()
	return nil
}

// Delete removes keys and reports how many existed.
func (s *Store) Delete(ctx context.Context, keys ...string) (int64, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var deleted int64
	for _, key := range keys {
		if _, ok := s.entries[key]; ok {
			delete(s.entries, key)
			deleted++
		}
	}
	return deleted, nil
}

// CurrentVersion returns the in-process invalidation watermark for key.
func (s *Store) CurrentVersion(ctx context.Context, key string) (uint64, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.versions[key], nil
}

// SetIfVersion stores only while the invalidation watermark is unchanged.
func (s *Store) SetIfVersion(ctx context.Context, key string, value []byte, ttl time.Duration, expected uint64) (bool, error) {
	if cause := context.Cause(ctx); cause != nil {
		return false, cause
	}
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = s.clock.Now().Add(ttl)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.versions[key] != expected {
		return false, nil
	}
	s.entries[key] = entry{
		value:     append([]byte(nil), value...),
		expiresAt: expiresAt,
	}
	return true, nil
}

// ApplyInvalidation advances a key watermark and atomically deletes its value.
func (s *Store) ApplyInvalidation(ctx context.Context, key string, version uint64) (cache.InvalidationState, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	if version == 0 {
		return 0, fmt.Errorf("%w: version is zero", cache.ErrInvalidOption)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.versions[key]
	switch {
	case version > current:
		s.versions[key] = version
		delete(s.entries, key)
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
