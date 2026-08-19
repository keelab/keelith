// Package cache provides backend-neutral read-through cache policy.
package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/sync/singleflight"
)

var (
	// ErrMiss reports that a backend does not contain a key.
	ErrMiss = errors.New("cache: miss")
	// ErrNotFound is the default stable negative loader result.
	ErrNotFound = errors.New("cache: value not found")
	// ErrCorrupt reports a malformed or undecodable cached value.
	ErrCorrupt = errors.New("cache: corrupt value")
	// ErrInvalidOption reports an invalid cache dependency or policy.
	ErrInvalidOption = errors.New("cache: invalid option")
	// ErrVersioningUnsupported reports a backend without atomic version guards.
	ErrVersioningUnsupported = errors.New("cache: versioning unsupported")
	// ErrStaleWrite reports an explicit Set that raced a newer invalidation.
	ErrStaleWrite = errors.New("cache: stale write suppressed")
	// ErrVersionRequired reports legacy deletion on a versioned Cache.
	ErrVersionRequired = errors.New("cache: invalidation version required")
)

const (
	envelopeValue    byte = 1    // envelopeValue is the byte value used to indicate a positive cache hit.
	envelopeNegative byte = 2    // envelopeNegative is the byte value used to indicate a negative cache hit.
	mutationStripes       = 256  // mutationStripes is the number of stripes used for cache mutation.
	maxCacheKeyBytes      = 1024 // maxCacheKeyBytes is the maximum number of bytes allowed in a cache key.
)

// Backend is the minimal binary cache storage contract.
type Backend interface {
	// Get retrieves the value associated with the given key from the cache.
	Get(context.Context, string) ([]byte, error)
	// Set stores the value associated with the given key in the cache.
	Set(context.Context, string, []byte, time.Duration) error
	// Delete removes the value associated with the given key from the cache.
	Delete(context.Context, ...string) (int64, error)
}

// InvalidationState reports an atomic versioned backend decision.
type InvalidationState uint8

const (
	// InvalidationApplied means a newer version replaced the watermark and
	// deleted the cached value.
	InvalidationApplied InvalidationState = iota + 1
	// InvalidationCurrent means the same version was already applied.
	InvalidationCurrent
	// InvalidationStale means a newer version had already been observed.
	InvalidationStale
)

// VersionedBackend prevents a loader from writing after another replica has
// atomically advanced a key's invalidation watermark.
type VersionedBackend interface {
	Backend // Backend is the minimal binary cache storage contract.
	// CurrentVersion returns the current version of the given key.
	CurrentVersion(context.Context, string) (uint64, error)
	// SetIfVersion stores the value associated with the given key in the cache if the version matches.
	SetIfVersion(context.Context, string, []byte, time.Duration, uint64) (bool, error)
	// ApplyInvalidation applies an invalidation state to the given key.
	ApplyInvalidation(context.Context, string, uint64) (InvalidationState, error)
}

// Loader obtains a value when no valid cache entry exists.
type Loader[T any] func(context.Context, string) (T, error)

// Random supplies cache TTL jitter values in [0, 1).
type Random interface {
	// Float64 returns a random float64 value in [0, 1).
	Float64() float64
}

// Cache is a typed read-through cache with request coalescing.
type Cache[T any] struct {
	backend Backend            // backend is the backend used to store cache values.
	version VersionedBackend   // version is the versioned backend used to store cache values.
	codec   Codec[T]           // codec is the codec used to serialize/deserialize values.
	loader  Loader[T]          // loader is the function used to load cache values.
	policy  Policy             // policy is the cache policy.
	random  Random             // random is used to generate random values.
	group   singleflight.Group // group is used to coalesce cache load requests.

	mutations              [mutationStripes]mutationStripe // mutationStripes is the number of stripes used for cache mutation.
	staleWritesSuppressed  atomic.Uint64                   // number of stale writes suppressed
	invalidations          atomic.Uint64                   // number of invalidations
	observedInvalidations  atomic.Uint64                   // number of observed invalidations
	versionedInvalidations atomic.Uint64                   // number of versioned invalidations
	currentInvalidations   atomic.Uint64                   // number of current invalidations
	staleInvalidations     atomic.Uint64                   // number of stale invalidations
	versionFailures        atomic.Uint64                   // number of version failures
}

type loadResult[T any] struct {
	value T
}

type mutationStripe struct {
	mu         sync.Mutex
	generation atomic.Uint64
}

// Description is a value-free cache consistency snapshot.
type Description struct {
	StaleWritesSuppressed  uint64 // number of stale writes suppressed
	Invalidations          uint64 // number of invalidations
	ObservedInvalidations  uint64 // number of observed invalidations
	VersionedInvalidations uint64 // number of versioned invalidations
	CurrentInvalidations   uint64 // number of current invalidations
	StaleInvalidations     uint64 // number of stale invalidations
	VersionFailures        uint64 // number of version failures
}

// New creates a typed read-through cache.
func New[T any](backend Backend, codec Codec[T], loader Loader[T], policy Policy, options ...Option) (*Cache[T], error) {
	if isNil(backend) || isNil(codec) || loader == nil {
		return nil, fmt.Errorf("%w: backend, codec, and loader are required", ErrInvalidOption)
	}
	if err := validatePolicy(policy); err != nil {
		return nil, err
	}
	settings := settings{random: packageRandom{}}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}

	result := &Cache[T]{
		backend: backend,
		codec:   codec,
		loader:  loader,
		policy:  policy,
		random:  settings.random,
	}
	if settings.versioning {
		versioned, ok := backend.(VersionedBackend)
		if !ok || isNil(versioned) {
			return nil, ErrVersioningUnsupported
		}
		result.version = versioned
	}
	return result, nil
}

// Get returns a cached value or coalesces concurrent loads for key.
func (c *Cache[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	if c == nil {
		return zero, fmt.Errorf("%w: cache is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return zero, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := validateKey(key); err != nil {
		return zero, err
	}
	value, hit, err := c.read(ctx, key)
	if hit {
		return value, err
	}
	if err != nil && !c.policy.FailOpen {
		return zero, err
	}

	resultChannel := c.group.DoChan(key, func() (any, error) {
		generation := c.generation(key)
		// Recheck after becoming the singleflight leader. Another request may
		// have populated the key between the first read and this point.
		cached, cachedHit, cachedErr := c.read(ctx, key)
		if cachedHit {
			return loadResult[T]{value: cached}, cachedErr
		}
		if cachedErr != nil && !c.policy.FailOpen {
			return loadResult[T]{}, cachedErr
		}
		version, versionKnown, versionErr := c.currentVersion(ctx, key)
		if versionErr != nil && !c.policy.FailOpen {
			return loadResult[T]{}, versionErr
		}

		loaded, loadErr := c.loader(ctx, key)
		if loadErr != nil {
			if c.policy.IsNegative != nil && c.policy.IsNegative(loadErr) && c.policy.NegativeTTL > 0 {
				writeErr := c.storeNegativeIfCurrent(ctx, key, generation, version, versionKnown)
				if writeErr != nil && !c.policy.FailOpen {
					return loadResult[T]{}, errors.Join(loadErr, writeErr)
				}
			}
			return loadResult[T]{}, loadErr
		}
		if writeErr := c.storeIfCurrent(ctx, key, loaded, generation, version, versionKnown); writeErr != nil &&
			!c.policy.FailOpen {
			return loadResult[T]{}, writeErr
		}
		return loadResult[T]{value: loaded}, nil
	})

	select {
	case <-ctx.Done():
		return zero, context.Cause(ctx)
	case result := <-resultChannel:
		if result.Err != nil {
			return zero, result.Err
		}
		loaded, ok := result.Val.(loadResult[T])
		if !ok {
			return zero, fmt.Errorf("%w: invalid coalesced result", ErrCorrupt)
		}
		return loaded.value, nil
	}
}

// InvalidateVersion applies a monotonic invalidation watermark and suppresses
// local in-flight loaders. Equal versions are locally observed but do not
// delete a freshly repopulated backend value; stale versions are ignored.
func (c *Cache[T]) InvalidateVersion(ctx context.Context, key string, version uint64) (InvalidationState, error) {
	if c == nil {
		return 0, fmt.Errorf("%w: cache is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return 0, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if version == 0 {
		return 0, fmt.Errorf("%w: version is zero", ErrInvalidOption)
	}
	if c.version == nil {
		return 0, ErrVersioningUnsupported
	}
	state, err := c.version.ApplyInvalidation(ctx, key, version)
	if err != nil {
		c.versionFailures.Add(1)
		return 0, err
	}
	switch state {
	case InvalidationApplied:
		c.versionedInvalidations.Add(1)
	case InvalidationCurrent:
		c.currentInvalidations.Add(1)
	case InvalidationStale:
		c.staleInvalidations.Add(1)
		return state, nil
	default:
		c.versionFailures.Add(1)
		return 0, fmt.Errorf("%w: invalid backend invalidation state", ErrInvalidOption)
	}
	stripe := c.stripe(key)
	stripe.mu.Lock()
	stripe.generation.Add(1)
	c.group.Forget(key)
	stripe.mu.Unlock()
	return state, nil
}

// Set stores one value using the configured positive TTL.
func (c *Cache[T]) Set(ctx context.Context, key string, value T) error {
	if c == nil {
		return fmt.Errorf("%w: cache is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := validateKey(key); err != nil {
		return err
	}
	stripe := c.stripe(key)
	stripe.mu.Lock()
	defer stripe.mu.Unlock()
	stripe.generation.Add(1)
	c.group.Forget(key)
	return c.store(ctx, key, value)
}

// Invalidate deletes one or more keys.
func (c *Cache[T]) Invalidate(ctx context.Context, keys ...string) (int64, error) {
	if c == nil {
		return 0, fmt.Errorf("%w: cache is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return 0, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if len(keys) == 0 {
		return 0, fmt.Errorf("%w: no keys", ErrInvalidOption)
	}
	if c.version != nil {
		return 0, ErrVersionRequired
	}
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return 0, err
		}
	}
	stripes := c.lockStripes(keys)
	defer unlockStripes(stripes)
	for _, stripe := range stripes {
		stripe.generation.Add(1)
	}
	for _, key := range keys {
		c.group.Forget(key)
	}
	count, err := c.backend.Delete(ctx, keys...)
	c.invalidations.Add(uint64(len(keys)))
	return count, err
}

// ObserveInvalidation applies an invalidation event already committed by
// another replica. It does not delete the shared backend again or publish a
// new event.
//
// Publishers must delete the backend before broadcasting the event.
func (c *Cache[T]) ObserveInvalidation(keys ...string) error {
	if c == nil {
		return fmt.Errorf("%w: cache is nil", ErrInvalidOption)
	}
	if len(keys) == 0 {
		return fmt.Errorf("%w: no keys", ErrInvalidOption)
	}
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return err
		}
	}
	stripes := c.lockStripes(keys)
	defer unlockStripes(stripes)
	for _, stripe := range stripes {
		stripe.generation.Add(1)
	}
	for _, key := range keys {
		c.group.Forget(key)
	}
	c.observedInvalidations.Add(uint64(len(keys)))
	return nil
}

// Description returns consistency counters without cache keys or values.
func (c *Cache[T]) Description() Description {
	if c == nil {
		return Description{}
	}

	return Description{
		StaleWritesSuppressed:  c.staleWritesSuppressed.Load(),
		Invalidations:          c.invalidations.Load(),
		ObservedInvalidations:  c.observedInvalidations.Load(),
		VersionedInvalidations: c.versionedInvalidations.Load(),
		CurrentInvalidations:   c.currentInvalidations.Load(),
		StaleInvalidations:     c.staleInvalidations.Load(),
		VersionFailures:        c.versionFailures.Load(),
	}
}

func (c *Cache[T]) read(ctx context.Context, key string) (T, bool, error) {
	var zero T
	payload, err := c.backend.Get(ctx, key)
	if errors.Is(err, ErrMiss) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	if len(payload) == 0 {
		return zero, false, fmt.Errorf("%w: empty envelope", ErrCorrupt)
	}
	switch payload[0] {
	case envelopeNegative:
		return zero, true, c.policy.NegativeError
	case envelopeValue:
		value, decodeErr := c.codec.Decode(payload[1:])
		if decodeErr != nil {
			return zero, false, fmt.Errorf("%w: decode: %w", ErrCorrupt, decodeErr)
		}
		return value, true, nil
	default:
		return zero, false, fmt.Errorf("%w: unknown envelope %d", ErrCorrupt, payload[0])
	}
}

func (c *Cache[T]) store(ctx context.Context, key string, value T) error {
	payload, err := c.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("cache: encode: %w", err)
	}
	envelope := make([]byte, len(payload)+1)
	envelope[0] = envelopeValue
	copy(envelope[1:], payload)
	ttl := c.jitter(c.policy.TTL)
	if c.version == nil {
		if err := c.backend.Set(ctx, key, envelope, ttl); err != nil {
			return fmt.Errorf("cache: set: %w", err)
		}
		return nil
	}
	version, err := c.version.CurrentVersion(ctx, key)
	if err != nil {
		c.versionFailures.Add(1)
		return fmt.Errorf("cache: current version: %w", err)
	}
	stored, err := c.version.SetIfVersion(
		ctx,
		key,
		envelope,
		ttl,
		version,
	)
	if err != nil {
		return fmt.Errorf("cache: set: %w", err)
	}
	if !stored {
		c.staleWritesSuppressed.Add(1)
		return ErrStaleWrite
	}

	return nil
}

func (c *Cache[T]) storeIfCurrent(ctx context.Context, key string, value T, generation uint64, version uint64, versionKnown bool) error {
	stripe := c.stripe(key)
	stripe.mu.Lock()
	defer stripe.mu.Unlock()
	if stripe.generation.Load() != generation {
		c.staleWritesSuppressed.Add(1)
		return nil
	}
	payload, err := c.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("cache: encode: %w", err)
	}
	envelope := make([]byte, len(payload)+1)
	envelope[0] = envelopeValue
	copy(envelope[1:], payload)

	return c.storeEnvelopeIfVersion(
		ctx,
		key,
		envelope,
		c.jitter(c.policy.TTL),
		version,
		versionKnown,
	)
}

func (c *Cache[T]) storeNegativeIfCurrent(ctx context.Context, key string, generation uint64, version uint64, versionKnown bool) error {
	stripe := c.stripe(key)
	stripe.mu.Lock()
	defer stripe.mu.Unlock()
	if stripe.generation.Load() != generation {
		c.staleWritesSuppressed.Add(1)
		return nil
	}

	return c.storeEnvelopeIfVersion(
		ctx,
		key,
		[]byte{envelopeNegative},
		c.jitter(c.policy.NegativeTTL),
		version,
		versionKnown,
	)
}

func (c *Cache[T]) currentVersion(ctx context.Context, key string) (uint64, bool, error) {
	if c.version == nil {
		return 0, false, nil
	}
	version, err := c.version.CurrentVersion(ctx, key)
	if err != nil {
		c.versionFailures.Add(1)
		return 0, false, fmt.Errorf("cache: current version: %w", err)
	}
	return version, true, nil
}

func (c *Cache[T]) storeEnvelopeIfVersion(ctx context.Context, key string, envelope []byte, ttl time.Duration, version uint64, versionKnown bool) error {
	if c.version == nil {
		return c.backend.Set(ctx, key, envelope, ttl)
	}
	if !versionKnown {
		// A fail-open read may still return the loader value, but writing
		// without a known watermark could resurrect data deleted remotely.
		return nil
	}
	stored, err := c.version.SetIfVersion(ctx, key, envelope, ttl, version)
	if err != nil {
		return err
	}
	if !stored {
		c.staleWritesSuppressed.Add(1)
	}

	return nil
}

// generation returns the generation of the mutation stripe for the given key.
func (c *Cache[T]) generation(key string) uint64 {
	return c.stripe(key).generation.Load()
}

// stripe returns the mutation stripe for the given key.
func (c *Cache[T]) stripe(key string) *mutationStripe {
	return &c.mutations[mutationStripeIndex(key)]
}

// lockStripes locks the mutation stripes for the given keys and returns them in order.
func (c *Cache[T]) lockStripes(keys []string) []*mutationStripe {
	indexSet := make(map[int]struct{}, len(keys))
	for _, key := range keys {
		indexSet[mutationStripeIndex(key)] = struct{}{}
	}
	indices := make([]int, 0, len(indexSet))
	for index := range indexSet {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	stripes := make([]*mutationStripe, len(indices))
	for position, index := range indices {
		stripe := &c.mutations[index]
		stripe.mu.Lock()
		stripes[position] = stripe
	}
	return stripes
}

// unlockStripes unlocks the given stripes in reverse order.
func unlockStripes(stripes []*mutationStripe) {
	for index := len(stripes) - 1; index >= 0; index-- {
		stripes[index].mu.Unlock()
	}
}

// mutationStripeIndex returns the index of the mutation stripe for the given key.
func mutationStripeIndex(key string) int {
	var hash uint32 = 2166136261
	for index := range len(key) {
		hash ^= uint32(key[index])
		hash *= 16777619
	}
	return int(hash % mutationStripes)
}

func (c *Cache[T]) jitter(ttl time.Duration) time.Duration {
	if c.policy.JitterRatio == 0 {
		return ttl
	}
	ratio := c.random.Float64()
	if ratio < 0 {
		ratio = 0
	}
	if ratio >= 1 {
		ratio = 0.999999999999
	}
	return ttl - time.Duration(
		float64(ttl)*c.policy.JitterRatio*ratio,
	)
}

func validatePolicy(policy Policy) error {
	if policy.TTL <= 0 {
		return fmt.Errorf("%w: TTL must be positive", ErrInvalidOption)
	}
	if policy.NegativeTTL < 0 {
		return fmt.Errorf("%w: negative TTL is negative", ErrInvalidOption)
	}
	if policy.JitterRatio < 0 || policy.JitterRatio >= 1 {
		return fmt.Errorf("%w: jitter ratio must be in [0, 1)", ErrInvalidOption)
	}
	if policy.NegativeTTL > 0 {
		if policy.IsNegative == nil || policy.NegativeError == nil {
			return fmt.Errorf("%w: negative caching requires classifier and stable error", ErrInvalidOption)
		}
	}
	return nil
}

func validateKey(key string) error {
	if key == "" || len(key) > maxCacheKeyBytes || strings.TrimSpace(key) != key || !utf8.ValidString(key) {
		return fmt.Errorf("%w: cache key is invalid", ErrInvalidOption)
	}
	for _, character := range key {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: cache key is invalid", ErrInvalidOption)
		}
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type packageRandom struct{}

func (packageRandom) Float64() float64 {
	return rand.Float64()
}
