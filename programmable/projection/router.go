package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidShardRouter reports mismatched stores, resolver, or schema.
	ErrInvalidShardRouter = errors.New("projection: invalid shard router")
	// ErrStaleShardRoute reports a non-monotonic resolver revision.
	ErrStaleShardRoute = errors.New("projection: stale shard route")
)

// ShardStatusCode is a fixed-cardinality availability class.
type ShardStatusCode uint8

const (
	// ShardStatusReady means the shard checkpoint was read successfully.
	ShardStatusReady ShardStatusCode = iota + 1
	// ShardStatusEmpty means the shard has no visible generation yet.
	ShardStatusEmpty
	// ShardStatusUnavailable means the shard has no store or the store failed.
	ShardStatusUnavailable
)

// ShardFreshness is a payload-free per-shard fan-out result.
type ShardFreshness struct {
	Checkpoint ShardCheckpoint
	Status     ShardStatusCode
}

// ReplayFloorStore optionally exposes the oldest resumable cursor.
type ReplayFloorStore interface {
	ReplayFloor(context.Context, ProjectionID) (Cursor, bool, error)
}

// ShardedReplica performs one-shard typed reads over existing Store contracts.
// A nil Store marks that shard unavailable without making other shards unreadable.
type ShardedReplica[K, V any] struct {
	schema   Schema
	resolver ShardResolver[K]
	stores   map[ShardID]Store
	replicas map[ShardID]*Replica[K, V]
}

// NewShardedReplica constructs one typed replica per available shard.
func NewShardedReplica[K, V any](
	schema Schema,
	stores map[ShardID]Store,
	resolver ShardResolver[K],
	encodeKey KeyEncoder[K],
	decode ValueDecoder[V],
	options ...ReplicaOption,
) (*ShardedReplica[K, V], error) {
	return NewShardedReplicaWithCompatibleDecoders(
		schema,
		stores,
		resolver,
		encodeKey,
		decode,
		nil,
		options...,
	)
}

// NewShardedReplicaWithCompatibleDecoders constructs shard replicas with the
// same explicit historical decoder registry used by the unsharded Replica.
func NewShardedReplicaWithCompatibleDecoders[K, V any](
	schema Schema,
	stores map[ShardID]Store,
	resolver ShardResolver[K],
	encodeKey KeyEncoder[K],
	current ValueDecoder[V],
	compatible map[string]ValueDecoder[V],
	options ...ReplicaOption,
) (*ShardedReplica[K, V], error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if resolver == nil || resolver.KeyFingerprint() != schema.KeyFingerprint ||
		encodeKey == nil || current == nil {
		return nil, fmt.Errorf("%w: schema or resolver", ErrInvalidShardRouter)
	}
	shards, err := canonicalShards(resolver.Shards())
	if err != nil || len(stores) != len(shards) {
		return nil, fmt.Errorf("%w: shard set", ErrInvalidShardRouter)
	}
	result := &ShardedReplica[K, V]{
		schema:   schema,
		resolver: resolver,
		stores:   make(map[ShardID]Store, len(stores)),
		replicas: make(map[ShardID]*Replica[K, V], len(stores)),
	}
	for _, shard := range shards {
		store, exists := stores[shard]
		if !exists {
			return nil, fmt.Errorf("%w: missing shard", ErrInvalidShardRouter)
		}
		result.stores[shard] = store
		if isNilInterface(store) {
			continue
		}
		replica, replicaErr := NewReplicaWithCompatibleDecoders(
			schema,
			store,
			encodeKey,
			current,
			compatible,
			options...,
		)
		if replicaErr != nil {
			return nil, replicaErr
		}
		result.replicas[shard] = replica
	}
	return result, nil
}

// Resolve returns the single shard selected for key.
func (replica *ShardedReplica[K, V]) Resolve(key K) (ShardID, error) {
	if replica == nil || replica.resolver == nil {
		return "", ErrInvalidShardRouter
	}
	shard, err := replica.resolver.Resolve(key)
	if err != nil {
		return "", err
	}
	if _, exists := replica.stores[shard]; !exists {
		return "", fmt.Errorf("%w: resolver returned unknown shard", ErrInvalidShardRouter)
	}
	return shard, nil
}

// Get resolves key once and reads exactly one shard.
func (replica *ShardedReplica[K, V]) Get(
	ctx context.Context,
	key K,
	options ...ReadOption,
) (V, bool, error) {
	var zero V
	shard, err := replica.Resolve(key)
	if err != nil {
		return zero, false, err
	}
	reader := replica.replicas[shard]
	if reader == nil {
		return zero, false, fmt.Errorf("%w: %q", ErrShardUnavailable, shard)
	}
	return reader.Get(ctx, key, options...)
}

// WaitUntil resolves key once and waits on only that shard. A non-zero target
// generation must equal the reached durable generation.
func (replica *ShardedReplica[K, V]) WaitUntil(
	ctx context.Context,
	key K,
	target ShardCheckpoint,
) (ShardCheckpoint, error) {
	shard, err := replica.Resolve(key)
	if err != nil {
		return ShardCheckpoint{}, err
	}
	if target.Shard != shard {
		return ShardCheckpoint{}, fmt.Errorf("%w: routed shard", ErrShardGeneration)
	}
	reader := replica.replicas[shard]
	if reader == nil {
		return ShardCheckpoint{}, fmt.Errorf("%w: %q", ErrShardUnavailable, shard)
	}
	checkpoint, err := reader.WaitUntil(ctx, target.Cursor)
	if err != nil {
		return ShardCheckpoint{}, err
	}
	if target.Generation != 0 && target.Generation != checkpoint.Generation {
		return ShardCheckpoint{}, fmt.Errorf("%w: %q", ErrShardGeneration, shard)
	}
	return shardCheckpoint(ctx, shard, replica.stores[shard], checkpoint), nil
}

// FanOutFreshness explicitly reads every shard checkpoint. Individual shard
// failures are represented by fixed status codes and do not hide healthy data.
func (replica *ShardedReplica[K, V]) FanOutFreshness(
	ctx context.Context,
) ([]ShardFreshness, error) {
	if replica == nil || replica.resolver == nil {
		return nil, ErrInvalidShardRouter
	}
	if err := validateReplicaContext(ctx); err != nil {
		return nil, err
	}
	shards := replica.resolver.Shards()
	result := make([]ShardFreshness, 0, len(shards))
	for _, shard := range shards {
		store := replica.stores[shard]
		if isNilInterface(store) {
			result = append(result, ShardFreshness{
				Checkpoint: ShardCheckpoint{Shard: shard},
				Status:     ShardStatusUnavailable,
			})
			continue
		}
		checkpoint, exists, err := store.Checkpoint(ctx, replica.schema.ID)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			result = append(result, ShardFreshness{
				Checkpoint: ShardCheckpoint{Shard: shard},
				Status:     ShardStatusUnavailable,
			})
			continue
		}
		if !exists {
			result = append(result, ShardFreshness{
				Checkpoint: ShardCheckpoint{Shard: shard},
				Status:     ShardStatusEmpty,
			})
			continue
		}
		if err := requireMatchingSchema(replica.schema, checkpoint.Schema); err != nil {
			result = append(result, ShardFreshness{
				Checkpoint: ShardCheckpoint{Shard: shard},
				Status:     ShardStatusUnavailable,
			})
			continue
		}
		result = append(result, ShardFreshness{
			Checkpoint: shardCheckpoint(ctx, shard, store, checkpoint),
			Status:     ShardStatusReady,
		})
	}
	return result, nil
}

func shardCheckpoint(
	ctx context.Context,
	shard ShardID,
	store Store,
	checkpoint Checkpoint,
) ShardCheckpoint {
	result := ShardCheckpoint{
		Shard:      shard,
		Cursor:     checkpoint.Cursor,
		Generation: checkpoint.Generation,
		SourceTime: checkpoint.SourceTime,
		AppliedAt:  checkpoint.AppliedAt,
	}
	if floors, ok := store.(ReplayFloorStore); ok {
		floor, exists, err := floors.ReplayFloor(ctx, checkpoint.Schema.ID)
		if err == nil && exists {
			result.Floor = floor
		}
	}
	return result
}

// Router atomically swaps immutable sharded replicas at monotonic revisions.
// In-flight reads retain the replica snapshot they acquired before a move.
type Router[K, V any] struct {
	mu       sync.RWMutex
	revision uint64
	replica  *ShardedReplica[K, V]
}

// NewRouter constructs a typed router at a positive control-plane revision.
func NewRouter[K, V any](
	revision uint64,
	replica *ShardedReplica[K, V],
) (*Router[K, V], error) {
	if revision == 0 || replica == nil {
		return nil, ErrInvalidShardRouter
	}
	return &Router[K, V]{revision: revision, replica: replica}, nil
}

// Update atomically installs a newer resolver/store generation.
func (router *Router[K, V]) Update(
	revision uint64,
	replica *ShardedReplica[K, V],
) error {
	if router == nil || replica == nil || revision == 0 {
		return ErrInvalidShardRouter
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if revision <= router.revision {
		return ErrStaleShardRoute
	}
	router.revision = revision
	router.replica = replica
	return nil
}

// Revision returns the currently installed routing revision.
func (router *Router[K, V]) Revision() uint64 {
	if router == nil {
		return 0
	}
	router.mu.RLock()
	defer router.mu.RUnlock()
	return router.revision
}

// Get delegates to one immutable routing snapshot.
func (router *Router[K, V]) Get(
	ctx context.Context,
	key K,
	options ...ReadOption,
) (V, bool, error) {
	replica, err := router.current()
	if err != nil {
		var zero V
		return zero, false, err
	}
	return replica.Get(ctx, key, options...)
}

// WaitUntil delegates to exactly one resolved shard.
func (router *Router[K, V]) WaitUntil(
	ctx context.Context,
	key K,
	target ShardCheckpoint,
) (ShardCheckpoint, error) {
	replica, err := router.current()
	if err != nil {
		return ShardCheckpoint{}, err
	}
	return replica.WaitUntil(ctx, key, target)
}

// FanOutFreshness explicitly queries all configured shards.
func (router *Router[K, V]) FanOutFreshness(
	ctx context.Context,
) ([]ShardFreshness, error) {
	replica, err := router.current()
	if err != nil {
		return nil, err
	}
	return replica.FanOutFreshness(ctx)
}

func (router *Router[K, V]) current() (*ShardedReplica[K, V], error) {
	if router == nil {
		return nil, ErrInvalidShardRouter
	}
	router.mu.RLock()
	replica := router.replica
	router.mu.RUnlock()
	if replica == nil {
		return nil, ErrInvalidShardRouter
	}
	return replica, nil
}
