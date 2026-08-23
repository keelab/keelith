package projection

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"
)

const maxShardIDBytes = 128

var (
	// ErrInvalidShard reports an empty, duplicated, or unbounded shard identity.
	ErrInvalidShard = errors.New("projection: invalid shard")
	// ErrShardUnavailable reports that a resolved shard has no readable replica.
	ErrShardUnavailable = errors.New("projection: shard unavailable")
	// ErrShardGeneration reports a cursor from another shard generation.
	ErrShardGeneration = errors.New("projection: shard generation mismatch")
)

// ShardID is a stable, payload-free partition identity.
type ShardID string

// Validate rejects ambiguous or unbounded shard identities.
func (id ShardID) Validate() error {
	if !validIdentity(string(id), maxShardIDBytes) {
		return fmt.Errorf("%w: %q", ErrInvalidShard, id)
	}
	return nil
}

// ShardCheckpoint is one shard's independent durable watermark.
type ShardCheckpoint struct {
	Shard      ShardID
	Cursor     Cursor
	Floor      Cursor
	Generation uint64
	SourceTime time.Time
	AppliedAt  time.Time
}

// ShardResolver maps one typed key to exactly one shard.
//
// KeyFingerprint prevents a resolver compiled for a different key encoding
// from silently routing reads to the wrong shard.
type ShardResolver[K any] interface {
	Resolve(K) (ShardID, error)
	Shards() []ShardID
	KeyFingerprint() string
}

// HashShardResolver uses stable rendezvous hashing. Adding or removing one
// shard only remaps keys won by or previously owned by that shard.
type HashShardResolver[K any] struct {
	keyFingerprint string
	encode         KeyEncoder[K]
	shards         []ShardID
}

// NewHashShardResolver constructs an immutable stable hash resolver.
func NewHashShardResolver[K any](
	keyFingerprint string,
	encode KeyEncoder[K],
	shards ...ShardID,
) (*HashShardResolver[K], error) {
	if !validFingerprint(keyFingerprint) || encode == nil {
		return nil, fmt.Errorf("%w: key contract", ErrInvalidShard)
	}
	canonical, err := canonicalShards(shards)
	if err != nil {
		return nil, err
	}
	return &HashShardResolver[K]{
		keyFingerprint: keyFingerprint,
		encode:         encode,
		shards:         canonical,
	}, nil
}

// Resolve returns exactly one stable shard for key.
func (resolver *HashShardResolver[K]) Resolve(key K) (ShardID, error) {
	if resolver == nil || resolver.encode == nil || len(resolver.shards) == 0 {
		return "", ErrInvalidShard
	}
	encoded, err := resolver.encode(key)
	if err != nil {
		return "", fmt.Errorf("projection: encode shard key: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maxKeyBytes {
		return "", fmt.Errorf("%w: encoded key", ErrInvalidShard)
	}
	winner := resolver.shards[0]
	winnerScore := shardScore(encoded, winner)
	for _, shard := range resolver.shards[1:] {
		score := shardScore(encoded, shard)
		if score > winnerScore {
			winner = shard
			winnerScore = score
		}
	}
	return winner, nil
}

// Shards returns an independent canonical shard list.
func (resolver *HashShardResolver[K]) Shards() []ShardID {
	if resolver == nil {
		return nil
	}
	return append([]ShardID(nil), resolver.shards...)
}

// KeyFingerprint returns the immutable key encoding contract.
func (resolver *HashShardResolver[K]) KeyFingerprint() string {
	if resolver == nil {
		return ""
	}
	return resolver.keyFingerprint
}

// RangeBoundary routes keys less than Upper to Shard. Boundaries must be
// strictly increasing; keys at or above the final boundary use the fallback.
type RangeBoundary[K any] struct {
	Upper K
	Shard ShardID
}

// RangeShardResolver routes ordered key spaces without inspecting values.
type RangeShardResolver[K any] struct {
	keyFingerprint string
	compare        func(K, K) int
	boundaries     []RangeBoundary[K]
	fallback       ShardID
	shards         []ShardID
}

// NewRangeShardResolver constructs an immutable validated range resolver.
func NewRangeShardResolver[K any](
	keyFingerprint string,
	compare func(K, K) int,
	boundaries []RangeBoundary[K],
	fallback ShardID,
) (*RangeShardResolver[K], error) {
	if !validFingerprint(keyFingerprint) ||
		compare == nil ||
		len(boundaries) == 0 ||
		len(boundaries) > 4096 ||
		fallback.Validate() != nil {
		return nil, fmt.Errorf("%w: range contract", ErrInvalidShard)
	}
	copyBoundaries := append([]RangeBoundary[K](nil), boundaries...)
	ids := make([]ShardID, 0, len(boundaries)+1)
	for index, boundary := range copyBoundaries {
		if boundary.Shard.Validate() != nil ||
			index > 0 && compare(copyBoundaries[index-1].Upper, boundary.Upper) >= 0 {
			return nil, fmt.Errorf("%w: range boundary %d", ErrInvalidShard, index)
		}
		ids = append(ids, boundary.Shard)
	}
	ids = append(ids, fallback)
	canonical, err := canonicalShards(ids)
	if err != nil {
		return nil, err
	}
	return &RangeShardResolver[K]{
		keyFingerprint: keyFingerprint,
		compare:        compare,
		boundaries:     copyBoundaries,
		fallback:       fallback,
		shards:         canonical,
	}, nil
}

// Resolve returns the first matching range or the explicit fallback shard.
func (resolver *RangeShardResolver[K]) Resolve(key K) (ShardID, error) {
	if resolver == nil || resolver.compare == nil {
		return "", ErrInvalidShard
	}
	for _, boundary := range resolver.boundaries {
		if resolver.compare(key, boundary.Upper) < 0 {
			return boundary.Shard, nil
		}
	}
	return resolver.fallback, nil
}

// Shards returns an independent canonical shard list.
func (resolver *RangeShardResolver[K]) Shards() []ShardID {
	if resolver == nil {
		return nil
	}
	return append([]ShardID(nil), resolver.shards...)
}

// KeyFingerprint returns the immutable key encoding contract.
func (resolver *RangeShardResolver[K]) KeyFingerprint() string {
	if resolver == nil {
		return ""
	}
	return resolver.keyFingerprint
}

func canonicalShards(values []ShardID) ([]ShardID, error) {
	if len(values) == 0 || len(values) > 4096 {
		return nil, fmt.Errorf("%w: shard count", ErrInvalidShard)
	}
	result := append([]ShardID(nil), values...)
	sort.Slice(result, func(first, second int) bool {
		return result[first] < result[second]
	})
	for index, shard := range result {
		if shard.Validate() != nil || index > 0 && result[index-1] == shard {
			return nil, fmt.Errorf("%w: shard identity", ErrInvalidShard)
		}
	}
	return result, nil
}

func shardScore(key []byte, shard ShardID) uint64 {
	digest := sha256.New()
	_, _ = digest.Write(key)
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(shard))
	return binary.BigEndian.Uint64(digest.Sum(nil)[:8])
}
