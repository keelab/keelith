package projection

import (
	"context"
	"errors"
	"fmt"
)

const maxCompactEntries = 100_000

var (
	// ErrInvalidAdmin reports malformed shard or compaction requests.
	ErrInvalidAdmin = errors.New("projection: invalid admin request")
	// ErrUnsafeCompaction reports an attempt to pass a protected subscriber.
	ErrUnsafeCompaction = errors.New("projection: unsafe compaction")
)

// DefaultShard identifies an unsharded v1 owner through the v2 admin API.
const DefaultShard ShardID = "default"

// ShardStats is a payload-free owner capacity and freshness snapshot.
type ShardStats struct {
	Projection        ProjectionID
	Shard             ShardID
	Rows              uint64
	RowBytes          uint64
	LogBytes          uint64
	Head              Cursor
	Floor             Cursor
	Protected         Cursor
	HeadOffset        uint64
	FloorOffset       uint64
	ProtectedOffset   uint64
	Generation        uint64
	ActiveSubscribers uint64
}

// CompactRequest asks an owner to advance one replay floor without crossing
// the oldest active subscriber checkpoint.
type CompactRequest struct {
	Shard      ShardID
	Before     Cursor
	MaxEntries int
}

// Validate checks transport-neutral request bounds.
func (request CompactRequest) Validate() error {
	if request.Shard.Validate() != nil || request.Before.Validate() != nil ||
		request.MaxEntries <= 0 || request.MaxEntries > maxCompactEntries {
		return ErrInvalidAdmin
	}
	return nil
}

// CompactResult reports only cursor boundaries and deleted entry count.
type CompactResult struct {
	Shard          ShardID
	PreviousFloor  Cursor
	Floor          Cursor
	Protected      Cursor
	DeletedEntries uint64
}

// ForcedSnapshot reports the new resume floor used to force stale clients
// through the ordinary GAP -> snapshot recovery path.
type ForcedSnapshot struct {
	Shard      ShardID
	Head       Cursor
	Floor      Cursor
	Protected  Cursor
	Generation uint64
}

// OwnerAdmin is the storage-neutral projection maintenance contract.
type OwnerAdmin interface {
	ProjectionStats(context.Context, ShardID) (ShardStats, error)
	CompactProjection(context.Context, CompactRequest) (CompactResult, error)
	ForceProjectionSnapshot(context.Context, ShardID) (ForcedSnapshot, error)
}

// Validate ensures an implementation returned bounded, internally ordered
// payload-free stats. Cursor ordering remains implementation-specific.
func (stats ShardStats) Validate() error {
	if stats.Projection.Validate() != nil || stats.Shard.Validate() != nil ||
		stats.Head.Validate() != nil || stats.Floor.Validate() != nil ||
		stats.Protected.Validate() != nil || stats.Generation == 0 ||
		stats.Floor == "" || stats.Protected == "" ||
		stats.FloorOffset > stats.HeadOffset ||
		stats.ProtectedOffset > stats.HeadOffset {
		return fmt.Errorf("%w: shard stats", ErrInvalidAdmin)
	}
	return nil
}
