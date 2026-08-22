package ownermemory

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/programmable/projection"
)

var _ projection.OwnerAdmin = (*Source)(nil)

// ProjectionStats returns payload-free capacity and protected checkpoint data.
func (source *Source) ProjectionStats(
	ctx context.Context,
	shard projection.ShardID,
) (projection.ShardStats, error) {
	if source == nil || ctx == nil || shard != projection.DefaultShard {
		return projection.ShardStats{}, projection.ErrInvalidAdmin
	}
	if cause := context.Cause(ctx); cause != nil {
		return projection.ShardStats{}, cause
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return projection.ShardStats{}, ErrSourceClosed
	}
	if source.offset == 0 {
		return projection.ShardStats{}, ErrNotSeeded
	}
	rows, rowBytes := uint64(len(source.rows)), uint64(0)
	for key, value := range source.rows {
		rowBytes += uint64(len(key) + len(value))
	}
	logBytes := uint64(0)
	for _, entry := range source.log {
		for _, mutation := range entry.batch.Mutations {
			logBytes += uint64(len(mutation.Key()) + len(mutation.Value()))
		}
	}
	protected := source.protectedOffsetLocked()
	return projection.ShardStats{
		Projection:        source.schema.ID,
		Shard:             shard,
		Rows:              rows,
		RowBytes:          rowBytes,
		LogBytes:          logBytes,
		Head:              encodeCursor(source.offset),
		Floor:             encodeCursor(source.retentionFloorLocked()),
		Protected:         encodeCursor(protected),
		HeadOffset:        source.offset,
		FloorOffset:       source.retentionFloorLocked(),
		ProtectedOffset:   protected,
		Generation:        source.offset,
		ActiveSubscribers: uint64(len(source.subscribers)),
	}, nil
}

// CompactProjection removes a bounded number of replay entries no newer than
// the oldest active subscriber checkpoint.
func (source *Source) CompactProjection(
	ctx context.Context,
	request projection.CompactRequest,
) (projection.CompactResult, error) {
	if source == nil || ctx == nil || request.Validate() != nil ||
		request.Shard != projection.DefaultShard {
		return projection.CompactResult{}, projection.ErrInvalidAdmin
	}
	if cause := context.Cause(ctx); cause != nil {
		return projection.CompactResult{}, cause
	}
	target, err := decodeCursor(request.Before)
	if err != nil {
		return projection.CompactResult{}, projection.ErrInvalidAdmin
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return projection.CompactResult{}, ErrSourceClosed
	}
	if source.offset == 0 {
		return projection.CompactResult{}, ErrNotSeeded
	}
	if target > source.offset {
		return projection.CompactResult{}, fmt.Errorf(
			"%w: target is ahead of head",
			projection.ErrInvalidAdmin,
		)
	}
	previousFloor := source.retentionFloorLocked()
	protected := source.protectedOffsetLocked()
	if target > protected {
		target = protected
	}
	deleted := 0
	for deleted < len(source.log) && deleted < request.MaxEntries &&
		source.log[deleted].offset <= target {
		deleted++
	}
	if deleted > 0 {
		source.log = append([]logEntry(nil), source.log[deleted:]...)
	}
	return projection.CompactResult{
		Shard:          request.Shard,
		PreviousFloor:  encodeCursor(previousFloor),
		Floor:          encodeCursor(source.retentionFloorLocked()),
		Protected:      encodeCursor(protected),
		DeletedEntries: uint64(deleted),
	}, nil
}

// ForceProjectionSnapshot advances replay retention as far as safety permits;
// older clients recover through the existing GAP -> forced snapshot path.
func (source *Source) ForceProjectionSnapshot(
	ctx context.Context,
	shard projection.ShardID,
) (projection.ForcedSnapshot, error) {
	stats, err := source.ProjectionStats(ctx, shard)
	if err != nil {
		return projection.ForcedSnapshot{}, err
	}
	result, err := source.CompactProjection(ctx, projection.CompactRequest{
		Shard:      shard,
		Before:     stats.Head,
		MaxEntries: 100_000,
	})
	if err != nil {
		return projection.ForcedSnapshot{}, err
	}
	return projection.ForcedSnapshot{
		Shard:      shard,
		Head:       stats.Head,
		Floor:      result.Floor,
		Protected:  result.Protected,
		Generation: stats.Generation,
	}, nil
}

func (source *Source) protectedOffsetLocked() uint64 {
	protected := source.offset
	for subscriber := range source.subscribers {
		if subscriber.protected < protected {
			protected = subscriber.protected
		}
	}
	return protected
}
