// Package projection stores projection owner state in PostgreSQL.
package projection

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"math"

	core "github.com/keelab/keelith/programmable/projection"
)

var _ core.OwnerAdmin = (*Source)(nil)

// ProjectionStats reads one repeatable, payload-free capacity snapshot.
func (source *Source) ProjectionStats(
	ctx context.Context,
	shard core.ShardID,
) (core.ShardStats, error) {
	if shard != core.DefaultShard {
		return core.ShardStats{}, core.ErrInvalidAdmin
	}
	if err := source.validate(ctx); err != nil {
		return core.ShardStats{}, err
	}
	transaction, err := source.database.BeginTx(ctx, &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return core.ShardStats{}, fmt.Errorf(
			"postgres projection: begin stats: %w",
			err,
		)
	}
	defer func() { _ = transaction.Rollback() }()
	meta, err := source.readMeta(ctx, transaction, false)
	if err != nil {
		return core.ShardStats{}, err
	}
	var rows, rowBytes, logBytes int64
	if err := transaction.QueryRowContext(
		ctx,
		`/*projection:admin-stats*/ SELECT
  count(*),
  COALESCE(sum(octet_length(row_key) + octet_length(payload)), 0)
FROM `+source.tables.rows+`
WHERE projection_id = $1`,
		string(source.schema.ID),
	).Scan(&rows, &rowBytes); err != nil {
		return core.ShardStats{}, fmt.Errorf(
			"postgres projection: read row stats: %w",
			err,
		)
	}
	if err := transaction.QueryRowContext(
		ctx,
		`/*projection:admin-log-stats*/ SELECT
  COALESCE(sum(octet_length(payload)), 0)
FROM `+source.tables.changelog+`
WHERE projection_id = $1 AND payload IS NOT NULL`,
		string(source.schema.ID),
	).Scan(&logBytes); err != nil {
		return core.ShardStats{}, fmt.Errorf(
			"postgres projection: read log stats: %w",
			err,
		)
	}
	if rows < 0 || rowBytes < 0 || logBytes < 0 {
		return core.ShardStats{}, fmt.Errorf("%w: negative stats", ErrCorrupt)
	}
	if err := transaction.Commit(); err != nil {
		return core.ShardStats{}, fmt.Errorf(
			"postgres projection: commit stats: %w",
			err,
		)
	}
	protected, subscribers := source.protectedOffset(meta.head)
	return core.ShardStats{
		Projection:        source.schema.ID,
		Shard:             shard,
		Rows:              uint64(rows),
		RowBytes:          uint64(rowBytes),
		LogBytes:          uint64(logBytes),
		Head:              encodeCursor(meta.head),
		Floor:             encodeCursor(meta.floor),
		Protected:         encodeCursor(protected),
		HeadOffset:        meta.head,
		FloorOffset:       meta.floor,
		ProtectedOffset:   protected,
		Generation:        meta.head,
		ActiveSubscribers: uint64(subscribers),
	}, nil
}

// CompactProjection advances floor in a serializable transaction and removes
// only entries not needed by the oldest locally active subscriber.
func (source *Source) CompactProjection(
	ctx context.Context,
	request core.CompactRequest,
) (core.CompactResult, error) {
	if request.Validate() != nil || request.Shard != core.DefaultShard {
		return core.CompactResult{}, core.ErrInvalidAdmin
	}
	if err := source.validate(ctx); err != nil {
		return core.CompactResult{}, err
	}
	target, err := decodeCursor(request.Before)
	if err != nil {
		return core.CompactResult{}, core.ErrInvalidAdmin
	}
	source.adminMu.Lock()
	defer source.adminMu.Unlock()
	transaction, err := source.database.BeginTx(ctx, &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		return core.CompactResult{}, fmt.Errorf(
			"postgres projection: begin compact: %w",
			err,
		)
	}
	defer func() { _ = transaction.Rollback() }()
	meta, err := source.readMeta(ctx, transaction, true)
	if err != nil {
		return core.CompactResult{}, err
	}
	if target > meta.head {
		return core.CompactResult{}, fmt.Errorf(
			"%w: target is ahead of head",
			core.ErrInvalidAdmin,
		)
	}
	protected, _ := source.protectedOffset(meta.head)
	if target > protected {
		target = protected
	}
	if target < meta.floor {
		target = meta.floor
	}
	maximumTarget := meta.floor + uint64(request.MaxEntries)
	if maximumTarget < meta.floor {
		maximumTarget = math.MaxUint64
	}
	if target > maximumTarget {
		target = maximumTarget
	}
	deleted := int64(0)
	if target > meta.floor {
		if _, err := transaction.ExecContext(
			ctx,
			`/*projection:admin-prune*/ UPDATE `+source.tables.changelog+`
SET payload = NULL
WHERE projection_id = $1
  AND offset_value <= $2
  AND payload IS NOT NULL`,
			string(source.schema.ID),
			int64(target),
		); err != nil {
			return core.CompactResult{}, fmt.Errorf(
				"postgres projection: prune compacted payload: %w",
				err,
			)
		}
		result, err := transaction.ExecContext(
			ctx,
			`/*projection:admin-delete*/ WITH doomed AS (
  SELECT ctid FROM `+source.tables.changelog+`
  WHERE projection_id = $1 AND offset_value <= $2
  ORDER BY offset_value
  LIMIT $3
)
DELETE FROM `+source.tables.changelog+` AS changelog
USING doomed
WHERE changelog.ctid = doomed.ctid`,
			string(source.schema.ID),
			int64(target),
			request.MaxEntries,
		)
		if err != nil {
			return core.CompactResult{}, fmt.Errorf(
				"postgres projection: delete compacted log: %w",
				err,
			)
		}
		deleted, err = result.RowsAffected()
		if err != nil || deleted < 0 {
			return core.CompactResult{}, fmt.Errorf(
				"%w: compact result",
				ErrCorrupt,
			)
		}
		result, err = transaction.ExecContext(
			ctx,
			`/*projection:admin-floor*/ UPDATE `+source.tables.meta+`
SET floor_offset = $2
WHERE projection_id = $1 AND floor_offset = $3`,
			string(source.schema.ID),
			int64(target),
			int64(meta.floor),
		)
		if err != nil {
			return core.CompactResult{}, fmt.Errorf(
				"postgres projection: update compact floor: %w",
				err,
			)
		}
		affected, affectedErr := result.RowsAffected()
		if affectedErr != nil || affected != 1 {
			return core.CompactResult{}, fmt.Errorf(
				"%w: compact floor update",
				ErrCorrupt,
			)
		}
	}
	if err := transaction.Commit(); err != nil {
		return core.CompactResult{}, fmt.Errorf(
			"postgres projection: commit compact: %w",
			err,
		)
	}
	return core.CompactResult{
		Shard:          request.Shard,
		PreviousFloor:  encodeCursor(meta.floor),
		Floor:          encodeCursor(target),
		Protected:      encodeCursor(protected),
		DeletedEntries: uint64(deleted),
	}, nil
}

// ForceProjectionSnapshot advances floor toward head; any clamped older
// subscriber remains protected and stale reconnects receive the normal GAP.
func (source *Source) ForceProjectionSnapshot(
	ctx context.Context,
	shard core.ShardID,
) (core.ForcedSnapshot, error) {
	stats, err := source.ProjectionStats(ctx, shard)
	if err != nil {
		return core.ForcedSnapshot{}, err
	}
	result, err := source.CompactProjection(ctx, core.CompactRequest{
		Shard:      shard,
		Before:     stats.Head,
		MaxEntries: 100_000,
	})
	if err != nil {
		return core.ForcedSnapshot{}, err
	}
	return core.ForcedSnapshot{
		Shard:      shard,
		Head:       stats.Head,
		Floor:      result.Floor,
		Protected:  result.Protected,
		Generation: stats.Generation,
	}, nil
}

func (source *Source) protectedOffset(head uint64) (uint64, int) {
	source.mu.Lock()
	sessions := make([]*session, 0, len(source.sessions))
	for current := range source.sessions {
		sessions = append(sessions, current)
	}
	source.mu.Unlock()
	protected := head
	for _, current := range sessions {
		current.nextMu.Lock()
		delivered := current.delivered
		current.nextMu.Unlock()
		if delivered < protected {
			protected = delivered
		}
	}
	return protected, len(sessions)
}
