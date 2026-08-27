package continuation

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"time"

	core "github.com/keelab/keelith/programmable/continuation"
)

// ListCalls returns payload-free summaries in stable CallID order.
func (store *Store) ListCalls(
	ctx context.Context,
	request core.ListRequest,
) ([]core.CallSummary, error) {
	if err := store.validate(ctx); err != nil {
		return nil, err
	}
	if request.Limit < 1 || request.Limit > 1000 {
		return nil, core.ErrInvalidRetention
	}
	if request.After != "" {
		if _, err := core.NewCallID(request.After); err != nil {
			return nil, core.ErrInvalidRetention
		}
	}
	rows, err := store.database.QueryContext(
		ctx,
		`SELECT
  call_id, status, revision, fence, sequence, ready_at, snapshot, expires_at
FROM `+store.table+`
WHERE call_id > $1
ORDER BY call_id
LIMIT $2`,
		request.After,
		int64(request.Limit),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"postgres continuation: list calls: %w",
			err,
		)
	}
	defer func() { _ = rows.Close() }()
	result := make([]core.CallSummary, 0, request.Limit)
	for rows.Next() {
		summary, scanErr := scanAdminSummary(rows)
		if scanErr != nil {
			return nil, fmt.Errorf(
				"postgres continuation: list calls: %w",
				scanErr,
			)
		}
		result = append(result, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"postgres continuation: list calls rows: %w",
			err,
		)
	}
	return result, nil
}

// GetCall returns one payload-free summary.
func (store *Store) GetCall(
	ctx context.Context,
	callid core.CallID,
) (core.CallSummary, error) {
	if err := store.validateCall(ctx, callid); err != nil {
		return core.CallSummary{}, err
	}
	summary, err := scanAdminSummary(store.database.QueryRowContext(
		ctx,
		`SELECT
  call_id, status, revision, fence, sequence, ready_at, snapshot, expires_at
FROM `+store.table+`
WHERE call_id = $1`,
		callid.String(),
	))
	if errors.Is(err, stdsql.ErrNoRows) {
		return core.CallSummary{}, core.ErrNotFound
	}
	if err != nil {
		return core.CallSummary{}, fmt.Errorf(
			"postgres continuation: get call: %w",
			err,
		)
	}
	return summary, nil
}

// PruneFrames physically removes terminal frames below floor.
func (store *Store) PruneFrames(
	ctx context.Context,
	callid core.CallID,
	floor uint64,
) (core.Snapshot, error) {
	if err := store.validateCall(ctx, callid); err != nil {
		return core.Snapshot{}, err
	}
	transaction, err := store.begin(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	defer func() { _ = transaction.Rollback() }()
	current, _, err := store.loadForUpdate(ctx, transaction, callid)
	if err != nil {
		return core.Snapshot{}, err
	}
	if !current.Status().Terminal() {
		return core.Snapshot{}, core.ErrNotReady
	}
	pruned, err := core.PruneSnapshot(current, floor)
	if err != nil {
		return core.Snapshot{}, err
	}
	encoded, _, err := encodeSnapshot(pruned)
	if err != nil {
		return core.Snapshot{}, err
	}
	result, err := transaction.ExecContext(
		ctx,
		`/*continuation:prune*/
UPDATE `+store.table+`
SET snapshot = $2
WHERE call_id = $1`,
		callid.String(),
		encoded,
	)
	if err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: prune frames: %w",
			err,
		)
	}
	if err := requireOneRow(result, "prune frames"); err != nil {
		return core.Snapshot{}, err
	}
	if err := transaction.Commit(); err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: commit prune frames: %w",
			err,
		)
	}
	return pruned, nil
}

// Expire atomically terminates one non-terminal call and clears its lease.
func (store *Store) Expire(
	ctx context.Context,
	request core.ExpireRequest,
) (core.Snapshot, error) {
	if err := store.validateCall(ctx, request.CallID); err != nil {
		return core.Snapshot{}, err
	}
	if request.ExpectedRevision == 0 || request.ExpiresAt.IsZero() {
		return core.Snapshot{}, core.ErrInvalidRetention
	}
	if err := requireBigint(
		request.ExpectedRevision,
		"expected revision",
	); err != nil {
		return core.Snapshot{}, err
	}
	transaction, err := store.begin(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	defer func() { _ = transaction.Rollback() }()
	current, _, err := store.loadForUpdate(
		ctx,
		transaction,
		request.CallID,
	)
	if err != nil {
		return core.Snapshot{}, err
	}
	if current.Status() == core.StatusExpired {
		if err := transaction.Commit(); err != nil {
			return core.Snapshot{}, fmt.Errorf(
				"postgres continuation: commit expire replay: %w",
				err,
			)
		}
		return current, nil
	}
	if current.Status().Terminal() {
		return core.Snapshot{}, core.ErrTerminal
	}
	if current.Revision() != request.ExpectedRevision {
		return core.Snapshot{}, core.ErrConflict
	}
	frame, err := core.NewFrame(core.FrameExpired, nil)
	if err != nil {
		return core.Snapshot{}, err
	}
	expired, err := core.Apply(
		current,
		core.Move(core.StatusExpired, current.Fence(), frame),
	)
	if err != nil {
		return core.Snapshot{}, err
	}
	if err := store.updateSnapshot(
		ctx,
		transaction,
		expired,
		true,
		request.ExpiresAt,
	); err != nil {
		return core.Snapshot{}, err
	}
	if err := transaction.Commit(); err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: commit expire: %w",
			err,
		)
	}
	return expired, nil
}

// DeleteExpired removes one bounded, lock-safe batch of elapsed terminal rows.
func (store *Store) DeleteExpired(
	ctx context.Context,
	before time.Time,
	limit int,
) (int, error) {
	if err := store.validate(ctx); err != nil {
		return 0, err
	}
	if before.IsZero() || limit < 1 || limit > 1000 {
		return 0, core.ErrInvalidRetention
	}
	result, err := store.database.ExecContext(
		ctx,
		`/*continuation:delete-expired*/
WITH candidates AS (
  SELECT call_id
  FROM `+store.table+`
  WHERE expires_at <= $1
    AND status IN ('completed', 'failed', 'canceled', 'expired')
  ORDER BY call_id
  FOR UPDATE SKIP LOCKED
  LIMIT $2
)
DELETE FROM `+store.table+` AS target
USING candidates
WHERE target.call_id = candidates.call_id`,
		before.UTC(),
		int64(limit),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"postgres continuation: delete expired: %w",
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf(
			"postgres continuation: delete expired result: %w",
			err,
		)
	}
	return int(affected), nil
}

func scanAdminSummary(scanner rowScanner) (core.CallSummary, error) {
	var (
		callid    string
		status    string
		revision  int64
		fence     int64
		sequence  int64
		readyAt   stdsql.NullTime
		encoded   []byte
		expiresAt stdsql.NullTime
	)
	if err := scanner.Scan(
		&callid,
		&status,
		&revision,
		&fence,
		&sequence,
		&readyAt,
		&encoded,
		&expiresAt,
	); err != nil {
		return core.CallSummary{}, err
	}
	snapshot, err := decodeIndexedSnapshot(
		callid,
		status,
		revision,
		fence,
		sequence,
		readyAt,
		encoded,
	)
	if err != nil {
		return core.CallSummary{}, err
	}
	var expiration time.Time
	if expiresAt.Valid {
		if !snapshot.Status().Terminal() {
			return core.CallSummary{}, fmt.Errorf(
				"%w: non-terminal expiration",
				ErrCorrupt,
			)
		}
		expiration = expiresAt.Time.UTC()
	}
	return core.NewCallSummary(snapshot, expiration)
}
