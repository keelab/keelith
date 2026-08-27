package mysql

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	core "github.com/keelab/keelith/programmable/projection"
)

type session struct {
	source *Source
	ctx    context.Context
	cancel context.CancelCauseFunc

	nextMu  sync.Mutex
	stateMu sync.Mutex
	runErr  error

	snapshotTx       *stdsql.Tx
	snapshotRows     *stdsql.Rows
	snapshotMode     bool
	snapshotBegin    bool
	snapshotFinished bool
	snapshotEnd      bool
	pending          *core.Mutation
	cut              metaState

	delivered uint64
	gap       *core.GapFrame
}

func (session *session) openSnapshot() error {
	transaction, err := session.source.database.BeginTx(session.ctx, &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("mysql projection: begin consistent snapshot: %w", err)
	}
	cut, err := session.source.readMeta(session.ctx, transaction, false)
	if err != nil {
		_ = transaction.Rollback()
		return err
	}
	if cut.head == 0 {
		_ = transaction.Rollback()
		return ErrNotSeeded
	}
	if err := matchingSchema(session.source.schema, cut.schema); err != nil {
		_ = transaction.Rollback()
		return err
	}
	rows, err := transaction.QueryContext(session.ctx, `SELECT row_key, payload
FROM `+session.source.tables.rows+`
WHERE projection_id = ? ORDER BY row_hash`, string(session.source.schema.ID))
	if err != nil {
		_ = transaction.Rollback()
		return fmt.Errorf("mysql projection: query snapshot rows: %w", err)
	}
	session.snapshotTx = transaction
	session.snapshotRows = rows
	session.snapshotMode = true
	session.cut = cut
	session.delivered = cut.head
	return nil
}

func (session *session) Next(ctx context.Context) (core.Frame, error) {
	if session == nil {
		return nil, ErrSessionClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	session.nextMu.Lock()
	defer session.nextMu.Unlock()
	if err := session.currentError(); err != nil {
		return nil, err
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if session.snapshotMode && !session.snapshotFinished {
		frame, err := session.nextSnapshot(ctx)
		if err != nil || frame != nil {
			return frame, err
		}
	}
	if session.snapshotMode && !session.snapshotEnd && session.snapshotFinished {
		session.snapshotEnd = true
		return core.SnapshotEndFrame{
			Cursor: encodeCursor(session.cut.head), SourceTime: session.cut.sourceTime,
		}, nil
	}
	return session.nextLive(ctx)
}

func (session *session) nextSnapshot(ctx context.Context) (core.Frame, error) {
	if !session.snapshotBegin {
		session.snapshotBegin = true
		return core.SnapshotBeginFrame{Schema: session.source.schema}, nil
	}
	chunk := make([]core.Mutation, 0)
	chunkBytes := 0
	if session.pending != nil {
		mutation := session.pending.Clone()
		session.pending = nil
		chunk = append(chunk, mutation)
		chunkBytes = len(mutation.Key()) + len(mutation.Value())
	}
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		if context.Cause(session.ctx) != nil {
			return nil, session.currentError()
		}
		if !session.snapshotRows.Next() {
			if err := session.snapshotRows.Err(); err != nil {
				return nil, fmt.Errorf("mysql projection: snapshot rows: %w", err)
			}
			if err := session.finishSnapshot(); err != nil {
				return nil, err
			}
			if len(chunk) == 0 {
				return nil, nil
			}
			return core.SnapshotChunkFrame{Mutations: cloneMutations(chunk)}, nil
		}
		var key, value []byte
		if err := session.snapshotRows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("mysql projection: scan snapshot row: %w", err)
		}
		mutation := core.Upsert(key, value)
		if err := mutation.Validate(); err != nil {
			return nil, fmt.Errorf("%w: invalid snapshot row", ErrCorrupt)
		}
		size := len(key) + len(value)
		if size > session.source.chunkBudget {
			return nil, fmt.Errorf("%w: row exceeds chunk budget", ErrCorrupt)
		}
		if len(chunk) > 0 && chunkBytes+size > session.source.chunkBudget {
			cloned := mutation.Clone()
			session.pending = &cloned
			return core.SnapshotChunkFrame{Mutations: cloneMutations(chunk)}, nil
		}
		chunk = append(chunk, mutation)
		chunkBytes += size
	}
}

func (session *session) finishSnapshot() error {
	rows, transaction := session.snapshotRows, session.snapshotTx
	session.snapshotRows = nil
	session.snapshotTx = nil
	session.snapshotFinished = true
	if rows != nil {
		if err := rows.Close(); err != nil {
			_ = transaction.Rollback()
			return fmt.Errorf("mysql projection: close snapshot rows: %w", err)
		}
	}
	if transaction != nil {
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf("mysql projection: commit snapshot read: %w", err)
		}
	}
	return nil
}

func (session *session) nextLive(ctx context.Context) (core.Frame, error) {
	if session.gap != nil {
		frame := *session.gap
		session.gap = nil
		session.finishLive(ErrSessionClosed)
		return frame, nil
	}
	for {
		queryCtx, cancel := session.queryContext(ctx)
		frame, gap, available, err := session.loadNext(queryCtx)
		cancel()
		if err != nil {
			if context.Cause(session.ctx) != nil {
				return nil, session.currentError()
			}
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			return nil, err
		}
		if gap != nil {
			session.finishLive(ErrSessionClosed)
			return *gap, nil
		}
		if available {
			session.delivered++
			return frame, nil
		}
		timer := time.NewTimer(session.source.pollInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, context.Cause(ctx)
		case <-session.ctx.Done():
			timer.Stop()
			return nil, session.currentError()
		}
	}
}

func (session *session) loadNext(ctx context.Context) (core.Frame, *core.GapFrame, bool, error) {
	transaction, err := session.source.database.BeginTx(ctx, &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, nil, false, fmt.Errorf("mysql projection: begin changelog read: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()
	meta, err := session.source.readMeta(ctx, transaction, false)
	if err != nil {
		return nil, nil, false, err
	}
	if err := matchingSchema(session.source.schema, meta.schema); err != nil {
		return nil, nil, false, err
	}
	if session.delivered < meta.floor {
		if err := transaction.Commit(); err != nil {
			return nil, nil, false, err
		}
		return nil, &core.GapFrame{
			Requested: encodeCursor(session.delivered), Floor: encodeCursor(meta.floor),
		}, false, nil
	}
	if session.delivered > meta.head {
		return nil, nil, false, fmt.Errorf("%w: delivered cursor is ahead", ErrCorrupt)
	}
	if session.delivered == meta.head {
		if err := transaction.Commit(); err != nil {
			return nil, nil, false, err
		}
		return nil, nil, false, nil
	}
	nextOffset := session.delivered + 1
	var previous, cursor string
	var sourceTime time.Time
	var payload []byte
	err = transaction.QueryRowContext(ctx, `SELECT previous_cursor, cursor_value, source_time, payload
FROM `+session.source.tables.changelog+`
WHERE projection_id = ? AND offset_value = ? AND payload IS NOT NULL`,
		string(session.source.schema.ID), nextOffset,
	).Scan(&previous, &cursor, &sourceTime, &payload)
	if errors.Is(err, stdsql.ErrNoRows) {
		return nil, nil, false, fmt.Errorf("%w: retained changelog offset %d missing", ErrCorrupt, nextOffset)
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("mysql projection: read changelog: %w", err)
	}
	if previous != string(encodeCursor(session.delivered)) ||
		cursor != string(encodeCursor(nextOffset)) || sourceTime.IsZero() {
		return nil, nil, false, fmt.Errorf("%w: changelog continuity", ErrCorrupt)
	}
	mutations, err := decodeMutations(payload)
	if err != nil {
		return nil, nil, false, err
	}
	batch := core.DeltaBatch{
		Schema: session.source.schema, Previous: core.Cursor(previous), Cursor: core.Cursor(cursor),
		SourceTime: sourceTime.UTC(), Mutations: mutations,
	}
	if err := batch.Validate(); err != nil {
		return nil, nil, false, fmt.Errorf("%w: invalid delta: %w", ErrCorrupt, err)
	}
	if err := transaction.Commit(); err != nil {
		return nil, nil, false, fmt.Errorf("mysql projection: commit changelog read: %w", err)
	}
	return core.DeltaFrame{Batch: batch.Clone()}, nil, true, nil
}

func (session *session) queryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	queryCtx, cancelCause := context.WithCancelCause(ctx)
	stop := context.AfterFunc(session.ctx, func() { cancelCause(context.Cause(session.ctx)) })
	return queryCtx, func() {
		stop()
		cancelCause(context.Canceled)
	}
}

func (session *session) Close() error {
	if session == nil {
		return nil
	}
	session.terminate(ErrSessionClosed)
	return nil
}

func (session *session) finishLive(err error) {
	session.setTerminal(err)
	session.cancel(err)
	if session.source != nil {
		session.source.mu.Lock()
		delete(session.source.sessions, session)
		session.source.mu.Unlock()
	}
}

func (session *session) terminate(err error) {
	session.setTerminal(err)
	session.cancel(err)
	session.nextMu.Lock()
	if session.snapshotRows != nil {
		_ = session.snapshotRows.Close()
		session.snapshotRows = nil
	}
	if session.snapshotTx != nil {
		_ = session.snapshotTx.Rollback()
		session.snapshotTx = nil
	}
	session.nextMu.Unlock()
	if session.source != nil {
		session.source.mu.Lock()
		delete(session.source.sessions, session)
		session.source.mu.Unlock()
	}
}

func (session *session) setTerminal(err error) {
	session.stateMu.Lock()
	if session.runErr == nil {
		session.runErr = err
	}
	session.stateMu.Unlock()
}

func (session *session) currentError() error {
	session.stateMu.Lock()
	err := session.runErr
	session.stateMu.Unlock()
	if err != nil {
		return err
	}
	if cause := context.Cause(session.ctx); cause != nil {
		return cause
	}
	return nil
}
