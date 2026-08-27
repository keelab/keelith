package projection

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	core "github.com/keelab/keelith/programmable/projection"
)

const (
	cursorPrefix          = "postgres/"
	cursorDigits          = 20
	defaultRetention      = 1_000
	defaultChunkBudget    = 256 * 1024
	defaultPollInterval   = 100 * time.Millisecond
	maxRetention          = 100_000
	maxChunkBudget        = 32 * 1024 * 1024
	maxChangePayloadBytes = 40 * 1024 * 1024
	maxMutationCount      = 10_000
	maxKeyBytes           = 4 * 1024
	maxValueBytes         = 16 * 1024 * 1024
	maxChangeidBytes      = 512
	mutationCodecVersion  = 1
)

var _ core.Source = (*Source)(nil)

// Options configure tables, retention, snapshot chunks, and live polling.
type Options struct {
	TablePrefix  string
	Retention    int
	ChunkBudget  int
	PollInterval time.Duration
}

// CommitRequest is one transaction-local owner row and changelog change.
type CommitRequest struct {
	Changeid   string
	Mutations  []core.Mutation
	SourceTime time.Time
}

// Source writes through caller-owned transactions and serves durable streams.
//
// Source does not own or close Database.
type Source struct {
	mu      sync.Mutex
	adminMu sync.RWMutex

	database     *stdsql.DB
	schema       core.Schema
	tables       tableSet
	retention    int
	chunkBudget  int
	pollInterval time.Duration
	sessions     map[*session]struct{}
	closed       bool
}

// New constructs a PostgreSQL owner/changelog adapter for one projection.
func New(
	database *stdsql.DB,
	schema core.Schema,
	options Options,
) (*Source, error) {
	if database == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	tables, err := resolveTables(options.TablePrefix)
	if err != nil {
		return nil, err
	}
	retention := options.Retention
	if retention == 0 {
		retention = defaultRetention
	}
	chunkBudget := options.ChunkBudget
	if chunkBudget == 0 {
		chunkBudget = defaultChunkBudget
	}
	pollInterval := options.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}
	if retention < 1 ||
		retention > maxRetention ||
		chunkBudget < 1 ||
		chunkBudget > maxChunkBudget ||
		pollInterval < time.Millisecond {
		return nil, fmt.Errorf("%w: budgets", ErrInvalidOption)
	}
	return &Source{
		database:     database,
		schema:       schema,
		tables:       tables,
		retention:    retention,
		chunkBudget:  chunkBudget,
		pollInterval: pollInterval,
		sessions:     make(map[*session]struct{}),
	}, nil
}

// Commit writes current rows, one ordered changelog entry, and its owner
// checkpoint through transaction. The caller retains commit/rollback control.
func (source *Source) Commit(
	ctx context.Context,
	transaction *stdsql.Tx,
	request CommitRequest,
) (core.Cursor, error) {
	if err := source.validate(ctx); err != nil {
		return "", err
	}
	if transaction == nil ||
		!validIdentity(request.Changeid, maxChangeidBytes) ||
		request.SourceTime.IsZero() {
		return "", fmt.Errorf("%w: commit request", ErrInvalidOption)
	}
	sourceTime := request.SourceTime.UTC().Truncate(time.Microsecond)
	mutations, payload, err := encodeMutations(request.Mutations)
	if err != nil {
		return "", err
	}
	for index, mutation := range mutations {
		if mutation.Kind() == core.MutationUpsert &&
			len(mutation.Key())+len(mutation.Value()) > source.chunkBudget {
			return "", fmt.Errorf(
				"%w: mutation %d exceeds snapshot chunk budget",
				ErrInvalidOption,
				index,
			)
		}
	}
	digest := changeDigest(
		source.schema,
		sourceTime,
		payload,
	)

	if _, err := transaction.ExecContext(
		ctx,
		`/*projection:meta-insert*/ INSERT INTO `+source.tables.meta+` (
  projection_id, fingerprint, key_fingerprint,
  head_offset, floor_offset, source_time
) VALUES ($1, $2, $3, 0, 0, NULL)
ON CONFLICT (projection_id) DO NOTHING`,
		string(source.schema.ID),
		source.schema.Fingerprint,
		source.schema.KeyFingerprint,
	); err != nil {
		return "", fmt.Errorf(
			"postgres projection: initialize owner: %w",
			err,
		)
	}
	meta, err := source.readMeta(ctx, transaction, true)
	if err != nil {
		return "", err
	}
	if err := matchingSchema(source.schema, meta.schema); err != nil {
		return "", err
	}

	var (
		existingOffset int64
		existingDigest []byte
	)
	err = transaction.QueryRowContext(
		ctx,
		`/*projection:dedupe-read*/ SELECT offset_value, digest
FROM `+source.tables.changelog+`
WHERE projection_id = $1 AND change_id = $2`,
		string(source.schema.ID),
		request.Changeid,
	).Scan(&existingOffset, &existingDigest)
	switch {
	case err == nil:
		if existingOffset <= 0 ||
			uint64(existingOffset) > meta.head ||
			len(existingDigest) != sha256.Size {
			return "", fmt.Errorf("%w: invalid change receipt", ErrCorrupt)
		}
		if !bytes.Equal(existingDigest, digest[:]) {
			return "", fmt.Errorf(
				"%w: projection %q change %q",
				core.ErrReplayConflict,
				source.schema.ID,
				request.Changeid,
			)
		}
		return encodeCursor(uint64(existingOffset)), nil
	case errors.Is(err, stdsql.ErrNoRows):
	default:
		return "", fmt.Errorf(
			"postgres projection: inspect change receipt: %w",
			err,
		)
	}

	if meta.head > 0 && sourceTime.Before(meta.sourceTime) {
		return "", fmt.Errorf("%w: source time moved backwards", ErrInvalidOption)
	}
	if meta.head >= uint64(math.MaxInt64) {
		return "", fmt.Errorf("%w: cursor exhausted", ErrInvalidCursor)
	}
	nextOffset := meta.head + 1
	previous := encodeCursor(meta.head)
	cursor := encodeCursor(nextOffset)
	batch := core.DeltaBatch{
		Schema:     source.schema,
		Previous:   previous,
		Cursor:     cursor,
		SourceTime: sourceTime,
		Mutations:  cloneMutations(mutations),
	}
	if err := batch.Validate(); err != nil {
		return "", err
	}

	for _, mutation := range mutations {
		key := mutation.Key()
		switch mutation.Kind() {
		case core.MutationUpsert:
			if _, err := transaction.ExecContext(
				ctx,
				`/*projection:row-upsert*/ INSERT INTO `+source.tables.rows+` (
  projection_id, row_key, payload
) VALUES ($1, $2, $3)
ON CONFLICT (projection_id, row_key)
DO UPDATE SET payload = EXCLUDED.payload`,
				string(source.schema.ID),
				key,
				mutation.Value(),
			); err != nil {
				return "", fmt.Errorf(
					"postgres projection: upsert row: %w",
					err,
				)
			}
		case core.MutationDelete:
			if _, err := transaction.ExecContext(
				ctx,
				`/*projection:row-delete*/ DELETE FROM `+source.tables.rows+`
WHERE projection_id = $1 AND row_key = $2`,
				string(source.schema.ID),
				key,
			); err != nil {
				return "", fmt.Errorf(
					"postgres projection: delete row: %w",
					err,
				)
			}
		default:
			return "", core.ErrInvalidMutation
		}
	}
	if _, err := transaction.ExecContext(
		ctx,
		`/*projection:log-insert*/ INSERT INTO `+source.tables.changelog+` (
  projection_id, offset_value, previous_cursor, cursor_value,
  source_time, change_id, digest, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		string(source.schema.ID),
		int64(nextOffset),
		string(previous),
		string(cursor),
		sourceTime,
		request.Changeid,
		digest[:],
		payload,
	); err != nil {
		return "", fmt.Errorf(
			"postgres projection: insert changelog: %w",
			err,
		)
	}
	floor := meta.floor
	if nextOffset > uint64(source.retention) {
		candidate := nextOffset - uint64(source.retention)
		if candidate > floor {
			floor = candidate
		}
	}
	if floor > meta.floor {
		if _, err := transaction.ExecContext(
			ctx,
			`/*projection:log-prune*/ UPDATE `+source.tables.changelog+`
SET payload = NULL
WHERE projection_id = $1
  AND offset_value <= $2
  AND payload IS NOT NULL`,
			string(source.schema.ID),
			int64(floor),
		); err != nil {
			return "", fmt.Errorf(
				"postgres projection: prune changelog: %w",
				err,
			)
		}
	}
	result, err := transaction.ExecContext(
		ctx,
		`/*projection:meta-update*/ UPDATE `+source.tables.meta+`
SET head_offset = $2,
    floor_offset = $3,
    source_time = $4
WHERE projection_id = $1`,
		string(source.schema.ID),
		int64(nextOffset),
		int64(floor),
		sourceTime,
	)
	if err != nil {
		return "", fmt.Errorf(
			"postgres projection: update owner cursor: %w",
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf(
			"postgres projection: owner cursor result: %w",
			err,
		)
	}
	if affected != 1 {
		return "", fmt.Errorf(
			"%w: owner update affected %d rows",
			ErrCorrupt,
			affected,
		)
	}
	return cursor, nil
}

// Open starts a consistent snapshot or strict cursor-resume Session.
func (source *Source) Open(
	ctx context.Context,
	request core.SubscribeRequest,
) (core.Session, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: source or context", ErrInvalidOption)
	}
	source.adminMu.RLock()
	defer source.adminMu.RUnlock()
	if err := source.validate(ctx); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := request.Schema.Accepts(source.schema); err != nil {
		return nil, err
	}
	sessionCtx, cancel := context.WithCancelCause(ctx)
	created := &session{
		source: source,
		ctx:    sessionCtx,
		cancel: cancel,
	}
	if request.ForceSnapshot {
		if err := created.openSnapshot(); err != nil {
			cancel(err)
			return nil, err
		}
	} else {
		after, err := decodeCursor(request.After)
		if err != nil {
			cancel(err)
			return nil, fmt.Errorf(
				"%w: %q",
				ErrInvalidCursor,
				request.After,
			)
		}
		meta, err := source.readMeta(ctx, source.database, false)
		if err != nil {
			cancel(err)
			return nil, err
		}
		if meta.head == 0 {
			cancel(ErrNotSeeded)
			return nil, ErrNotSeeded
		}
		if after > meta.head {
			cancel(ErrInvalidCursor)
			return nil, fmt.Errorf(
				"%w: cursor is ahead of owner",
				ErrInvalidCursor,
			)
		}
		created.delivered = after
		if after < meta.floor {
			created.gap = &core.GapFrame{
				Requested: request.After,
				Floor:     encodeCursor(meta.floor),
			}
		}
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		created.terminate(ErrSourceClosed)
		return nil, ErrSourceClosed
	}
	source.sessions[created] = struct{}{}
	source.mu.Unlock()
	return created, nil
}

// Close rejects new work and unblocks all sessions without closing Database.
func (source *Source) Close() error {
	if source == nil {
		return nil
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		return nil
	}
	source.closed = true
	sessions := make([]*session, 0, len(source.sessions))
	for current := range source.sessions {
		sessions = append(sessions, current)
		delete(source.sessions, current)
	}
	source.mu.Unlock()
	for _, current := range sessions {
		current.terminate(ErrSourceClosed)
	}
	return nil
}

type metaState struct {
	schema     core.Schema
	head       uint64
	floor      uint64
	sourceTime time.Time
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *stdsql.Row
}

func (source *Source) readMeta(
	ctx context.Context,
	queryer queryRower,
	forUpdate bool,
) (metaState, error) {
	statement := `/*projection:meta-read*/ SELECT
  fingerprint, key_fingerprint, head_offset, floor_offset, source_time
FROM ` + source.tables.meta + `
WHERE projection_id = $1`
	if forUpdate {
		statement += "\nFOR UPDATE"
	}
	var (
		fingerprint    string
		keyFingerprint string
		head           int64
		floor          int64
		sourceTime     stdsql.NullTime
	)
	err := queryer.QueryRowContext(
		ctx,
		statement,
		string(source.schema.ID),
	).Scan(
		&fingerprint,
		&keyFingerprint,
		&head,
		&floor,
		&sourceTime,
	)
	if errors.Is(err, stdsql.ErrNoRows) {
		return metaState{}, ErrNotSeeded
	}
	if err != nil {
		return metaState{}, fmt.Errorf(
			"postgres projection: read owner metadata: %w",
			err,
		)
	}
	if head < 0 ||
		floor < 0 ||
		floor > head ||
		(head == 0 && sourceTime.Valid) ||
		(head > 0 && (!sourceTime.Valid || sourceTime.Time.IsZero())) {
		return metaState{}, fmt.Errorf("%w: owner metadata", ErrCorrupt)
	}
	return metaState{
		schema: core.Schema{
			ID:             source.schema.ID,
			Fingerprint:    fingerprint,
			KeyFingerprint: keyFingerprint,
		},
		head:       uint64(head),
		floor:      uint64(floor),
		sourceTime: sourceTime.Time,
	}, nil
}

func (source *Source) validate(ctx context.Context) error {
	if source == nil || source.database == nil || ctx == nil {
		return fmt.Errorf("%w: source or context", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	source.mu.Lock()
	closed := source.closed
	source.mu.Unlock()
	if closed {
		return ErrSourceClosed
	}
	return nil
}

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
	transaction, err := session.source.database.BeginTx(
		session.ctx,
		&stdsql.TxOptions{
			Isolation: stdsql.LevelRepeatableRead,
			ReadOnly:  true,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"postgres projection: begin snapshot: %w",
			err,
		)
	}
	cut, err := session.source.readMeta(
		session.ctx,
		transaction,
		false,
	)
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
	rows, err := transaction.QueryContext(
		session.ctx,
		`/*projection:snapshot-rows*/ SELECT row_key, payload
FROM `+session.source.tables.rows+`
WHERE projection_id = $1
ORDER BY row_key`,
		string(session.source.schema.ID),
	)
	if err != nil {
		_ = transaction.Rollback()
		return fmt.Errorf(
			"postgres projection: query snapshot rows: %w",
			err,
		)
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
	if session.snapshotMode &&
		!session.snapshotEnd &&
		session.snapshotFinished {
		session.snapshotEnd = true
		return core.SnapshotEndFrame{
			Cursor:     encodeCursor(session.cut.head),
			SourceTime: session.cut.sourceTime,
		}, nil
	}
	return session.nextLive(ctx)
}

func (session *session) nextSnapshot(
	ctx context.Context,
) (core.Frame, error) {
	if !session.snapshotBegin {
		session.snapshotBegin = true
		return core.SnapshotBeginFrame{
			Schema: session.source.schema,
		}, nil
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
		if cause := context.Cause(session.ctx); cause != nil {
			return nil, session.currentError()
		}
		if !session.snapshotRows.Next() {
			if err := session.snapshotRows.Err(); err != nil {
				return nil, fmt.Errorf(
					"postgres projection: snapshot rows: %w",
					err,
				)
			}
			if err := session.finishSnapshot(); err != nil {
				return nil, err
			}
			if len(chunk) == 0 {
				return nil, nil
			}
			return core.SnapshotChunkFrame{
				Mutations: cloneMutations(chunk),
			}, nil
		}
		var key, value []byte
		if err := session.snapshotRows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf(
				"postgres projection: scan snapshot row: %w",
				err,
			)
		}
		mutation := core.Upsert(key, value)
		if err := mutation.Validate(); err != nil {
			return nil, fmt.Errorf("%w: invalid snapshot row", ErrCorrupt)
		}
		size := len(key) + len(value)
		if size > session.source.chunkBudget {
			return nil, fmt.Errorf(
				"%w: row exceeds chunk budget",
				ErrCorrupt,
			)
		}
		if len(chunk) > 0 &&
			chunkBytes+size > session.source.chunkBudget {
			cloned := mutation.Clone()
			session.pending = &cloned
			return core.SnapshotChunkFrame{
				Mutations: cloneMutations(chunk),
			}, nil
		}
		chunk = append(chunk, mutation)
		chunkBytes += size
	}
}

func (session *session) finishSnapshot() error {
	rows := session.snapshotRows
	transaction := session.snapshotTx
	session.snapshotRows = nil
	session.snapshotTx = nil
	session.snapshotFinished = true
	if rows != nil {
		if err := rows.Close(); err != nil {
			_ = transaction.Rollback()
			return fmt.Errorf(
				"postgres projection: close snapshot rows: %w",
				err,
			)
		}
	}
	if transaction != nil {
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf(
				"postgres projection: commit snapshot read: %w",
				err,
			)
		}
	}
	return nil
}

func (session *session) nextLive(ctx context.Context) (
	core.Frame,
	error,
) {
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

func (session *session) finishLive(err error) {
	session.setTerminal(err)
	session.cancel(err)
	if session.source != nil {
		session.source.mu.Lock()
		delete(session.source.sessions, session)
		session.source.mu.Unlock()
	}
}

func (session *session) loadNext(
	ctx context.Context,
) (core.Frame, *core.GapFrame, bool, error) {
	transaction, err := session.source.database.BeginTx(
		ctx,
		&stdsql.TxOptions{
			Isolation: stdsql.LevelRepeatableRead,
			ReadOnly:  true,
		},
	)
	if err != nil {
		return nil, nil, false, fmt.Errorf(
			"postgres projection: begin changelog read: %w",
			err,
		)
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
			Requested: encodeCursor(session.delivered),
			Floor:     encodeCursor(meta.floor),
		}, false, nil
	}
	if session.delivered > meta.head {
		return nil, nil, false, fmt.Errorf(
			"%w: delivered cursor is ahead",
			ErrCorrupt,
		)
	}
	if session.delivered == meta.head {
		if err := transaction.Commit(); err != nil {
			return nil, nil, false, err
		}
		return nil, nil, false, nil
	}
	nextOffset := session.delivered + 1
	var (
		previous   string
		cursor     string
		sourceTime time.Time
		payload    []byte
	)
	err = transaction.QueryRowContext(
		ctx,
		`/*projection:log-read*/ SELECT
  previous_cursor, cursor_value, source_time, payload
FROM `+session.source.tables.changelog+`
WHERE projection_id = $1
  AND offset_value = $2
  AND payload IS NOT NULL`,
		string(session.source.schema.ID),
		int64(nextOffset),
	).Scan(&previous, &cursor, &sourceTime, &payload)
	if errors.Is(err, stdsql.ErrNoRows) {
		return nil, nil, false, fmt.Errorf(
			"%w: retained changelog offset %d missing",
			ErrCorrupt,
			nextOffset,
		)
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf(
			"postgres projection: read changelog: %w",
			err,
		)
	}
	if previous != string(encodeCursor(session.delivered)) ||
		cursor != string(encodeCursor(nextOffset)) ||
		sourceTime.IsZero() {
		return nil, nil, false, fmt.Errorf(
			"%w: changelog continuity",
			ErrCorrupt,
		)
	}
	mutations, err := decodeMutations(payload)
	if err != nil {
		return nil, nil, false, err
	}
	batch := core.DeltaBatch{
		Schema:     session.source.schema,
		Previous:   core.Cursor(previous),
		Cursor:     core.Cursor(cursor),
		SourceTime: sourceTime,
		Mutations:  mutations,
	}
	if err := batch.Validate(); err != nil {
		return nil, nil, false, fmt.Errorf(
			"%w: invalid delta: %w",
			ErrCorrupt,
			err,
		)
	}
	if err := transaction.Commit(); err != nil {
		return nil, nil, false, fmt.Errorf(
			"postgres projection: commit changelog read: %w",
			err,
		)
	}
	return core.DeltaFrame{Batch: batch.Clone()}, nil, true, nil
}

func (session *session) queryContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	queryCtx, cancelCause := context.WithCancelCause(ctx)
	stop := context.AfterFunc(session.ctx, func() {
		cancelCause(context.Cause(session.ctx))
	})
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

func matchingSchema(expected, actual core.Schema) error {
	if expected.ID != actual.ID {
		return &core.SchemaMismatchError{
			Projection: expected.ID,
			Field:      "projection_id",
			Expected:   string(expected.ID),
			Actual:     string(actual.ID),
		}
	}
	if expected.Fingerprint != actual.Fingerprint {
		return &core.SchemaMismatchError{
			Projection: expected.ID,
			Field:      "fingerprint",
			Expected:   expected.Fingerprint,
			Actual:     actual.Fingerprint,
		}
	}
	if expected.KeyFingerprint != actual.KeyFingerprint {
		return &core.SchemaMismatchError{
			Projection: expected.ID,
			Field:      "key_fingerprint",
			Expected:   expected.KeyFingerprint,
			Actual:     actual.KeyFingerprint,
		}
	}
	return nil
}

func encodeCursor(offset uint64) core.Cursor {
	return core.Cursor(fmt.Sprintf(
		"%s%0*d",
		cursorPrefix,
		cursorDigits,
		offset,
	))
}

func decodeCursor(cursor core.Cursor) (uint64, error) {
	value := string(cursor)
	if len(value) != len(cursorPrefix)+cursorDigits ||
		!strings.HasPrefix(value, cursorPrefix) {
		return 0, ErrInvalidCursor
	}
	offset := uint64(0)
	for _, character := range value[len(cursorPrefix):] {
		if character < '0' || character > '9' {
			return 0, ErrInvalidCursor
		}
		digit := uint64(character - '0')
		if offset > (math.MaxUint64-digit)/10 {
			return 0, ErrInvalidCursor
		}
		offset = offset*10 + digit
	}
	if offset > uint64(math.MaxInt64) || encodeCursor(offset) != cursor {
		return 0, ErrInvalidCursor
	}
	return offset, nil
}

func encodeMutations(
	mutations []core.Mutation,
) ([]core.Mutation, []byte, error) {
	if len(mutations) == 0 || len(mutations) > maxMutationCount {
		return nil, nil, fmt.Errorf("%w: mutation count", ErrInvalidOption)
	}
	cloned := cloneMutations(mutations)
	var buffer bytes.Buffer
	buffer.Grow(5)
	buffer.WriteByte(mutationCodecVersion)
	writeUint32(&buffer, uint32(len(cloned)))
	total := 0
	for index, mutation := range cloned {
		if err := mutation.Validate(); err != nil {
			return nil, nil, fmt.Errorf(
				"%w: mutation %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
		key := mutation.Key()
		value := mutation.Value()
		if len(key) > maxKeyBytes || len(value) > maxValueBytes {
			return nil, nil, fmt.Errorf(
				"%w: mutation %d key or value size",
				ErrInvalidOption,
				index,
			)
		}
		total += len(key) + len(value)
		if total > 32*1024*1024 {
			return nil, nil, fmt.Errorf(
				"%w: mutation bytes",
				ErrInvalidOption,
			)
		}
		buffer.WriteByte(byte(mutation.Kind()))
		writeUint32(&buffer, uint32(len(key)))
		writeUint32(&buffer, uint32(len(value)))
		buffer.Write(key)
		buffer.Write(value)
		if buffer.Len() > maxChangePayloadBytes {
			return nil, nil, fmt.Errorf(
				"%w: changelog payload",
				ErrInvalidOption,
			)
		}
	}
	return cloned, buffer.Bytes(), nil
}

func decodeMutations(payload []byte) ([]core.Mutation, error) {
	if len(payload) < 5 || len(payload) > maxChangePayloadBytes {
		return nil, fmt.Errorf("%w: changelog payload size", ErrCorrupt)
	}
	reader := bytes.NewReader(payload)
	version, err := reader.ReadByte()
	if err != nil || version != mutationCodecVersion {
		return nil, fmt.Errorf("%w: changelog payload version", ErrCorrupt)
	}
	count, err := readUint32(reader)
	if err != nil || count == 0 || count > maxMutationCount {
		return nil, fmt.Errorf("%w: changelog mutation count", ErrCorrupt)
	}
	result := make([]core.Mutation, 0, count)
	total := 0
	for range count {
		kind, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("%w: mutation kind", ErrCorrupt)
		}
		keySize, err := readUint32(reader)
		if err != nil || keySize == 0 || keySize > maxKeyBytes {
			return nil, fmt.Errorf("%w: mutation key size", ErrCorrupt)
		}
		valueSize, err := readUint32(reader)
		if err != nil || valueSize > maxValueBytes {
			return nil, fmt.Errorf("%w: mutation value size", ErrCorrupt)
		}
		total += int(keySize) + int(valueSize)
		if total > 32*1024*1024 ||
			uint64(keySize)+uint64(valueSize) > uint64(reader.Len()) {
			return nil, fmt.Errorf("%w: mutation payload size", ErrCorrupt)
		}
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, fmt.Errorf("%w: mutation key", ErrCorrupt)
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, fmt.Errorf("%w: mutation value", ErrCorrupt)
		}
		var mutation core.Mutation
		switch core.MutationKind(kind) {
		case core.MutationUpsert:
			mutation = core.Upsert(key, value)
		case core.MutationDelete:
			if len(value) != 0 {
				return nil, fmt.Errorf(
					"%w: delete value",
					ErrCorrupt,
				)
			}
			mutation = core.Delete(key)
		default:
			return nil, fmt.Errorf("%w: mutation kind", ErrCorrupt)
		}
		if err := mutation.Validate(); err != nil {
			return nil, fmt.Errorf("%w: invalid mutation", ErrCorrupt)
		}
		result = append(result, mutation)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing changelog bytes", ErrCorrupt)
	}
	return result, nil
}

func writeUint32(writer io.Writer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func readUint32(reader io.Reader) (uint32, error) {
	var encoded [4]byte
	if _, err := io.ReadFull(reader, encoded[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(encoded[:]), nil
}

func changeDigest(
	schema core.Schema,
	sourceTime time.Time,
	payload []byte,
) [sha256.Size]byte {
	digest := sha256.New()
	writeDigest(digest, []byte("keelith-postgres-projection-change-v1"))
	writeDigest(digest, []byte(schema.ID))
	writeDigest(digest, []byte(schema.Fingerprint))
	writeDigest(digest, []byte(schema.KeyFingerprint))
	var timestamp [8]byte
	binary.BigEndian.PutUint64(
		timestamp[:],
		uint64(sourceTime.UnixNano()),
	)
	writeDigest(digest, timestamp[:])
	writeDigest(digest, payload)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeDigest(digest hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write(value)
}

func cloneMutations(mutations []core.Mutation) []core.Mutation {
	result := make([]core.Mutation, len(mutations))
	for index, mutation := range mutations {
		result[index] = mutation.Clone()
	}
	return result
}

func validIdentity(value string, maximum int) bool {
	if value == "" ||
		len(value) > maximum ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
