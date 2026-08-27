package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	core "github.com/keelab/keelith/programmable/projection"
)

const (
	defaultRetention    = 1_000
	defaultChunkBudget  = 256 * 1024
	defaultPollInterval = 100 * time.Millisecond
	maxRetention        = 100_000
	maxChunkBudget      = 32 * 1024 * 1024
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

// Source writes current rows and changelog entries through caller-owned
// InnoDB transactions. It does not own or close Database.
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

// New constructs a MySQL owner/changelog adapter for one projection.
func New(database *stdsql.DB, schema core.Schema, options Options) (*Source, error) {
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
	if retention < 1 || retention > maxRetention || chunkBudget < 1 ||
		chunkBudget > maxChunkBudget || pollInterval < time.Millisecond {
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

// Commit atomically writes owner rows, one ordered changelog entry, and owner
// metadata through transaction. The caller controls commit and rollback.
func (source *Source) Commit(
	ctx context.Context,
	transaction *stdsql.Tx,
	request CommitRequest,
) (core.Cursor, error) {
	if err := source.validate(ctx); err != nil {
		return "", err
	}
	if transaction == nil || !validIdentity(request.Changeid, maxChangeidBytes) || request.SourceTime.IsZero() {
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
			return "", fmt.Errorf("%w: mutation %d exceeds snapshot chunk budget", ErrInvalidOption, index)
		}
	}
	digest := changeDigest(source.schema, sourceTime, payload)
	changeHash := sha256.Sum256([]byte(request.Changeid))

	if _, err := transaction.ExecContext(ctx, `INSERT INTO `+source.tables.meta+` (
  projection_id, fingerprint, key_fingerprint,
  head_offset, floor_offset, source_time
) VALUES (?, ?, ?, 0, 0, NULL)
ON DUPLICATE KEY UPDATE projection_id = VALUES(projection_id)`,
		string(source.schema.ID), source.schema.Fingerprint, source.schema.KeyFingerprint,
	); err != nil {
		return "", fmt.Errorf("mysql projection: initialize owner: %w", err)
	}
	meta, err := source.readMeta(ctx, transaction, true)
	if err != nil {
		return "", err
	}
	if err := matchingSchema(source.schema, meta.schema); err != nil {
		return "", err
	}

	var existingOffset uint64
	var existingid, existingDigest []byte
	err = transaction.QueryRowContext(ctx, `SELECT offset_value, change_id, digest
FROM `+source.tables.changelog+`
WHERE projection_id = ? AND change_hash = ?`,
		string(source.schema.ID), changeHash[:],
	).Scan(&existingOffset, &existingid, &existingDigest)
	switch {
	case err == nil:
		if string(existingid) != request.Changeid {
			return "", fmt.Errorf("%w: change hash collision", ErrCorrupt)
		}
		if existingOffset == 0 || existingOffset > meta.head || len(existingDigest) != sha256.Size {
			return "", fmt.Errorf("%w: invalid change receipt", ErrCorrupt)
		}
		if !bytes.Equal(existingDigest, digest[:]) {
			return "", fmt.Errorf("%w: projection %q change %q", core.ErrReplayConflict, source.schema.ID, request.Changeid)
		}
		return encodeCursor(existingOffset), nil
	case errors.Is(err, stdsql.ErrNoRows):
	default:
		return "", fmt.Errorf("mysql projection: inspect change receipt: %w", err)
	}

	if meta.head > 0 && sourceTime.Before(meta.sourceTime) {
		return "", fmt.Errorf("%w: source time moved backwards", ErrInvalidOption)
	}
	if meta.head >= uint64(math.MaxInt64) {
		return "", fmt.Errorf("%w: cursor exhausted", ErrInvalidCursor)
	}
	nextOffset := meta.head + 1
	previous, cursor := encodeCursor(meta.head), encodeCursor(nextOffset)
	batch := core.DeltaBatch{
		Schema: source.schema, Previous: previous, Cursor: cursor,
		SourceTime: sourceTime, Mutations: cloneMutations(mutations),
	}
	if err := batch.Validate(); err != nil {
		return "", err
	}
	for _, mutation := range mutations {
		if err := source.applyMutation(ctx, transaction, mutation); err != nil {
			return "", err
		}
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO `+source.tables.changelog+` (
  projection_id, offset_value, previous_cursor, cursor_value,
  source_time, change_hash, change_id, digest, payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(source.schema.ID), nextOffset, string(previous), string(cursor),
		sourceTime, changeHash[:], []byte(request.Changeid), digest[:], payload,
	); err != nil {
		return "", fmt.Errorf("mysql projection: insert changelog: %w", err)
	}
	floor := meta.floor
	if nextOffset > uint64(source.retention) {
		candidate := nextOffset - uint64(source.retention)
		if candidate > floor {
			floor = candidate
		}
	}
	if floor > meta.floor {
		if _, err := transaction.ExecContext(ctx, `UPDATE `+source.tables.changelog+`
SET payload = NULL
WHERE projection_id = ? AND offset_value <= ? AND payload IS NOT NULL`,
			string(source.schema.ID), floor,
		); err != nil {
			return "", fmt.Errorf("mysql projection: prune changelog: %w", err)
		}
	}
	result, err := transaction.ExecContext(ctx, `UPDATE `+source.tables.meta+`
SET head_offset = ?, floor_offset = ?, source_time = ?
WHERE projection_id = ?`,
		nextOffset, floor, sourceTime, string(source.schema.ID),
	)
	if err != nil {
		return "", fmt.Errorf("mysql projection: update owner cursor: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return "", err
	}
	return cursor, nil
}

func (source *Source) applyMutation(
	ctx context.Context,
	transaction *stdsql.Tx,
	mutation core.Mutation,
) error {
	key := mutation.Key()
	keyHash := sha256.Sum256(key)
	var existing []byte
	err := transaction.QueryRowContext(ctx, `SELECT row_key FROM `+source.tables.rows+`
WHERE projection_id = ? AND row_hash = ? FOR UPDATE`,
		string(source.schema.ID), keyHash[:],
	).Scan(&existing)
	if err != nil && !errors.Is(err, stdsql.ErrNoRows) {
		return fmt.Errorf("mysql projection: inspect row identity: %w", err)
	}
	if err == nil && !bytes.Equal(existing, key) {
		return fmt.Errorf("%w: row key hash collision", ErrCorrupt)
	}
	switch mutation.Kind() {
	case core.MutationUpsert:
		_, err = transaction.ExecContext(ctx, `INSERT INTO `+source.tables.rows+` (
  projection_id, row_hash, row_key, payload
) VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE row_key = VALUES(row_key), payload = VALUES(payload)`,
			string(source.schema.ID), keyHash[:], key, mutation.Value(),
		)
	case core.MutationDelete:
		_, err = transaction.ExecContext(ctx, `DELETE FROM `+source.tables.rows+`
WHERE projection_id = ? AND row_hash = ? AND row_key = ?`,
			string(source.schema.ID), keyHash[:], key,
		)
	default:
		return core.ErrInvalidMutation
	}
	if err != nil {
		return fmt.Errorf("mysql projection: apply row mutation: %w", err)
	}
	return nil
}

// Open starts a consistent snapshot or strict cursor-resume Session.
func (source *Source) Open(ctx context.Context, request core.SubscribeRequest) (core.Session, error) {
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
	created := &session{source: source, ctx: sessionCtx, cancel: cancel}
	if request.ForceSnapshot {
		if err := created.openSnapshot(); err != nil {
			cancel(err)
			return nil, err
		}
	} else {
		after, err := decodeCursor(request.After)
		if err != nil {
			cancel(err)
			return nil, fmt.Errorf("%w: %q", ErrInvalidCursor, request.After)
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
			return nil, fmt.Errorf("%w: cursor is ahead of owner", ErrInvalidCursor)
		}
		created.delivered = after
		if after < meta.floor {
			created.gap = &core.GapFrame{Requested: request.After, Floor: encodeCursor(meta.floor)}
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

// Close rejects new work and unblocks sessions without closing Database.
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

func (source *Source) readMeta(ctx context.Context, queryer queryRower, forUpdate bool) (metaState, error) {
	statement := `SELECT fingerprint, key_fingerprint, head_offset, floor_offset, source_time
FROM ` + source.tables.meta + ` WHERE projection_id = ?`
	if forUpdate {
		statement += " FOR UPDATE"
	}
	var fingerprint, keyFingerprint string
	var head, floor uint64
	var sourceTime stdsql.NullTime
	err := queryer.QueryRowContext(ctx, statement, string(source.schema.ID)).Scan(
		&fingerprint, &keyFingerprint, &head, &floor, &sourceTime,
	)
	if errors.Is(err, stdsql.ErrNoRows) {
		return metaState{}, ErrNotSeeded
	}
	if err != nil {
		return metaState{}, fmt.Errorf("mysql projection: read owner metadata: %w", err)
	}
	if floor > head || head == 0 && sourceTime.Valid ||
		head > 0 && (!sourceTime.Valid || sourceTime.Time.IsZero()) {
		return metaState{}, fmt.Errorf("%w: owner metadata", ErrCorrupt)
	}
	return metaState{
		schema: core.Schema{ID: source.schema.ID, Fingerprint: fingerprint, KeyFingerprint: keyFingerprint},
		head:   head, floor: floor, sourceTime: sourceTime.Time.UTC(),
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

func matchingSchema(expected, actual core.Schema) error {
	if expected.ID != actual.ID {
		return &core.SchemaMismatchError{Projection: expected.ID, Field: "projection_id", Expected: string(expected.ID), Actual: string(actual.ID)}
	}
	if expected.Fingerprint != actual.Fingerprint {
		return &core.SchemaMismatchError{Projection: expected.ID, Field: "fingerprint", Expected: expected.Fingerprint, Actual: actual.Fingerprint}
	}
	if expected.KeyFingerprint != actual.KeyFingerprint {
		return &core.SchemaMismatchError{Projection: expected.ID, Field: "key_fingerprint", Expected: expected.KeyFingerprint, Actual: actual.KeyFingerprint}
	}
	return nil
}

func requireOneRow(result stdsql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mysql projection: rows affected: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: expected one affected row, got %d", ErrCorrupt, affected)
	}
	return nil
}
