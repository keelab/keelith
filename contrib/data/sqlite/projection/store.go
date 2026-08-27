// Package projection provides a WAL-backed SQLite projection subscriber Store.
package projection

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	stdsql "database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	core "github.com/keelab/keelith/programmable/projection"
	// Register the sqlite3 database/sql driver owned by this adapter.
	_ "github.com/mattn/go-sqlite3"
)

const schemaDDL = `
CREATE TABLE IF NOT EXISTS keelith_projection_meta (
  projection_id TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  key_fingerprint TEXT NOT NULL,
  cursor_value TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  source_time_ns INTEGER NOT NULL,
  applied_at_ns INTEGER NOT NULL,
  last_delta_digest BLOB
);
CREATE TABLE IF NOT EXISTS keelith_projection_rows (
  projection_id TEXT NOT NULL,
  row_key BLOB NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (projection_id, row_key),
  FOREIGN KEY (projection_id) REFERENCES keelith_projection_meta(projection_id)
    ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS keelith_projection_staging_meta (
  transaction_id TEXT PRIMARY KEY,
  projection_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  key_fingerprint TEXT NOT NULL,
  base_generation INTEGER NOT NULL CHECK (base_generation >= 0),
  created_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS keelith_projection_staging_rows (
  transaction_id TEXT NOT NULL,
  row_key BLOB NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (transaction_id, row_key),
  FOREIGN KEY (transaction_id) REFERENCES keelith_projection_staging_meta(transaction_id)
    ON DELETE CASCADE
);`

var (
	// ErrInvalidOption reports an unsafe path, clock, schema, or context.
	ErrInvalidOption = errors.New("sqlite projection: invalid option")
	// ErrClosed reports use after Store.Close.
	ErrClosed = errors.New("sqlite projection: store closed")
	// ErrCorrupt reports impossible durable metadata or staging state.
	ErrCorrupt = errors.New("sqlite projection: corrupt state")
)

// Option configures a SQLite Store.
type Option func(*storeOptions) error

type storeOptions struct {
	clock func() time.Time
}

// WithClock replaces the applied-at clock for deterministic tests.
func WithClock(clock func() time.Time) Option {
	return func(options *storeOptions) error {
		if clock == nil {
			return ErrInvalidOption
		}
		options.clock = clock
		return nil
	}
}

// Store owns one SQLite database and persists atomic visible generations.
type Store struct {
	mu     sync.RWMutex
	db     *stdsql.DB
	clock  func() time.Time
	closed bool
}

var _ core.Store = (*Store)(nil)

// Open enables WAL/FULL durability, installs schema, and removes abandoned
// staging transactions older than one day.
func Open(path string, optionList ...Option) (*Store, error) {
	if path == "" || strings.ContainsAny(path, "?#\x00") {
		return nil, ErrInvalidOption
	}
	options := storeOptions{clock: func() time.Time { return time.Now().UTC() }}
	for _, option := range optionList {
		if option == nil || option(&options) != nil {
			return nil, ErrInvalidOption
		}
	}
	dsn := "file:" + path + "?_busy_timeout=5000&_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL"
	database, err := stdsql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite projection: open: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("sqlite projection: ping: %w", err)
	}
	if _, err := database.ExecContext(ctx, schemaDDL); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("sqlite projection: initialize: %w", err)
	}
	cutoff := options.clock().UTC().Add(-24 * time.Hour).UnixNano()
	if _, err := database.ExecContext(
		ctx,
		`DELETE FROM keelith_projection_staging_meta WHERE created_at_ns < ?`,
		cutoff,
	); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("sqlite projection: clean staging: %w", err)
	}
	return &Store{db: database, clock: options.clock}, nil
}

// Close releases the owned database idempotently.
func (store *Store) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return nil
	}
	store.closed = true
	database := store.db
	store.db = nil
	store.mu.Unlock()
	if database == nil {
		return nil
	}
	if err := database.Close(); err != nil {
		return fmt.Errorf("sqlite projection: close: %w", err)
	}
	return nil
}

// BeginSnapshot creates a durable but invisible empty staging generation.
func (store *Store) BeginSnapshot(
	ctx context.Context,
	schema core.Schema,
) (core.SnapshotTxn, error) {
	database, err := store.database(ctx)
	if err != nil {
		return nil, err
	}
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	base, exists, err := readMeta(ctx, database, schema.ID)
	if err != nil {
		return nil, err
	}
	if exists {
		if err := matchingSchema(schema, base.schema); err != nil {
			return nil, err
		}
	}
	id, err := stagingid()
	if err != nil {
		return nil, err
	}
	baseGeneration := uint64(0)
	if exists {
		baseGeneration = base.generation
	}
	if _, err := database.ExecContext(
		ctx,
		`INSERT INTO keelith_projection_staging_meta (
  transaction_id, projection_id, fingerprint, key_fingerprint,
  base_generation, created_at_ns
) VALUES (?, ?, ?, ?, ?, ?)`,
		id,
		string(schema.ID),
		schema.Fingerprint,
		schema.KeyFingerprint,
		baseGeneration,
		store.clock().UTC().UnixNano(),
	); err != nil {
		return nil, fmt.Errorf("sqlite projection: begin snapshot: %w", err)
	}
	return &snapshotTxn{
		store:          store,
		ID:             id,
		schema:         schema,
		baseGeneration: baseGeneration,
	}, nil
}

// ApplyDelta atomically mutates visible rows and advances their checkpoint.
func (store *Store) ApplyDelta(
	ctx context.Context,
	batch core.DeltaBatch,
) error {
	database, err := store.database(ctx)
	if err != nil {
		return err
	}
	if err := batch.Validate(); err != nil {
		return err
	}
	digest := deltaDigest(batch)
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite projection: begin delta: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()
	meta, exists, err := readMeta(ctx, transaction, batch.Schema.ID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %q", core.ErrProjectionNotFound, batch.Schema.ID)
	}
	if err := matchingSchema(batch.Schema, meta.schema); err != nil {
		return err
	}
	if meta.cursor == batch.Cursor {
		if len(meta.lastDigest) == sha256.Size && string(meta.lastDigest) == string(digest[:]) {
			return nil
		}
		return core.ErrReplayConflict
	}
	if meta.cursor != batch.Previous {
		return &core.CursorMismatchError{
			Projection: batch.Schema.ID,
			Expected:   meta.cursor,
			Actual:     batch.Previous,
		}
	}
	for _, mutation := range batch.Mutations {
		if err := applyMutation(ctx, transaction, batch.Schema.ID, mutation); err != nil {
			return err
		}
	}
	result, err := transaction.ExecContext(
		ctx,
		`UPDATE keelith_projection_meta SET
  cursor_value = ?, generation = ?, source_time_ns = ?, applied_at_ns = ?,
  last_delta_digest = ?
WHERE projection_id = ? AND generation = ?`,
		string(batch.Cursor),
		meta.generation+1,
		batch.SourceTime.UTC().UnixNano(),
		store.clock().UTC().UnixNano(),
		digest[:],
		string(batch.Schema.ID),
		meta.generation,
	)
	if err != nil {
		return fmt.Errorf("sqlite projection: update delta checkpoint: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("sqlite projection: commit delta: %w", err)
	}
	return nil
}

// Get reads one independent visible payload.
func (store *Store) Get(
	ctx context.Context,
	id core.ProjectionID,
	key []byte,
) ([]byte, bool, error) {
	database, err := store.database(ctx)
	if err != nil {
		return nil, false, err
	}
	if err := id.Validate(); err != nil {
		return nil, false, err
	}
	if err := core.Upsert(key, nil).Validate(); err != nil {
		return nil, false, err
	}
	var payload []byte
	err = database.QueryRowContext(
		ctx,
		`SELECT payload FROM keelith_projection_rows
WHERE projection_id = ? AND row_key = ?`,
		string(id),
		key,
	).Scan(&payload)
	if errors.Is(err, stdsql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlite projection: get: %w", err)
	}
	return append([]byte(nil), payload...), true, nil
}

// Checkpoint reads the atomic visible generation metadata.
func (store *Store) Checkpoint(
	ctx context.Context,
	id core.ProjectionID,
) (core.Checkpoint, bool, error) {
	database, err := store.database(ctx)
	if err != nil {
		return core.Checkpoint{}, false, err
	}
	if err := id.Validate(); err != nil {
		return core.Checkpoint{}, false, err
	}
	meta, exists, err := readMeta(ctx, database, id)
	if err != nil || !exists {
		return core.Checkpoint{}, exists, err
	}
	return core.Checkpoint{
		Schema:     meta.schema,
		Cursor:     meta.cursor,
		Generation: meta.generation,
		SourceTime: time.Unix(0, meta.sourceTime).UTC(),
		AppliedAt:  time.Unix(0, meta.appliedAt).UTC(),
	}, true, nil
}

type snapshotTxn struct {
	mu             sync.Mutex
	store          *Store
	ID             string
	schema         core.Schema
	baseGeneration uint64
	closed         bool
}

func (transaction *snapshotTxn) Stage(mutation core.Mutation) error {
	if transaction == nil || mutation.Validate() != nil {
		return core.ErrInvalidMutation
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return core.ErrSnapshotClosed
	}
	database, err := transaction.store.database(context.Background())
	if err != nil {
		return err
	}
	switch mutation.Kind() {
	case core.MutationUpsert:
		_, err = database.Exec(
			`INSERT INTO keelith_projection_staging_rows (transaction_id, row_key, payload)
VALUES (?, ?, ?)
ON CONFLICT(transaction_id, row_key) DO UPDATE SET payload = excluded.payload`,
			transaction.ID,
			mutation.Key(),
			mutation.Value(),
		)
	case core.MutationDelete:
		_, err = database.Exec(
			`DELETE FROM keelith_projection_staging_rows
WHERE transaction_id = ? AND row_key = ?`,
			transaction.ID,
			mutation.Key(),
		)
	}
	if err != nil {
		return fmt.Errorf("sqlite projection: stage snapshot: %w", err)
	}
	return nil
}

func (transaction *snapshotTxn) Commit(
	ctx context.Context,
	cursor core.Cursor,
	sourceTime time.Time,
) error {
	if transaction == nil || ctx == nil || cursor.Validate() != nil || sourceTime.IsZero() {
		return ErrInvalidOption
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return core.ErrSnapshotClosed
	}
	database, err := transaction.store.database(ctx)
	if err != nil {
		return err
	}
	sqlTxn, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite projection: begin snapshot commit: %w", err)
	}
	defer func() { _ = sqlTxn.Rollback() }()
	var base int64
	err = sqlTxn.QueryRowContext(
		ctx,
		`SELECT base_generation FROM keelith_projection_staging_meta
WHERE transaction_id = ? AND projection_id = ?`,
		transaction.ID,
		string(transaction.schema.ID),
	).Scan(&base)
	if errors.Is(err, stdsql.ErrNoRows) || base < 0 {
		return ErrCorrupt
	}
	if err != nil {
		return fmt.Errorf("sqlite projection: read staging: %w", err)
	}
	meta, exists, err := readMeta(ctx, sqlTxn, transaction.schema.ID)
	if err != nil {
		return err
	}
	currentGeneration := uint64(0)
	if exists {
		currentGeneration = meta.generation
		if err := matchingSchema(transaction.schema, meta.schema); err != nil {
			return err
		}
	}
	if uint64(base) != transaction.baseGeneration ||
		currentGeneration != transaction.baseGeneration {
		return core.ErrSnapshotConflict
	}
	if _, err := sqlTxn.ExecContext(
		ctx,
		`DELETE FROM keelith_projection_rows WHERE projection_id = ?`,
		string(transaction.schema.ID),
	); err != nil {
		return fmt.Errorf("sqlite projection: clear rows: %w", err)
	}
	if _, err := sqlTxn.ExecContext(
		ctx,
		`INSERT INTO keelith_projection_meta (
  projection_id, fingerprint, key_fingerprint, cursor_value, generation,
  source_time_ns, applied_at_ns, last_delta_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
ON CONFLICT(projection_id) DO UPDATE SET
  fingerprint = excluded.fingerprint,
  key_fingerprint = excluded.key_fingerprint,
  cursor_value = excluded.cursor_value,
  generation = excluded.generation,
  source_time_ns = excluded.source_time_ns,
  applied_at_ns = excluded.applied_at_ns,
  last_delta_digest = NULL`,
		string(transaction.schema.ID),
		transaction.schema.Fingerprint,
		transaction.schema.KeyFingerprint,
		string(cursor),
		currentGeneration+1,
		sourceTime.UTC().UnixNano(),
		transaction.store.clock().UTC().UnixNano(),
	); err != nil {
		return fmt.Errorf("sqlite projection: publish checkpoint: %w", err)
	}
	if _, err := sqlTxn.ExecContext(
		ctx,
		`INSERT INTO keelith_projection_rows (projection_id, row_key, payload)
SELECT ?, row_key, payload FROM keelith_projection_staging_rows
WHERE transaction_id = ?`,
		string(transaction.schema.ID),
		transaction.ID,
	); err != nil {
		return fmt.Errorf("sqlite projection: publish rows: %w", err)
	}
	if _, err := sqlTxn.ExecContext(
		ctx,
		`DELETE FROM keelith_projection_staging_meta WHERE transaction_id = ?`,
		transaction.ID,
	); err != nil {
		return fmt.Errorf("sqlite projection: remove staging: %w", err)
	}
	if err := sqlTxn.Commit(); err != nil {
		return fmt.Errorf("sqlite projection: commit snapshot: %w", err)
	}
	transaction.closed = true
	return nil
}

func (transaction *snapshotTxn) Abort() error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return core.ErrSnapshotClosed
	}
	database, err := transaction.store.database(context.Background())
	if err == nil {
		_, err = database.Exec(
			`DELETE FROM keelith_projection_staging_meta WHERE transaction_id = ?`,
			transaction.ID,
		)
	}
	transaction.closed = true
	if err != nil {
		return fmt.Errorf("sqlite projection: abort snapshot: %w", err)
	}
	return nil
}

type metaState struct {
	schema     core.Schema
	cursor     core.Cursor
	generation uint64
	sourceTime int64
	appliedAt  int64
	lastDigest []byte
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *stdsql.Row
}

func readMeta(
	ctx context.Context,
	queryer queryRower,
	id core.ProjectionID,
) (metaState, bool, error) {
	var fingerprint, keyFingerprint, cursor string
	var generation, sourceTime, appliedAt int64
	var digest []byte
	err := queryer.QueryRowContext(
		ctx,
		`SELECT fingerprint, key_fingerprint, cursor_value, generation,
  source_time_ns, applied_at_ns, last_delta_digest
FROM keelith_projection_meta WHERE projection_id = ?`,
		string(id),
	).Scan(
		&fingerprint,
		&keyFingerprint,
		&cursor,
		&generation,
		&sourceTime,
		&appliedAt,
		&digest,
	)
	if errors.Is(err, stdsql.ErrNoRows) {
		return metaState{}, false, nil
	}
	if err != nil {
		return metaState{}, false, fmt.Errorf("sqlite projection: read metadata: %w", err)
	}
	state := metaState{
		schema: core.Schema{
			ID:             id,
			Fingerprint:    fingerprint,
			KeyFingerprint: keyFingerprint,
		},
		cursor:     core.Cursor(cursor),
		generation: uint64(generation),
		sourceTime: sourceTime,
		appliedAt:  appliedAt,
		lastDigest: append([]byte(nil), digest...),
	}
	if generation <= 0 ||
		state.schema.Validate() != nil || state.cursor.Validate() != nil ||
		len(digest) != 0 && len(digest) != sha256.Size {
		return metaState{}, false, ErrCorrupt
	}
	return state, true, nil
}

func applyMutation(
	ctx context.Context,
	transaction *stdsql.Tx,
	id core.ProjectionID,
	mutation core.Mutation,
) error {
	var err error
	switch mutation.Kind() {
	case core.MutationUpsert:
		_, err = transaction.ExecContext(
			ctx,
			`INSERT INTO keelith_projection_rows (projection_id, row_key, payload)
VALUES (?, ?, ?)
ON CONFLICT(projection_id, row_key) DO UPDATE SET payload = excluded.payload`,
			string(id), mutation.Key(), mutation.Value(),
		)
	case core.MutationDelete:
		_, err = transaction.ExecContext(
			ctx,
			`DELETE FROM keelith_projection_rows WHERE projection_id = ? AND row_key = ?`,
			string(id), mutation.Key(),
		)
	default:
		return core.ErrInvalidMutation
	}
	if err != nil {
		return fmt.Errorf("sqlite projection: apply mutation: %w", err)
	}
	return nil
}

func (store *Store) database(ctx context.Context) (*stdsql.DB, error) {
	if store == nil || ctx == nil {
		return nil, ErrInvalidOption
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	store.mu.RLock()
	database := store.db
	closed := store.closed
	store.mu.RUnlock()
	if closed || database == nil {
		return nil, ErrClosed
	}
	return database, nil
}

func matchingSchema(expected, actual core.Schema) error {
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

func requireOneRow(result stdsql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite projection: rows affected: %w", err)
	}
	if affected != 1 {
		return core.ErrSnapshotConflict
	}
	return nil
}

func stagingid() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("sqlite projection: staging identity: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func deltaDigest(batch core.DeltaBatch) [sha256.Size]byte {
	hasher := sha256.New()
	writeDigestPart(hasher, []byte(batch.Schema.ID))
	writeDigestPart(hasher, []byte(batch.Schema.Fingerprint))
	writeDigestPart(hasher, []byte(batch.Schema.KeyFingerprint))
	writeDigestPart(hasher, []byte(batch.Previous))
	writeDigestPart(hasher, []byte(batch.Cursor))
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(batch.SourceTime.UnixNano()))
	writeDigestPart(hasher, timestamp[:])
	for _, mutation := range batch.Mutations {
		writeDigestPart(hasher, []byte{byte(mutation.Kind())})
		writeDigestPart(hasher, mutation.Key())
		writeDigestPart(hasher, mutation.Value())
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestPart(writer digestWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}
