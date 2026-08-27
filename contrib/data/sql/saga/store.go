// Package saga implements the Keelith Saga Repository for PostgreSQL.
package saga

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	coresaga "github.com/keelab/keelith/saga"
)

const defaultTable = "keelith_sagas"

var (
	// ErrInvalidOption reports an invalid database, table, or record.
	ErrInvalidOption = errors.New("postgres saga: invalid option")
)

var _ coresaga.Repository = (*Repository)(nil)

// Options configure the PostgreSQL saga table.
type Options struct {
	Table string
}

// Repository stores revisioned, fence-checked saga control state.
type Repository struct {
	database *stdsql.DB
	table    string
}

// New constructs a PostgreSQL Repository.
func New(
	database *stdsql.DB,
	options Options,
) (*Repository, error) {
	if database == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	table := strings.TrimSpace(options.Table)
	if table == "" {
		table = defaultTable
	}
	quoted, err := quoteTable(table)
	if err != nil {
		return nil, err
	}
	return &Repository{database: database, table: quoted}, nil
}

// Schema returns idempotent PostgreSQL DDL for the saga control table.
func Schema(table string) (string, error) {
	if strings.TrimSpace(table) == "" {
		table = defaultTable
	}
	quoted, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  id varchar(256) PRIMARY KEY,
  definition varchar(256) NOT NULL,
  version varchar(256) NOT NULL,
  status varchar(32) NOT NULL
    CHECK (status IN ('running', 'compensating', 'completed', 'compensated', 'failed')),
  next_step integer NOT NULL CHECK (next_step >= 0),
  compensation_index integer NOT NULL CHECK (compensation_index >= -1),
  attempt integer NOT NULL CHECK (attempt >= 0),
  cause_reason varchar(256) NOT NULL DEFAULT '',
  failure_reason varchar(256) NOT NULL DEFAULT '',
  revision bigint NOT NULL CHECK (revision > 0),
  fence bigint NOT NULL CHECK (fence > 0),
  updated_at timestamptz NOT NULL
);
`, quoted), nil
}

// Load returns one current durable control record.
func (repository *Repository) Load(
	ctx context.Context,
	id string,
) (coresaga.Record, error) {
	if repository == nil || repository.database == nil || ctx == nil {
		return coresaga.Record{}, fmt.Errorf(
			"%w: repository, context, or id",
			ErrInvalidOption,
		)
	}
	record, _, err := repository.query(
		ctx,
		`SELECT id, definition, version, status, next_step,
       compensation_index, attempt, cause_reason, failure_reason,
       revision, fence, updated_at
FROM `+repository.table+`
WHERE id = $1`,
		id,
	)
	if errors.Is(err, stdsql.ErrNoRows) {
		return coresaga.Record{}, coresaga.ErrNotFound
	}
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("postgres saga: load: %w", err)
	}
	return record, nil
}

// Create inserts one record with revision 1 and its first fence.
func (repository *Repository) Create(
	ctx context.Context,
	record coresaga.Record,
	fence uint64,
) (coresaga.Record, error) {
	if repository == nil ||
		repository.database == nil ||
		ctx == nil ||
		fence == 0 ||
		fence > math.MaxInt64 {
		return coresaga.Record{}, fmt.Errorf(
			"%w: repository, context, or fence",
			ErrInvalidOption,
		)
	}
	record.Revision = 1
	if err := record.Validate(); err != nil {
		return coresaga.Record{}, err
	}
	created, _, err := repository.query(
		ctx,
		`INSERT INTO `+repository.table+` (
  id, definition, version, status, next_step, compensation_index,
  attempt, cause_reason, failure_reason, revision, fence, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $11)
ON CONFLICT (id) DO NOTHING
RETURNING id, definition, version, status, next_step,
          compensation_index, attempt, cause_reason, failure_reason,
          revision, fence, updated_at`,
		record.ID,
		record.Definition,
		record.Version,
		string(record.Status),
		record.NextStep,
		record.CompensationIndex,
		record.Attempt,
		record.CauseReason,
		record.FailureReason,
		int64(fence),
		record.UpdatedAt.UTC(),
	)
	if errors.Is(err, stdsql.ErrNoRows) {
		return coresaga.Record{}, coresaga.ErrAlreadyExists
	}
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("postgres saga: create: %w", err)
	}
	return created, nil
}

// Save atomically checks immutable definition, revision, and ownership fence.
func (repository *Repository) Save(
	ctx context.Context,
	record coresaga.Record,
	expectedRevision uint64,
	fence uint64,
) (coresaga.Record, error) {
	if repository == nil ||
		repository.database == nil ||
		ctx == nil ||
		expectedRevision == 0 ||
		expectedRevision > math.MaxInt64 ||
		fence == 0 ||
		fence > math.MaxInt64 {
		return coresaga.Record{}, fmt.Errorf(
			"%w: repository, context, revision, or fence",
			ErrInvalidOption,
		)
	}
	if err := record.Validate(); err != nil {
		return coresaga.Record{}, err
	}
	saved, _, err := repository.query(
		ctx,
		`UPDATE `+repository.table+`
SET status = $4,
    next_step = $5,
    compensation_index = $6,
    attempt = $7,
    cause_reason = $8,
    failure_reason = $9,
    revision = revision + 1,
    fence = $10,
    updated_at = $11
WHERE id = $1
  AND definition = $2
  AND version = $3
  AND revision = $12
  AND fence <= $10
RETURNING id, definition, version, status, next_step,
          compensation_index, attempt, cause_reason, failure_reason,
          revision, fence, updated_at`,
		record.ID,
		record.Definition,
		record.Version,
		string(record.Status),
		record.NextStep,
		record.CompensationIndex,
		record.Attempt,
		record.CauseReason,
		record.FailureReason,
		int64(fence),
		record.UpdatedAt.UTC(),
		int64(expectedRevision),
	)
	if err == nil {
		return saved, nil
	}
	if !errors.Is(err, stdsql.ErrNoRows) {
		return coresaga.Record{}, fmt.Errorf("postgres saga: save: %w", err)
	}
	return coresaga.Record{}, repository.conflict(
		ctx,
		record,
		expectedRevision,
		fence,
	)
}

func (repository *Repository) conflict(
	ctx context.Context,
	record coresaga.Record,
	expectedRevision uint64,
	fence uint64,
) error {
	var (
		definition string
		version    string
		revision   int64
		current    int64
	)
	err := repository.database.QueryRowContext(
		ctx,
		`SELECT definition, version, revision, fence
FROM `+repository.table+`
WHERE id = $1`,
		record.ID,
	).Scan(&definition, &version, &revision, &current)
	if errors.Is(err, stdsql.ErrNoRows) {
		return coresaga.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("postgres saga: inspect conflict: %w", err)
	}
	if definition != record.Definition || version != record.Version {
		return coresaga.ErrDefinitionMismatch
	}
	if current > int64(fence) {
		return coresaga.ErrStaleFence
	}
	if revision != int64(expectedRevision) {
		return coresaga.ErrConflict
	}
	return coresaga.ErrConflict
}

func (repository *Repository) query(
	ctx context.Context,
	statement string,
	arguments ...any,
) (coresaga.Record, uint64, error) {
	var (
		record   coresaga.Record
		status   string
		revision int64
		fence    int64
	)
	err := repository.database.QueryRowContext(
		ctx,
		statement,
		arguments...,
	).Scan(
		&record.ID,
		&record.Definition,
		&record.Version,
		&status,
		&record.NextStep,
		&record.CompensationIndex,
		&record.Attempt,
		&record.CauseReason,
		&record.FailureReason,
		&revision,
		&fence,
		&record.UpdatedAt,
	)
	if err != nil {
		return coresaga.Record{}, 0, err
	}
	if revision <= 0 || fence <= 0 {
		return coresaga.Record{}, 0, fmt.Errorf(
			"%w: non-positive revision or fence",
			ErrInvalidOption,
		)
	}
	record.Status = coresaga.Status(status)
	record.Revision = uint64(revision)
	if err := record.Validate(); err != nil {
		return coresaga.Record{}, 0, err
	}
	return record, uint64(fence), nil
}

func quoteTable(value string) (string, error) {
	parts := strings.Split(value, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return "", fmt.Errorf("%w: table name", ErrInvalidOption)
	}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		if !validIdentifier(part) {
			return "", fmt.Errorf("%w: table identifier", ErrInvalidOption)
		}
		quoted[index] = `"` + part + `"`
	}
	return strings.Join(quoted, "."), nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 63 || !utf8.ValidString(value) {
		return false
	}
	for index, character := range value {
		if character == '_' ||
			unicode.IsLetter(character) ||
			index > 0 && unicode.IsDigit(character) {
			continue
		}
		return false
	}
	return true
}
