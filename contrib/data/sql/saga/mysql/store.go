// Package mysql implements the Keelith Saga Repository for MySQL 8.0.16+
// with InnoDB.
package mysql

import (
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	mysqldriver "github.com/go-sql-driver/mysql"
	coresaga "github.com/keelab/keelith/saga"
)

const defaultTable = "keelith_sagas"

var (
	// ErrInvalidOption reports an invalid database, table, or record.
	ErrInvalidOption = errors.New("mysql saga: invalid option")
)

var _ coresaga.Repository = (*Repository)(nil)

// Options configure the MySQL saga table.
type Options struct {
	Table string
}

// Repository stores revisioned, fence-checked Saga control state.
type Repository struct {
	database *stdsql.DB
	table    string
}

// New constructs a MySQL Repository.
func New(database *stdsql.DB, options Options) (*Repository, error) {
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

// Schema returns idempotent MySQL 8.0.16+/InnoDB DDL.
func Schema(table string) (string, error) {
	if strings.TrimSpace(table) == "" {
		table = defaultTable
	}
	quoted, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(quoted))
	positionConstraint := fmt.Sprintf(
		"`keelith_saga_%x_position_chk`",
		sum[:6],
	)
	revisionConstraint := fmt.Sprintf(
		"`keelith_saga_%x_revision_chk`",
		sum[:6],
	)
	statusConstraint := fmt.Sprintf(
		"`keelith_saga_%x_status_chk`",
		sum[:6],
	)
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  id varbinary(256) NOT NULL,
  definition varbinary(256) NOT NULL,
  version varbinary(256) NOT NULL,
  status varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  next_step integer NOT NULL,
  compensation_index integer NOT NULL,
  attempt integer NOT NULL,
  cause_reason varbinary(256) NOT NULL,
  failure_reason varbinary(256) NOT NULL,
  revision bigint unsigned NOT NULL,
  fence bigint unsigned NOT NULL,
  updated_at datetime(6) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT %s CHECK (
    next_step >= 0 AND compensation_index >= -1 AND attempt >= 0
  ),
  CONSTRAINT %s CHECK (revision > 0 AND fence > 0),
  CONSTRAINT %s CHECK (
    status IN ('running', 'compensating', 'completed', 'compensated', 'failed')
  )
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
`, quoted, positionConstraint, revisionConstraint, statusConstraint), nil
}

// Load returns one current durable control record.
func (repository *Repository) Load(
	ctx context.Context,
	id string,
) (coresaga.Record, error) {
	if repository == nil || repository.database == nil || ctx == nil {
		return coresaga.Record{}, fmt.Errorf(
			"%w: repository or context is nil",
			ErrInvalidOption,
		)
	}
	record, _, err := queryRecord(
		ctx,
		repository.database,
		`SELECT id, definition, version, status, next_step,
       compensation_index, attempt, cause_reason, failure_reason,
       revision, fence, updated_at
FROM `+repository.table+`
WHERE id = ?`,
		id,
	)
	if errors.Is(err, stdsql.ErrNoRows) {
		return coresaga.Record{}, coresaga.ErrNotFound
	}
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("mysql saga: load: %w", err)
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
		fence == 0 {
		return coresaga.Record{}, fmt.Errorf(
			"%w: repository, context, or fence",
			ErrInvalidOption,
		)
	}
	record.Revision = 1
	if err := record.Validate(); err != nil {
		return coresaga.Record{}, err
	}
	_, err := repository.database.ExecContext(
		ctx,
		`INSERT INTO `+repository.table+` (
  id, definition, version, status, next_step, compensation_index,
  attempt, cause_reason, failure_reason, revision, fence, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		record.ID,
		record.Definition,
		record.Version,
		string(record.Status),
		record.NextStep,
		record.CompensationIndex,
		record.Attempt,
		record.CauseReason,
		record.FailureReason,
		fence,
		record.UpdatedAt.UTC(),
	)
	var mysqlError *mysqldriver.MySQLError
	if errors.As(err, &mysqlError) && mysqlError.Number == 1062 {
		return coresaga.Record{}, coresaga.ErrAlreadyExists
	}
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("mysql saga: create: %w", err)
	}
	return record, nil
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
		fence == 0 {
		return coresaga.Record{}, fmt.Errorf(
			"%w: repository, context, revision, or fence",
			ErrInvalidOption,
		)
	}
	if err := record.Validate(); err != nil {
		return coresaga.Record{}, err
	}
	transaction, err := repository.database.BeginTx(
		ctx,
		&stdsql.TxOptions{Isolation: stdsql.LevelReadCommitted},
	)
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("mysql saga: begin save: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	result, err := transaction.ExecContext(
		ctx,
		`UPDATE `+repository.table+`
SET status = ?,
    next_step = ?,
    compensation_index = ?,
    attempt = ?,
    cause_reason = ?,
    failure_reason = ?,
    revision = revision + 1,
    fence = ?,
    updated_at = ?
WHERE id = ?
  AND definition = ?
  AND version = ?
  AND revision = ?
  AND fence <= ?`,
		string(record.Status),
		record.NextStep,
		record.CompensationIndex,
		record.Attempt,
		record.CauseReason,
		record.FailureReason,
		fence,
		record.UpdatedAt.UTC(),
		record.ID,
		record.Definition,
		record.Version,
		expectedRevision,
		fence,
	)
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("mysql saga: save: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("mysql saga: save result: %w", err)
	}
	if affected != 1 {
		if err := transaction.Rollback(); err != nil {
			return coresaga.Record{}, fmt.Errorf(
				"mysql saga: rollback conflicted save: %w",
				err,
			)
		}
		return coresaga.Record{}, repository.conflict(
			ctx,
			record,
			expectedRevision,
			fence,
		)
	}
	saved, _, err := queryRecord(
		ctx,
		transaction,
		`SELECT id, definition, version, status, next_step,
       compensation_index, attempt, cause_reason, failure_reason,
       revision, fence, updated_at
FROM `+repository.table+`
WHERE id = ?
FOR UPDATE`,
		record.ID,
	)
	if err != nil {
		return coresaga.Record{}, fmt.Errorf("mysql saga: read saved record: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return coresaga.Record{}, fmt.Errorf("mysql saga: commit save: %w", err)
	}
	return saved, nil
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
		revision   uint64
		current    uint64
	)
	err := repository.database.QueryRowContext(
		ctx,
		`SELECT definition, version, revision, fence
FROM `+repository.table+`
WHERE id = ?`,
		record.ID,
	).Scan(&definition, &version, &revision, &current)
	if errors.Is(err, stdsql.ErrNoRows) {
		return coresaga.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("mysql saga: inspect conflict: %w", err)
	}
	if definition != record.Definition || version != record.Version {
		return coresaga.ErrDefinitionMismatch
	}
	if current > fence {
		return coresaga.ErrStaleFence
	}
	if revision != expectedRevision {
		return coresaga.ErrConflict
	}
	return coresaga.ErrConflict
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *stdsql.Row
}

func queryRecord(
	ctx context.Context,
	queryer rowQueryer,
	statement string,
	arguments ...any,
) (coresaga.Record, uint64, error) {
	var (
		record coresaga.Record
		status string
		fence  uint64
	)
	err := queryer.QueryRowContext(
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
		&record.Revision,
		&fence,
		&record.UpdatedAt,
	)
	if err != nil {
		return coresaga.Record{}, 0, err
	}
	if fence == 0 {
		return coresaga.Record{}, 0, fmt.Errorf(
			"%w: non-positive fence",
			ErrInvalidOption,
		)
	}
	record.Status = coresaga.Status(status)
	if err := record.Validate(); err != nil {
		return coresaga.Record{}, 0, err
	}
	return record, fence, nil
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
		quoted[index] = "`" + part + "`"
	}
	return strings.Join(quoted, "."), nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 64 || !utf8.ValidString(value) {
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
