// Package mysql implements transactional consumer idempotency for MySQL
// 8.0.16+ with InnoDB.
package mysql

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	mysqldriver "github.com/go-sql-driver/mysql"
	coreinbox "github.com/keelab/keelith/inbox"
)

const (
	defaultTable         = "keelith_inbox"
	mysqlDuplicateNumber = 1062
)

var (
	// ErrInvalidOption reports an invalid DB, table, key, or callback.
	ErrInvalidOption = errors.New("mysql inbox: invalid option")
)

var _ coreinbox.Executor[*stdsql.Tx] = (*Executor)(nil)

// Options configure the MySQL inbox table and transaction isolation.
type Options struct {
	Table     string
	Isolation stdsql.IsolationLevel
}

// PurgeRequest bounds completed-key retention cleanup.
type PurgeRequest struct {
	ProcessedBefore time.Time
	Limit           int
}

// Executor owns the transaction that contains business effects and inbox row.
type Executor struct {
	database  *stdsql.DB
	table     string
	isolation stdsql.IsolationLevel
	clock     func() time.Time
}

// New constructs a MySQL transactional inbox Executor.
func New(
	database *stdsql.DB,
	options Options,
) (*Executor, error) {
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
	return &Executor{
		database:  database,
		table:     quoted,
		isolation: options.Isolation,
		clock: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

// Schema returns idempotent MySQL 8.0.16+/InnoDB DDL for an inbox table.
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
  consumer varbinary(256) NOT NULL,
  id varbinary(512) NOT NULL,
  processed_at datetime(6) NOT NULL,
  PRIMARY KEY (consumer, id),
  KEY keelith_inbox_retention_idx (processed_at, consumer, id)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
`, quoted), nil
}

// Execute inserts a unique key and runs callback in the same transaction.
func (executor *Executor) Execute(
	ctx context.Context,
	key coreinbox.Key,
	callback coreinbox.TransactionFunc[*stdsql.Tx],
) (coreinbox.Outcome, error) {
	if executor == nil || executor.database == nil {
		return 0, fmt.Errorf("%w: executor is nil", ErrInvalidOption)
	}
	if ctx == nil || key.Validate() != nil || callback == nil {
		return 0, fmt.Errorf("%w: execution input", ErrInvalidOption)
	}
	transaction, err := executor.database.BeginTx(ctx, &stdsql.TxOptions{
		Isolation: executor.isolation,
	})
	if err != nil {
		return 0, fmt.Errorf("mysql inbox: begin: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	_, err = transaction.ExecContext(
		ctx,
		`INSERT INTO `+executor.table+` (consumer, id, processed_at)
VALUES (?, ?, ?)`,
		key.Consumer,
		key.Message,
		executor.clock(),
	)
	if err != nil {
		if isDuplicate(err) {
			return coreinbox.OutcomeDuplicate, nil
		}
		return 0, fmt.Errorf("mysql inbox: claim: %w", err)
	}

	switch callback(ctx, transaction) {
	case coreinbox.DecisionCommit:
		if err := transaction.Commit(); err != nil {
			return 0, fmt.Errorf("mysql inbox: commit: %w", err)
		}
		return coreinbox.OutcomeApplied, nil
	case coreinbox.DecisionRollback:
		if err := transaction.Rollback(); err != nil {
			return 0, fmt.Errorf("mysql inbox: rollback: %w", err)
		}
		return coreinbox.OutcomeRolledBack, nil
	default:
		return 0, coreinbox.ErrInvalidDecision
	}
}

// Purge deletes a bounded batch of completed keys older than the replay
// horizon selected by the application.
func (executor *Executor) Purge(
	ctx context.Context,
	request PurgeRequest,
) (int64, error) {
	if executor == nil || executor.database == nil {
		return 0, fmt.Errorf("%w: executor is nil", ErrInvalidOption)
	}
	if ctx == nil ||
		request.ProcessedBefore.IsZero() ||
		request.Limit <= 0 ||
		request.Limit > 10_000 {
		return 0, fmt.Errorf("%w: purge request", ErrInvalidOption)
	}
	result, err := executor.database.ExecContext(
		ctx,
		`DELETE FROM `+executor.table+`
WHERE processed_at < ?
ORDER BY processed_at, consumer, id
LIMIT ?`,
		request.ProcessedBefore.UTC(),
		request.Limit,
	)
	if err != nil {
		return 0, fmt.Errorf("mysql inbox: purge: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("mysql inbox: purge result: %w", err)
	}
	return affected, nil
}

func isDuplicate(err error) bool {
	var mysqlError *mysqldriver.MySQLError
	return errors.As(err, &mysqlError) &&
		mysqlError.Number == mysqlDuplicateNumber
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
