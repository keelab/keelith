// Package mysql implements the Keelith outbox Repository for MySQL 8.0.16+
// with InnoDB.
package mysql

import (
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	coreoutbox "github.com/keelab/keelith/outbox"
)

const defaultTable = "keelith_outbox"

var (
	// ErrInvalidOption reports an invalid DB, table, transaction, or message.
	ErrInvalidOption = errors.New("mysql outbox: invalid option")
	// ErrLeaseMismatch means a stale owner attempted to settle a newer claim.
	ErrLeaseMismatch = errors.New("mysql outbox: lease owner mismatch")
)

var (
	_ coreoutbox.Repository           = (*Repository)(nil)
	_ coreoutbox.Enqueuer[*stdsql.Tx] = (*Repository)(nil)
	_ coreoutbox.Maintenance          = (*Repository)(nil)
)

// Options configure the MySQL table and short claim transaction isolation.
type Options struct {
	Table     string
	Isolation stdsql.IsolationLevel
}

// Repository stores outbox rows in MySQL 8.0.16+/InnoDB.
type Repository struct {
	database  *stdsql.DB
	table     string
	isolation stdsql.IsolationLevel
	clock     func() time.Time
}

// New constructs a MySQL Repository.
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
	return &Repository{
		database:  database,
		table:     quoted,
		isolation: options.Isolation,
		clock: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

// Schema returns idempotent MySQL 8.0.16+/InnoDB DDL for an outbox table.
func Schema(table string) (string, error) {
	if strings.TrimSpace(table) == "" {
		table = defaultTable
	}
	quoted, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(quoted))
	attemptsConstraint := fmt.Sprintf(
		"`keelith_ob_%x_attempts_chk`",
		sum[:6],
	)
	stateConstraint := fmt.Sprintf(
		"`keelith_ob_%x_state_chk`",
		sum[:6],
	)
	replayConstraint := fmt.Sprintf(
		"`keelith_ob_%x_replay_chk`",
		sum[:6],
	)
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  id varbinary(256) NOT NULL,
  destination varbinary(512) NOT NULL,
  message_key longblob NOT NULL,
  payload longblob NOT NULL,
  headers json NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  available_at datetime(6) NOT NULL,
  lease_owner varbinary(256) NULL,
  lease_until datetime(6) NULL,
  state tinyint NOT NULL DEFAULT 0,
  failure_reason varbinary(64) NULL,
  created_at datetime(6) NOT NULL,
  published_at datetime(6) NULL,
  terminal_at datetime(6) NULL,
  replay_count integer NOT NULL DEFAULT 0,
  replayed_at datetime(6) NULL,
  PRIMARY KEY (id),
  KEY keelith_outbox_pending_idx (state, available_at, id),
  CONSTRAINT %s CHECK (attempts >= 0),
  CONSTRAINT %s CHECK (state IN (0, 1, 2)),
  CONSTRAINT %s CHECK (replay_count >= 0)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
`, quoted, attemptsConstraint, stateConstraint, replayConstraint), nil
}

// Enqueue inserts a message using the caller's business transaction.
func (repository *Repository) Enqueue(
	ctx context.Context,
	transaction *stdsql.Tx,
	message coreoutbox.Message,
	availableAt time.Time,
) error {
	if repository == nil || repository.database == nil || transaction == nil {
		return fmt.Errorf(
			"%w: repository or transaction is nil",
			ErrInvalidOption,
		)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := message.Validate(); err != nil {
		return err
	}
	if message.Attempts != 0 {
		return fmt.Errorf("%w: new message has attempts", ErrInvalidOption)
	}
	if availableAt.IsZero() {
		availableAt = repository.clock()
	}
	headers, err := json.Marshal(message.Headers)
	if err != nil {
		return fmt.Errorf("mysql outbox: encode headers: %w", err)
	}
	_, err = transaction.ExecContext(
		ctx,
		`INSERT INTO `+repository.table+` (
  id, destination, message_key, payload, headers,
  attempts, available_at, state, created_at
) VALUES (?, ?, ?, ?, ?, 0, ?, 0, ?)`,
		message.ID,
		message.Destination,
		message.Key,
		message.Payload,
		headers,
		availableAt.UTC(),
		repository.clock(),
	)
	if err != nil {
		return fmt.Errorf("mysql outbox: enqueue: %w", err)
	}
	return nil
}

// Claim atomically leases a bounded batch with FOR UPDATE SKIP LOCKED.
func (repository *Repository) Claim(
	ctx context.Context,
	request coreoutbox.ClaimRequest,
) ([]coreoutbox.Message, error) {
	if repository == nil || repository.database == nil {
		return nil, fmt.Errorf("%w: repository is nil", ErrInvalidOption)
	}
	now := repository.clock()
	if ctx == nil {
		return nil, fmt.Errorf("%w: claim request", ErrInvalidOption)
	}
	if err := request.Validate(now); err != nil {
		return nil, fmt.Errorf("%w: claim request: %w", ErrInvalidOption, err)
	}
	transaction, err := repository.database.BeginTx(ctx, &stdsql.TxOptions{
		Isolation: repository.isolation,
	})
	if err != nil {
		return nil, fmt.Errorf("mysql outbox: begin claim: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	rows, err := transaction.QueryContext(
		ctx,
		`SELECT id
FROM `+repository.table+`
WHERE state = 0
  AND available_at <= ?
  AND (lease_until IS NULL OR lease_until <= ?)
ORDER BY available_at, id
LIMIT ?
FOR UPDATE SKIP LOCKED`,
		now,
		now,
		request.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("mysql outbox: select claim: %w", err)
	}
	ids := make([]string, 0, request.Limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("mysql outbox: scan claim ID: %w", err)
		}
		if !validIdentity(id) {
			_ = rows.Close()
			return nil, fmt.Errorf(
				"%w: stored message id is invalid",
				ErrInvalidOption,
			)
		}
		ids = append(ids, id)
	}
	rowsErr := rows.Err()
	closeErr := rows.Close()
	if err := errors.Join(rowsErr, closeErr); err != nil {
		return nil, fmt.Errorf("mysql outbox: claim ids: %w", err)
	}
	if len(ids) == 0 {
		if err := transaction.Commit(); err != nil {
			return nil, fmt.Errorf("mysql outbox: commit empty claim: %w", err)
		}
		return []coreoutbox.Message{}, nil
	}

	arguments := make([]any, 0, 4+len(ids))
	arguments = append(
		arguments,
		request.Owner,
		request.LeaseUntil.UTC(),
		now,
		now,
	)
	for _, id := range ids {
		arguments = append(arguments, id)
	}
	result, err := transaction.ExecContext(
		ctx,
		`UPDATE `+repository.table+`
SET lease_owner = ?,
    lease_until = ?,
    attempts = attempts + 1
WHERE state = 0
  AND available_at <= ?
  AND (lease_until IS NULL OR lease_until <= ?)
  AND id IN (`+placeholders(len(ids))+`)`,
		arguments...,
	)
	if err != nil {
		return nil, fmt.Errorf("mysql outbox: lease claim: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("mysql outbox: lease result: %w", err)
	}
	if affected != int64(len(ids)) {
		return nil, fmt.Errorf(
			"mysql outbox: leased %d rows, selected %d",
			affected,
			len(ids),
		)
	}

	arguments = make([]any, 0, 1+len(ids))
	arguments = append(arguments, request.Owner)
	for _, id := range ids {
		arguments = append(arguments, id)
	}
	rows, err = transaction.QueryContext(
		ctx,
		`SELECT id, destination, message_key, payload, headers, attempts
FROM `+repository.table+`
WHERE lease_owner = ?
  AND id IN (`+placeholders(len(ids))+`)
ORDER BY available_at, id`,
		arguments...,
	)
	if err != nil {
		return nil, fmt.Errorf("mysql outbox: read claim: %w", err)
	}
	messages := make([]coreoutbox.Message, 0, len(ids))
	for rows.Next() {
		var message coreoutbox.Message
		var headers []byte
		if err := rows.Scan(
			&message.ID,
			&message.Destination,
			&message.Key,
			&message.Payload,
			&headers,
			&message.Attempts,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("mysql outbox: scan claim: %w", err)
		}
		if err := json.Unmarshal(headers, &message.Headers); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("mysql outbox: decode headers: %w", err)
		}
		if message.Headers == nil {
			message.Headers = map[string][]byte{}
		}
		if err := message.Validate(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf(
				"mysql outbox: invalid stored message: %w",
				err,
			)
		}
		messages = append(messages, message.Clone())
	}
	rowsErr = rows.Err()
	closeErr = rows.Close()
	if err := errors.Join(rowsErr, closeErr); err != nil {
		return nil, fmt.Errorf("mysql outbox: claim rows: %w", err)
	}
	if len(messages) != len(ids) {
		return nil, fmt.Errorf(
			"mysql outbox: read %d rows, selected %d",
			len(messages),
			len(ids),
		)
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("mysql outbox: commit claim: %w", err)
	}
	return messages, nil
}

// Complete marks one owner-matched claim as published.
func (repository *Repository) Complete(
	ctx context.Context,
	owner string,
	id string,
) error {
	return repository.settle(ctx, owner, id, true, false, time.Time{}, "")
}

// Reschedule releases one owner-matched claim for retry or terminal failure.
func (repository *Repository) Reschedule(
	ctx context.Context,
	owner string,
	id string,
	next time.Time,
	terminal bool,
	reason string,
) error {
	if !validReason(reason) || next.IsZero() {
		return fmt.Errorf("%w: reschedule fields", ErrInvalidOption)
	}
	return repository.settle(ctx, owner, id, false, terminal, next, reason)
}

// Replay atomically moves an exact terminal set back to pending.
func (repository *Repository) Replay(
	ctx context.Context,
	request coreoutbox.ReplayRequest,
) (coreoutbox.ReplayResult, error) {
	if repository == nil || repository.database == nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"%w: repository is nil",
			ErrInvalidOption,
		)
	}
	now := repository.clock()
	if ctx == nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidOption,
		)
	}
	if err := request.Validate(now); err != nil {
		return coreoutbox.ReplayResult{}, err
	}
	ids := append([]string(nil), request.IDs...)
	sort.Strings(ids)
	transaction, err := repository.database.BeginTx(
		ctx,
		&stdsql.TxOptions{Isolation: repository.isolation},
	)
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"mysql outbox: begin replay: %w",
			err,
		)
	}
	defer func() { _ = transaction.Rollback() }()

	arguments := make([]any, len(ids))
	for index, id := range ids {
		arguments[index] = id
	}
	rows, err := transaction.QueryContext(
		ctx,
		`SELECT id, failure_reason
FROM `+repository.table+`
WHERE state = 2
  AND id IN (`+placeholders(len(ids))+`)
ORDER BY id
FOR UPDATE`,
		arguments...,
	)
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"mysql outbox: select replay: %w",
			err,
		)
	}
	selected := 0
	matches := true
	for rows.Next() {
		var id string
		var reason string
		if err := rows.Scan(&id, &reason); err != nil {
			_ = rows.Close()
			return coreoutbox.ReplayResult{}, fmt.Errorf(
				"mysql outbox: scan replay: %w",
				err,
			)
		}
		if !validIdentity(id) || reason != request.ExpectedReason {
			matches = false
		}
		selected++
	}
	rowsErr := rows.Err()
	closeErr := rows.Close()
	if err := errors.Join(rowsErr, closeErr); err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"mysql outbox: replay rows: %w",
			err,
		)
	}
	if !matches || selected != len(ids) {
		return coreoutbox.ReplayResult{}, coreoutbox.ErrReplayConflict
	}

	arguments = make([]any, 0, 3+len(ids))
	arguments = append(
		arguments,
		request.AvailableAt.UTC(),
		now,
		request.ExpectedReason,
	)
	for _, id := range ids {
		arguments = append(arguments, id)
	}
	result, err := transaction.ExecContext(
		ctx,
		`UPDATE `+repository.table+`
SET state = 0,
    attempts = 0,
    available_at = ?,
    lease_owner = NULL,
    lease_until = NULL,
    failure_reason = NULL,
    terminal_at = NULL,
    replay_count = replay_count + 1,
    replayed_at = ?
WHERE state = 2
  AND failure_reason = ?
  AND id IN (`+placeholders(len(ids))+`)`,
		arguments...,
	)
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"mysql outbox: replay: %w",
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"mysql outbox: replay result: %w",
			err,
		)
	}
	if affected != int64(len(ids)) {
		return coreoutbox.ReplayResult{}, coreoutbox.ErrReplayConflict
	}
	if err := transaction.Commit(); err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"mysql outbox: commit replay: %w",
			err,
		)
	}
	return coreoutbox.ReplayResult{
		Requested: len(ids),
		Replayed:  len(ids),
	}, nil
}

func (repository *Repository) settle(
	ctx context.Context,
	owner string,
	id string,
	completed bool,
	terminal bool,
	next time.Time,
	reason string,
) error {
	if repository == nil || repository.database == nil {
		return fmt.Errorf("%w: repository is nil", ErrInvalidOption)
	}
	if ctx == nil || !validIdentity(owner) || !validIdentity(id) {
		return fmt.Errorf("%w: settlement identity", ErrInvalidOption)
	}
	var (
		result stdsql.Result
		err    error
	)
	switch {
	case completed:
		result, err = repository.database.ExecContext(
			ctx,
			`UPDATE `+repository.table+`
SET state = 1,
    published_at = ?,
    lease_owner = NULL,
    lease_until = NULL,
    failure_reason = NULL
WHERE id = ? AND lease_owner = ? AND state = 0`,
			repository.clock(),
			id,
			owner,
		)
	default:
		state := 0
		if terminal {
			state = 2
		}
		result, err = repository.database.ExecContext(
			ctx,
			`UPDATE `+repository.table+`
SET state = ?,
    available_at = ?,
    lease_owner = NULL,
    lease_until = NULL,
    failure_reason = ?,
    terminal_at = CASE WHEN ? = 2 THEN ? ELSE NULL END
WHERE id = ? AND lease_owner = ? AND state = 0`,
			state,
			next.UTC(),
			reason,
			state,
			repository.clock(),
			id,
			owner,
		)
	}
	if err != nil {
		return fmt.Errorf("mysql outbox: settle: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mysql outbox: settlement result: %w", err)
	}
	if affected != 1 {
		return ErrLeaseMismatch
	}
	return nil
}

// Purge deletes a bounded batch of published or terminal rows.
func (repository *Repository) Purge(
	ctx context.Context,
	request coreoutbox.RetentionRequest,
) (int64, error) {
	if repository == nil || repository.database == nil {
		return 0, fmt.Errorf("%w: repository is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return 0, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := request.Validate(); err != nil {
		return 0, err
	}
	result, err := repository.database.ExecContext(
		ctx,
		`DELETE FROM `+repository.table+`
WHERE (state = 1 AND published_at < ?)
   OR (state = 2 AND terminal_at < ?)
ORDER BY COALESCE(published_at, terminal_at), id
LIMIT ?`,
		request.PublishedBefore.UTC(),
		request.TerminalBefore.UTC(),
		request.Limit,
	)
	if err != nil {
		return 0, fmt.Errorf("mysql outbox: purge: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("mysql outbox: purge result: %w", err)
	}
	return affected, nil
}

func placeholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
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

func validIdentity(value string) bool {
	return value != "" &&
		len(value) <= 256 &&
		strings.TrimSpace(value) == value &&
		validText(value)
}

func validReason(value string) bool {
	return value != "" &&
		len(value) <= 64 &&
		strings.TrimSpace(value) == value &&
		validText(value)
}

func validText(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
