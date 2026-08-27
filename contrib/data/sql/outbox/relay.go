// Package outbox implements the Keelith outbox Repository for PostgreSQL.
package outbox

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
	ErrInvalidOption = errors.New("postgres outbox: invalid option")
	// ErrLeaseMismatch means a stale owner attempted to settle a newer claim.
	ErrLeaseMismatch = errors.New("postgres outbox: lease owner mismatch")
)

var (
	_ coreoutbox.Repository           = (*Repository)(nil)
	_ coreoutbox.Enqueuer[*stdsql.Tx] = (*Repository)(nil)
	_ coreoutbox.Maintenance          = (*Repository)(nil)
)

// Options configure the PostgreSQL table and clock.
type Options struct {
	Table     string
	Isolation stdsql.IsolationLevel
}

// Repository stores outbox rows in PostgreSQL.
type Repository struct {
	database  *stdsql.DB
	table     string
	isolation stdsql.IsolationLevel
	clock     func() time.Time
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
	return &Repository{
		database:  database,
		table:     quoted,
		isolation: options.Isolation,
		clock: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

// Schema returns the idempotent PostgreSQL DDL for an outbox table.
func Schema(table string) (string, error) {
	if strings.TrimSpace(table) == "" {
		table = defaultTable
	}
	quoted, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	indexBase := strings.NewReplacer(
		"\"", "",
		".", "_",
	).Replace(quoted)
	indexName := indexBase + "_pending_idx"
	if len(indexName) > 63 {
		sum := sha256.Sum256([]byte(quoted))
		indexName = fmt.Sprintf(
			"keelith_outbox_%x_pending_idx",
			sum[:8],
		)
	}
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  id varchar(256) PRIMARY KEY,
  destination varchar(512) NOT NULL,
  message_key bytea NOT NULL DEFAULT ''::bytea,
  payload bytea NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  available_at timestamptz NOT NULL,
  lease_owner varchar(256),
  lease_until timestamptz,
  state smallint NOT NULL DEFAULT 0 CHECK (state IN (0, 1, 2)),
  failure_reason varchar(64),
  created_at timestamptz NOT NULL,
  published_at timestamptz,
  terminal_at timestamptz,
  replay_count integer NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
  replayed_at timestamptz
);
CREATE INDEX IF NOT EXISTS %s
  ON %s (available_at, id)
  WHERE state = 0;
`, quoted, indexName, quoted), nil
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
		return fmt.Errorf("postgres outbox: encode headers: %w", err)
	}
	_, err = transaction.ExecContext(
		ctx,
		`INSERT INTO `+repository.table+` (
  id, destination, message_key, payload, headers,
  attempts, available_at, state, created_at
) VALUES ($1, $2, $3, $4, $5, 0, $6, 0, $7)`,
		message.ID,
		message.Destination,
		message.Key,
		message.Payload,
		headers,
		availableAt.UTC(),
		repository.clock(),
	)
	if err != nil {
		return fmt.Errorf("postgres outbox: enqueue: %w", err)
	}
	return nil
}

// Claim atomically leases pending rows with FOR UPDATE SKIP LOCKED.
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
	rows, err := repository.database.QueryContext(
		ctx,
		`WITH candidates AS (
  SELECT id
  FROM `+repository.table+`
  WHERE state = 0
    AND available_at <= $1
    AND (lease_until IS NULL OR lease_until <= $1)
  ORDER BY available_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT $2
)
UPDATE `+repository.table+` AS target
SET lease_owner = $3,
    lease_until = $4,
    attempts = target.attempts + 1
FROM candidates
WHERE target.ID = candidates.ID
RETURNING target.ID, target.destination, target.message_key,
          target.payload, target.headers, target.attempts`,
		now,
		request.Limit,
		request.Owner,
		request.LeaseUntil.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("postgres outbox: claim: %w", err)
	}
	defer func() { _ = rows.Close() }()
	messages := make([]coreoutbox.Message, 0, request.Limit)
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
			return nil, fmt.Errorf("postgres outbox: scan claim: %w", err)
		}
		if err := json.Unmarshal(headers, &message.Headers); err != nil {
			return nil, fmt.Errorf("postgres outbox: decode headers: %w", err)
		}
		if message.Headers == nil {
			message.Headers = map[string][]byte{}
		}
		if err := message.Validate(); err != nil {
			return nil, fmt.Errorf("postgres outbox: invalid stored message: %w", err)
		}
		messages = append(messages, message.Clone())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres outbox: claim rows: %w", err)
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
	transaction, err := repository.database.BeginTx(ctx, &stdsql.TxOptions{
		Isolation: repository.isolation,
	})
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"postgres outbox: begin replay: %w",
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
  AND id IN (`+postgresPlaceholders(1, len(ids))+`)
ORDER BY id
FOR UPDATE`,
		arguments...,
	)
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"postgres outbox: select replay: %w",
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
				"postgres outbox: scan replay: %w",
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
			"postgres outbox: replay rows: %w",
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
    available_at = $1,
    lease_owner = NULL,
    lease_until = NULL,
    failure_reason = NULL,
    terminal_at = NULL,
    replay_count = replay_count + 1,
    replayed_at = $2
WHERE state = 2
  AND failure_reason = $3
  AND id IN (`+postgresPlaceholders(4, len(ids))+`)`,
		arguments...,
	)
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"postgres outbox: replay: %w",
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"postgres outbox: replay result: %w",
			err,
		)
	}
	if affected != int64(len(ids)) {
		return coreoutbox.ReplayResult{}, coreoutbox.ErrReplayConflict
	}
	if err := transaction.Commit(); err != nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"postgres outbox: commit replay: %w",
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
    published_at = $3,
    lease_owner = NULL,
    lease_until = NULL,
    failure_reason = NULL
WHERE id = $1 AND lease_owner = $2 AND state = 0`,
			id,
			owner,
			repository.clock(),
		)
	default:
		state := 0
		if terminal {
			state = 2
		}
		result, err = repository.database.ExecContext(
			ctx,
			`UPDATE `+repository.table+`
SET state = $3,
    available_at = $4,
    lease_owner = NULL,
    lease_until = NULL,
    failure_reason = $5,
    terminal_at = CASE WHEN $3 = 2 THEN $6 ELSE NULL END
WHERE id = $1 AND lease_owner = $2 AND state = 0`,
			id,
			owner,
			state,
			next.UTC(),
			reason,
			repository.clock(),
		)
	}
	if err != nil {
		return fmt.Errorf("postgres outbox: settle: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres outbox: settlement result: %w", err)
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
		`WITH expired AS (
  SELECT id
  FROM `+repository.table+`
  WHERE (state = 1 AND published_at < $1)
     OR (state = 2 AND terminal_at < $2)
  ORDER BY COALESCE(published_at, terminal_at), id
  FOR UPDATE SKIP LOCKED
  LIMIT $3
)
DELETE FROM `+repository.table+` AS target
USING expired
WHERE target.ID = expired.ID`,
		request.PublishedBefore.UTC(),
		request.TerminalBefore.UTC(),
		request.Limit,
	)
	if err != nil {
		return 0, fmt.Errorf("postgres outbox: purge: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("postgres outbox: purge result: %w", err)
	}
	return affected, nil
}

func postgresPlaceholders(start int, count int) string {
	result := make([]string, count)
	for index := range result {
		result[index] = fmt.Sprintf("$%d", start+index)
	}
	return strings.Join(result, ",")
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
