// Package continuation implements a PostgreSQL continuation Store.
package continuation

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	core "github.com/keelab/keelith/programmable/continuation"
)

const (
	defaultTable = "keelith_continuations"
	maxReadyList = 10_000
)

var (
	// ErrInvalidOption reports an invalid database, table, or option.
	ErrInvalidOption = errors.New("postgres continuation: invalid option")
	// ErrCorrupt reports disagreement between indexed columns and the blob.
	ErrCorrupt = errors.New("postgres continuation: corrupt stored snapshot")
	// ErrBigintOverflow reports a uint64 that PostgreSQL bigint cannot hold.
	ErrBigintOverflow = errors.New(
		"postgres continuation: uint64 exceeds bigint",
	)
)

var (
	_ core.Store      = (*Store)(nil)
	_ core.LeaseStore = (*Store)(nil)
	_ core.AdminStore = (*Store)(nil)
)

// Options configure the PostgreSQL continuation table and isolation level.
type Options struct {
	Table     string
	Isolation stdsql.IsolationLevel
}

// Store persists complete continuation snapshots in one PostgreSQL row.
type Store struct {
	database  *stdsql.DB
	table     string
	isolation stdsql.IsolationLevel
}

// New constructs a PostgreSQL continuation Store.
func New(database *stdsql.DB, options Options) (*Store, error) {
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
	return &Store{
		database:  database,
		table:     quoted,
		isolation: options.Isolation,
	}, nil
}

// Schema returns idempotent PostgreSQL DDL for a continuation table.
func Schema(table string) (string, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		table = defaultTable
	}
	quoted, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	indexName := readyIndexName(table)
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  call_id varchar(512) PRIMARY KEY,
  status varchar(32) NOT NULL
    CHECK (status IN (
      'accepted', 'running', 'waiting', 'suspended',
      'cancel_requested', 'completed', 'failed', 'canceled', 'expired'
    )),
  revision bigint NOT NULL
    CHECK (revision > 0 AND revision <= 9223372036854775807),
  fence bigint NOT NULL
    CHECK (fence >= 0 AND fence <= 9223372036854775807),
  sequence bigint NOT NULL
    CHECK (sequence >= 0 AND sequence <= 9223372036854775807),
  ready_at timestamptz,
  snapshot bytea NOT NULL,
  lease_owner varchar(256),
  lease_until timestamptz,
  lease_previous_revision bigint
    CHECK (
      lease_previous_revision IS NULL OR
      lease_previous_revision > 0
    ),
  CHECK (
    (lease_owner IS NULL AND
      lease_until IS NULL AND
      lease_previous_revision IS NULL) OR
    (lease_owner IS NOT NULL AND
      lease_until IS NOT NULL AND
      lease_previous_revision IS NOT NULL)
  ),
  expires_at timestamptz
);
ALTER TABLE %s
  ADD COLUMN IF NOT EXISTS lease_owner varchar(256);
ALTER TABLE %s
  ADD COLUMN IF NOT EXISTS lease_until timestamptz;
ALTER TABLE %s
  ADD COLUMN IF NOT EXISTS lease_previous_revision bigint;
ALTER TABLE %s
  ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE %s
  ADD COLUMN IF NOT EXISTS ready_at timestamptz;
CREATE INDEX IF NOT EXISTS "%s"
  ON %s (status, lease_until, call_id, revision, fence, sequence)
  WHERE status IN ('accepted', 'running', 'suspended', 'cancel_requested');
CREATE INDEX IF NOT EXISTS "%s"
  ON %s (expires_at, call_id)
  WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS "%s"
  ON %s (status, ready_at, lease_until, call_id, revision)
  WHERE status IN ('accepted', 'running', 'suspended', 'cancel_requested');
`,
		quoted,
		quoted,
		quoted,
		quoted,
		quoted,
		quoted,
		indexName,
		quoted,
		expiryIndexName(table),
		quoted,
		timerReadyIndexName(table),
		quoted,
	), nil
}

// Create atomically inserts one initial accepted Snapshot.
func (store *Store) Create(
	ctx context.Context,
	snapshot core.Snapshot,
) (core.Snapshot, error) {
	if err := store.validate(ctx); err != nil {
		return core.Snapshot{}, err
	}
	if !initialSnapshot(snapshot) {
		return core.Snapshot{}, core.ErrInvalidStore
	}
	encoded, columns, err := encodeSnapshot(snapshot)
	if err != nil {
		return core.Snapshot{}, err
	}
	result, err := store.database.ExecContext(
		ctx,
		`INSERT INTO `+store.table+` (
  call_id, status, revision, fence, sequence, ready_at, snapshot
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (call_id) DO NOTHING`,
		snapshot.CallID().String(),
		columns.status,
		columns.revision,
		columns.fence,
		columns.sequence,
		columns.readyAt,
		encoded,
	)
	if err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: create: %w",
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: create result: %w",
			err,
		)
	}
	switch affected {
	case 1:
		return snapshot, nil
	case 0:
		return core.Snapshot{}, core.ErrAlreadyExists
	default:
		return core.Snapshot{}, fmt.Errorf(
			"%w: create affected %d rows",
			ErrCorrupt,
			affected,
		)
	}
}

// Load returns one validated immutable Snapshot.
func (store *Store) Load(
	ctx context.Context,
	callid core.CallID,
) (core.Snapshot, error) {
	if err := store.validateCall(ctx, callid); err != nil {
		return core.Snapshot{}, err
	}
	snapshot, err := scanSnapshot(store.database.QueryRowContext(
		ctx,
		store.selectStatement(false),
		callid.String(),
	))
	if errors.Is(err, stdsql.ErrNoRows) {
		return core.Snapshot{}, core.ErrNotFound
	}
	if err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: load: %w",
			err,
		)
	}
	return snapshot, nil
}

// Acquire atomically moves a ready call under a strictly newer fence.
func (store *Store) Acquire(
	ctx context.Context,
	callid core.CallID,
	expectedRevision uint64,
) (core.Snapshot, error) {
	if err := store.validateCall(ctx, callid); err != nil {
		return core.Snapshot{}, err
	}
	if expectedRevision == 0 {
		return core.Snapshot{}, core.ErrInvalidStore
	}
	if err := requireBigint(
		expectedRevision,
		"expected revision",
	); err != nil {
		return core.Snapshot{}, err
	}

	transaction, err := store.begin(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	defer func() { _ = transaction.Rollback() }()
	current, lease, err := store.loadForUpdate(ctx, transaction, callid)
	if err != nil {
		return core.Snapshot{}, err
	}
	if current.Revision() != expectedRevision {
		return core.Snapshot{}, core.ErrConflict
	}
	now, timeErr := databaseNow(ctx, transaction)
	if timeErr != nil {
		return core.Snapshot{}, timeErr
	}
	if lease.valid {
		if lease.deadline.After(now) {
			return core.Snapshot{}, core.ErrLeaseHeld
		}
	}
	if !ready(current.Status()) {
		return core.Snapshot{}, core.ErrNotReady
	}
	if err := core.TimerNotReady(current, now); err != nil {
		return core.Snapshot{}, err
	}
	if current.Fence() >= uint64(math.MaxInt64) {
		return core.Snapshot{}, core.ErrStaleFence
	}
	target := core.StatusRunning
	if current.Status() == core.StatusCancelRequested {
		target = core.StatusCancelRequested
	}
	next, err := core.Apply(
		current,
		core.Move(target, current.Fence()+1),
	)
	if err != nil {
		return core.Snapshot{}, err
	}
	if err := store.updateSnapshot(
		ctx,
		transaction,
		next,
		true,
		time.Time{},
	); err != nil {
		return core.Snapshot{}, err
	}
	if err := transaction.Commit(); err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: commit acquire: %w",
			err,
		)
	}
	return next, nil
}

// Transition atomically commits one direct Apply result.
func (store *Store) Transition(
	ctx context.Context,
	request core.CommitRequest,
) (core.Snapshot, error) {
	if err := store.validate(ctx); err != nil {
		return core.Snapshot{}, err
	}
	if request.ExpectedRevision == 0 ||
		request.Fence == 0 ||
		request.Snapshot.CallID().String() == "" {
		return core.Snapshot{}, core.ErrInvalidStore
	}
	if err := requireBigint(
		request.ExpectedRevision,
		"expected revision",
	); err != nil {
		return core.Snapshot{}, err
	}
	if err := requireBigint(request.Fence, "fence"); err != nil {
		return core.Snapshot{}, err
	}

	transaction, err := store.begin(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	defer func() { _ = transaction.Rollback() }()
	current, lease, err := store.loadForUpdate(
		ctx,
		transaction,
		request.Snapshot.CallID(),
	)
	if err != nil {
		return core.Snapshot{}, err
	}
	if request.LeaseOwner != "" {
		if err := (core.LeaseRequest{
			CallID:   request.Snapshot.CallID(),
			Revision: request.ExpectedRevision,
			Fence:    request.Fence,
			OwnerID:  request.LeaseOwner,
		}).Validate(false); err != nil {
			return core.Snapshot{}, core.ErrInvalidStore
		}
		now, timeErr := databaseNow(ctx, transaction)
		if timeErr != nil {
			return core.Snapshot{}, timeErr
		}
		if !lease.valid ||
			lease.owner != request.LeaseOwner ||
			current.Revision() != request.ExpectedRevision ||
			current.Fence() != request.Fence ||
			!lease.deadline.After(now) {
			return core.Snapshot{}, core.ErrLeaseLost
		}
	} else if lease.valid {
		now, timeErr := databaseNow(ctx, transaction)
		if timeErr != nil {
			return core.Snapshot{}, timeErr
		}
		if lease.deadline.After(now) {
			return core.Snapshot{}, core.ErrLeaseLost
		}
	}
	if request.Fence != current.Fence() {
		return core.Snapshot{}, core.ErrStaleFence
	}
	if request.ExpectedRevision != current.Revision() {
		return core.Snapshot{}, core.ErrConflict
	}
	if !validSuccessor(current, request.Snapshot, request.Fence) {
		return core.Snapshot{}, core.ErrInvalidStore
	}
	if err := store.updateSnapshot(
		ctx,
		transaction,
		request.Snapshot,
		request.LeaseOwner == "" ||
			request.Snapshot.Status() != core.StatusRunning,
		request.ExpiresAt,
	); err != nil {
		return core.Snapshot{}, err
	}
	if err := transaction.Commit(); err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: commit transition: %w",
			err,
		)
	}
	return request.Snapshot, nil
}

// ListReady returns ready snapshots ordered by CallID.
func (store *Store) ListReady(
	ctx context.Context,
	limit int,
) ([]core.Snapshot, error) {
	if err := store.validate(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > maxReadyList {
		return nil, core.ErrInvalidStore
	}
	rows, err := store.database.QueryContext(
		ctx,
		`SELECT call_id, status, revision, fence, sequence, ready_at, snapshot
FROM `+store.table+`
WHERE status IN ('accepted', 'running', 'suspended', 'cancel_requested')
  AND (lease_until IS NULL OR lease_until <= CURRENT_TIMESTAMP)
  AND (ready_at IS NULL OR ready_at <= CURRENT_TIMESTAMP)
ORDER BY call_id
LIMIT $1`,
		int64(limit),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"postgres continuation: list ready: %w",
			err,
		)
	}
	defer func() { _ = rows.Close() }()
	result := make([]core.Snapshot, 0, limit)
	for rows.Next() {
		snapshot, scanErr := scanSnapshot(rows)
		if scanErr != nil {
			return nil, fmt.Errorf(
				"postgres continuation: list ready: %w",
				scanErr,
			)
		}
		if !ready(snapshot.Status()) {
			return nil, fmt.Errorf(
				"%w: non-ready row selected",
				ErrCorrupt,
			)
		}
		result = append(result, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"postgres continuation: list ready rows: %w",
			err,
		)
	}
	return result, nil
}

// SubmitSignal atomically accepts one idempotent Signal command.
func (store *Store) SubmitSignal(
	ctx context.Context,
	request core.CommandRequest,
) (core.Snapshot, error) {
	frame, err := core.NewFrame(core.FrameSignal, request.Payload)
	if err != nil {
		return core.Snapshot{}, err
	}
	return store.command(
		ctx,
		request,
		core.Signal(request.CommandID, frame),
	)
}

// RequestCancel atomically records one cooperative cancellation request.
func (store *Store) RequestCancel(
	ctx context.Context,
	request core.CommandRequest,
) (core.Snapshot, error) {
	frame, err := core.NewFrame(
		core.FrameCancelRequested,
		request.Payload,
	)
	if err != nil {
		return core.Snapshot{}, err
	}
	return store.command(
		ctx,
		request,
		core.Cancel(request.CommandID, frame),
	)
}

func (store *Store) command(
	ctx context.Context,
	request core.CommandRequest,
	transition core.Transition,
) (core.Snapshot, error) {
	if err := store.validateCall(ctx, request.CallID); err != nil {
		return core.Snapshot{}, err
	}
	if request.ExpectedRevision == 0 {
		return core.Snapshot{}, core.ErrInvalidStore
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

	// Apply must precede revision comparison: it recognizes a committed command
	// retry after a lost response and returns the unchanged current Snapshot.
	next, err := core.Apply(current, transition)
	if err != nil {
		return core.Snapshot{}, err
	}
	if next.Revision() == current.Revision() {
		if err := transaction.Commit(); err != nil {
			return core.Snapshot{}, fmt.Errorf(
				"postgres continuation: commit command replay: %w",
				err,
			)
		}
		return current, nil
	}
	if request.ExpectedRevision != current.Revision() {
		return core.Snapshot{}, core.ErrConflict
	}
	if err := store.updateSnapshot(
		ctx,
		transaction,
		next,
		true,
		time.Time{},
	); err != nil {
		return core.Snapshot{}, err
	}
	if err := transaction.Commit(); err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"postgres continuation: commit command: %w",
			err,
		)
	}
	return next, nil
}

// Claim atomically owns one ready revision until a database-timed deadline.
func (store *Store) Claim(
	ctx context.Context,
	request core.ClaimRequest,
) (core.Lease, error) {
	if err := store.validate(ctx); err != nil {
		return core.Lease{}, err
	}
	if err := request.Validate(); err != nil {
		return core.Lease{}, err
	}
	if err := requireBigint(
		request.ExpectedRevision,
		"expected revision",
	); err != nil {
		return core.Lease{}, err
	}

	transaction, err := store.begin(ctx)
	if err != nil {
		return core.Lease{}, err
	}
	defer func() { _ = transaction.Rollback() }()
	current, lease, err := store.loadForUpdate(
		ctx,
		transaction,
		request.CallID,
	)
	if err != nil {
		return core.Lease{}, err
	}
	now, err := databaseNow(ctx, transaction)
	if err != nil {
		return core.Lease{}, err
	}
	if lease.valid && lease.deadline.After(now) {
		if lease.owner == request.OwnerID &&
			lease.previousRevision == request.ExpectedRevision {
			if err := transaction.Commit(); err != nil {
				return core.Lease{}, fmt.Errorf(
					"postgres continuation: commit claim replay: %w",
					err,
				)
			}
			return core.Lease{
				Snapshot: current,
				OwnerID:  lease.owner,
				Deadline: lease.deadline,
			}, nil
		}
		return core.Lease{}, core.ErrLeaseHeld
	}
	if current.Revision() != request.ExpectedRevision {
		return core.Lease{}, core.ErrConflict
	}
	if !ready(current.Status()) {
		return core.Lease{}, core.ErrNotReady
	}
	if err := core.TimerNotReady(current, now); err != nil {
		return core.Lease{}, err
	}
	if current.Fence() >= uint64(math.MaxInt64) {
		return core.Lease{}, core.ErrStaleFence
	}
	target := core.StatusRunning
	if current.Status() == core.StatusCancelRequested {
		target = core.StatusCancelRequested
	}
	next, err := core.Apply(
		current,
		core.Move(target, current.Fence()+1),
	)
	if err != nil {
		return core.Lease{}, err
	}
	deadline := now.Add(request.LeaseDuration)
	if err := store.updateClaim(
		ctx,
		transaction,
		next,
		request.OwnerID,
		deadline,
		current.Revision(),
	); err != nil {
		return core.Lease{}, err
	}
	if err := transaction.Commit(); err != nil {
		return core.Lease{}, fmt.Errorf(
			"postgres continuation: commit claim: %w",
			err,
		)
	}
	return core.Lease{
		Snapshot: next,
		OwnerID:  request.OwnerID,
		Deadline: deadline,
	}, nil
}

// Renew extends a current non-expired claim using database time.
func (store *Store) Renew(
	ctx context.Context,
	request core.LeaseRequest,
) (core.Lease, error) {
	if err := store.validate(ctx); err != nil {
		return core.Lease{}, err
	}
	if err := request.Validate(true); err != nil {
		return core.Lease{}, err
	}
	if err := requireLeaseCounters(request); err != nil {
		return core.Lease{}, err
	}

	transaction, err := store.begin(ctx)
	if err != nil {
		return core.Lease{}, err
	}
	defer func() { _ = transaction.Rollback() }()
	current, lease, err := store.loadForUpdate(
		ctx,
		transaction,
		request.CallID,
	)
	if err != nil {
		return core.Lease{}, err
	}
	now, err := databaseNow(ctx, transaction)
	if err != nil {
		return core.Lease{}, err
	}
	if !leaseMatches(current, lease, request) ||
		!lease.deadline.After(now) {
		return core.Lease{}, core.ErrLeaseLost
	}
	deadline := now.Add(request.LeaseDuration)
	if err := store.updateLeaseDeadline(
		ctx,
		transaction,
		request.CallID,
		deadline,
	); err != nil {
		return core.Lease{}, err
	}
	if err := transaction.Commit(); err != nil {
		return core.Lease{}, fmt.Errorf(
			"postgres continuation: commit renew: %w",
			err,
		)
	}
	return core.Lease{
		Snapshot: current,
		OwnerID:  lease.owner,
		Deadline: deadline,
	}, nil
}

// Release makes one uncommitted leased revision immediately reclaimable.
func (store *Store) Release(
	ctx context.Context,
	request core.LeaseRequest,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	if err := request.Validate(false); err != nil {
		return err
	}
	if err := requireLeaseCounters(request); err != nil {
		return err
	}

	transaction, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = transaction.Rollback() }()
	current, lease, err := store.loadForUpdate(
		ctx,
		transaction,
		request.CallID,
	)
	if err != nil {
		return err
	}
	if !leaseMatches(current, lease, request) {
		return core.ErrLeaseLost
	}
	if err := store.clearLease(
		ctx,
		transaction,
		request.CallID,
	); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf(
			"postgres continuation: commit release: %w",
			err,
		)
	}
	return nil
}

func (store *Store) begin(ctx context.Context) (*stdsql.Tx, error) {
	transaction, err := store.database.BeginTx(ctx, &stdsql.TxOptions{
		Isolation: store.isolation,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"postgres continuation: begin: %w",
			err,
		)
	}
	return transaction, nil
}

func (store *Store) loadForUpdate(
	ctx context.Context,
	transaction *stdsql.Tx,
	callid core.CallID,
) (core.Snapshot, storedLease, error) {
	snapshot, lease, err := scanLeasedSnapshot(transaction.QueryRowContext(
		ctx,
		store.selectLeasedForUpdateStatement(),
		callid.String(),
	))
	if errors.Is(err, stdsql.ErrNoRows) {
		return core.Snapshot{}, storedLease{}, core.ErrNotFound
	}
	if err != nil {
		return core.Snapshot{}, storedLease{}, fmt.Errorf(
			"postgres continuation: select for update: %w",
			err,
		)
	}
	return snapshot, lease, nil
}

func (store *Store) updateSnapshot(
	ctx context.Context,
	transaction *stdsql.Tx,
	snapshot core.Snapshot,
	clearLease bool,
	expiresAt time.Time,
) error {
	encoded, columns, err := encodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	leaseColumns := ""
	if clearLease {
		leaseColumns = `,
    lease_owner = NULL,
    lease_until = NULL,
    lease_previous_revision = NULL`
	}
	var expiration any
	if !expiresAt.IsZero() {
		if !snapshot.Status().Terminal() {
			return core.ErrInvalidRetention
		}
		expiration = expiresAt.UTC()
	}
	result, err := transaction.ExecContext(
		ctx,
		`/*continuation:snapshot*/
UPDATE `+store.table+`
SET status = $2,
    revision = $3,
    fence = $4,
    sequence = $5,
    ready_at = $6,
    snapshot = $7,
    expires_at = $8`+leaseColumns+`
WHERE call_id = $1`,
		snapshot.CallID().String(),
		columns.status,
		columns.revision,
		columns.fence,
		columns.sequence,
		columns.readyAt,
		encoded,
		expiration,
	)
	if err != nil {
		return fmt.Errorf(
			"postgres continuation: update snapshot: %w",
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"postgres continuation: update result: %w",
			err,
		)
	}
	if affected != 1 {
		return fmt.Errorf(
			"%w: update affected %d rows",
			ErrCorrupt,
			affected,
		)
	}
	return nil
}

func (store *Store) updateClaim(
	ctx context.Context,
	transaction *stdsql.Tx,
	snapshot core.Snapshot,
	owner string,
	deadline time.Time,
	previousRevision uint64,
) error {
	encoded, columns, err := encodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	if err := requireBigint(
		previousRevision,
		"lease previous revision",
	); err != nil {
		return err
	}
	result, err := transaction.ExecContext(
		ctx,
		`/*continuation:claim*/
UPDATE `+store.table+`
SET status = $2,
    revision = $3,
    fence = $4,
    sequence = $5,
    ready_at = $6,
    snapshot = $7,
    lease_owner = $8,
    lease_until = $9,
    lease_previous_revision = $10
WHERE call_id = $1`,
		snapshot.CallID().String(),
		columns.status,
		columns.revision,
		columns.fence,
		columns.sequence,
		columns.readyAt,
		encoded,
		owner,
		deadline,
		int64(previousRevision),
	)
	if err != nil {
		return fmt.Errorf(
			"postgres continuation: update claim: %w",
			err,
		)
	}
	return requireOneRow(result, "claim")
}

func (store *Store) updateLeaseDeadline(
	ctx context.Context,
	transaction *stdsql.Tx,
	callid core.CallID,
	deadline time.Time,
) error {
	result, err := transaction.ExecContext(
		ctx,
		`/*continuation:lease-renew*/
UPDATE `+store.table+`
SET lease_until = $2
WHERE call_id = $1`,
		callid.String(),
		deadline,
	)
	if err != nil {
		return fmt.Errorf(
			"postgres continuation: renew lease: %w",
			err,
		)
	}
	return requireOneRow(result, "renew")
}

func (store *Store) clearLease(
	ctx context.Context,
	transaction *stdsql.Tx,
	callid core.CallID,
) error {
	result, err := transaction.ExecContext(
		ctx,
		`/*continuation:lease-clear*/
UPDATE `+store.table+`
SET lease_owner = NULL,
    lease_until = NULL,
    lease_previous_revision = NULL
WHERE call_id = $1`,
		callid.String(),
	)
	if err != nil {
		return fmt.Errorf(
			"postgres continuation: clear lease: %w",
			err,
		)
	}
	return requireOneRow(result, "clear lease")
}

func requireOneRow(result stdsql.Result, operation string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"postgres continuation: %s result: %w",
			operation,
			err,
		)
	}
	if affected != 1 {
		return fmt.Errorf(
			"%w: %s affected %d rows",
			ErrCorrupt,
			operation,
			affected,
		)
	}
	return nil
}

func (store *Store) selectStatement(forUpdate bool) string {
	statement := `SELECT call_id, status, revision, fence, sequence, ready_at, snapshot
FROM ` + store.table + `
WHERE call_id = $1`
	if forUpdate {
		statement += "\nFOR UPDATE"
	}
	return statement
}

func (store *Store) selectLeasedForUpdateStatement() string {
	return `SELECT
  call_id, status, revision, fence, sequence, ready_at, snapshot,
  lease_owner, lease_until, lease_previous_revision
FROM ` + store.table + `
WHERE call_id = $1
FOR UPDATE`
}

func (store *Store) validate(ctx context.Context) error {
	if store == nil || store.database == nil || ctx == nil {
		return core.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}

func (store *Store) validateCall(
	ctx context.Context,
	callid core.CallID,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	if callid.String() == "" {
		return core.ErrInvalidStore
	}
	return nil
}

type indexedColumns struct {
	status   string
	revision int64
	fence    int64
	sequence int64
	readyAt  any
}

type storedLease struct {
	owner            string
	deadline         time.Time
	previousRevision uint64
	valid            bool
}

type rowScanner interface {
	Scan(...any) error
}

func encodeSnapshot(
	snapshot core.Snapshot,
) ([]byte, indexedColumns, error) {
	if err := requireBigint(snapshot.Revision(), "revision"); err != nil {
		return nil, indexedColumns{}, err
	}
	if err := requireBigint(snapshot.Fence(), "fence"); err != nil {
		return nil, indexedColumns{}, err
	}
	if err := requireBigint(snapshot.Sequence(), "sequence"); err != nil {
		return nil, indexedColumns{}, err
	}
	encoded, err := core.MarshalSnapshot(snapshot)
	if err != nil {
		return nil, indexedColumns{}, err
	}
	return encoded, indexedColumns{
		status:   string(snapshot.Status()),
		revision: int64(snapshot.Revision()),
		fence:    int64(snapshot.Fence()),
		sequence: int64(snapshot.Sequence()),
		readyAt:  nullableTime(snapshot.ReadyAt()),
	}, nil
}

func scanSnapshot(scanner rowScanner) (core.Snapshot, error) {
	var (
		callid   string
		status   string
		revision int64
		fence    int64
		sequence int64
		readyAt  stdsql.NullTime
		encoded  []byte
	)
	if err := scanner.Scan(
		&callid,
		&status,
		&revision,
		&fence,
		&sequence,
		&readyAt,
		&encoded,
	); err != nil {
		return core.Snapshot{}, err
	}
	return decodeIndexedSnapshot(
		callid,
		status,
		revision,
		fence,
		sequence,
		readyAt,
		encoded,
	)
}

func scanLeasedSnapshot(
	scanner rowScanner,
) (core.Snapshot, storedLease, error) {
	var (
		callid                string
		status                string
		revision              int64
		fence                 int64
		sequence              int64
		readyAt               stdsql.NullTime
		encoded               []byte
		owner                 stdsql.NullString
		deadline              stdsql.NullTime
		leasePreviousRevision stdsql.NullInt64
	)
	if err := scanner.Scan(
		&callid,
		&status,
		&revision,
		&fence,
		&sequence,
		&readyAt,
		&encoded,
		&owner,
		&deadline,
		&leasePreviousRevision,
	); err != nil {
		return core.Snapshot{}, storedLease{}, err
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
		return core.Snapshot{}, storedLease{}, err
	}
	leaseFields := 0
	if owner.Valid {
		leaseFields++
	}
	if deadline.Valid {
		leaseFields++
	}
	if leasePreviousRevision.Valid {
		leaseFields++
	}
	switch leaseFields {
	case 0:
		return snapshot, storedLease{}, nil
	case 3:
		if owner.String == "" ||
			deadline.Time.IsZero() ||
			leasePreviousRevision.Int64 <= 0 ||
			(core.LeaseRequest{
				CallID:   snapshot.CallID(),
				Revision: snapshot.Revision(),
				Fence:    snapshot.Fence(),
				OwnerID:  owner.String,
			}).Validate(false) != nil {
			return core.Snapshot{}, storedLease{}, fmt.Errorf(
				"%w: invalid lease metadata",
				ErrCorrupt,
			)
		}
		return snapshot, storedLease{
			owner:            owner.String,
			deadline:         deadline.Time.UTC(),
			previousRevision: uint64(leasePreviousRevision.Int64),
			valid:            true,
		}, nil
	default:
		return core.Snapshot{}, storedLease{}, fmt.Errorf(
			"%w: partial lease metadata",
			ErrCorrupt,
		)
	}
}

func decodeIndexedSnapshot(
	callid string,
	status string,
	revision int64,
	fence int64,
	sequence int64,
	readyAt stdsql.NullTime,
	encoded []byte,
) (core.Snapshot, error) {
	if revision <= 0 || fence < 0 || sequence < 0 {
		return core.Snapshot{}, fmt.Errorf(
			"%w: invalid indexed counters",
			ErrCorrupt,
		)
	}
	snapshot, err := core.ParseSnapshot(encoded)
	if err != nil {
		return core.Snapshot{}, fmt.Errorf(
			"%w: %w",
			ErrCorrupt,
			err,
		)
	}
	if snapshot.CallID().String() != callid ||
		string(snapshot.Status()) != status ||
		snapshot.Revision() != uint64(revision) ||
		snapshot.Fence() != uint64(fence) ||
		snapshot.Sequence() != uint64(sequence) {
		return core.Snapshot{}, fmt.Errorf(
			"%w: indexed columns disagree with blob",
			ErrCorrupt,
		)
	}
	if readyAt.Valid != !snapshot.ReadyAt().IsZero() ||
		readyAt.Valid && !readyAt.Time.UTC().Equal(snapshot.ReadyAt()) {
		return core.Snapshot{}, fmt.Errorf(
			"%w: indexed ready-at disagrees with blob",
			ErrCorrupt,
		)
	}
	return snapshot, nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func databaseNow(
	ctx context.Context,
	transaction *stdsql.Tx,
) (time.Time, error) {
	var now time.Time
	if err := transaction.QueryRowContext(
		ctx,
		"SELECT clock_timestamp()",
	).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf(
			"postgres continuation: database time: %w",
			err,
		)
	}
	return now.UTC(), nil
}

func leaseMatches(
	current core.Snapshot,
	lease storedLease,
	request core.LeaseRequest,
) bool {
	return lease.valid &&
		lease.owner == request.OwnerID &&
		current.Revision() == request.Revision &&
		current.Fence() == request.Fence
}

func requireLeaseCounters(request core.LeaseRequest) error {
	if err := requireBigint(request.Revision, "lease revision"); err != nil {
		return err
	}
	return requireBigint(request.Fence, "lease fence")
}

func requireBigint(value uint64, field string) error {
	if value <= uint64(math.MaxInt64) {
		return nil
	}
	return errors.Join(
		core.ErrInvalidStore,
		fmt.Errorf("%w: %s", ErrBigintOverflow, field),
	)
}

func initialSnapshot(snapshot core.Snapshot) bool {
	if core.ValidateSnapshot(snapshot) != nil ||
		snapshot.CallID().String() == "" ||
		snapshot.Operation().String() == "" ||
		snapshot.Status() != core.StatusAccepted ||
		snapshot.Revision() != 1 ||
		snapshot.Fence() != 0 {
		return false
	}
	frames := snapshot.Frames()
	switch snapshot.Sequence() {
	case 0:
		return len(frames) == 0
	case 1:
		return len(frames) == 1 &&
			frames[0].Sequence() == 1 &&
			frames[0].Kind() == core.FrameAccepted
	default:
		return false
	}
}

func ready(status core.Status) bool {
	switch status {
	case core.StatusAccepted,
		core.StatusRunning,
		core.StatusSuspended,
		core.StatusCancelRequested:
		return true
	default:
		return false
	}
}

func validSuccessor(
	current core.Snapshot,
	next core.Snapshot,
	fence uint64,
) bool {
	if core.ValidateSnapshot(next) != nil ||
		next.CallID() != current.CallID() ||
		next.Operation() != current.Operation() ||
		current.Revision() == math.MaxUint64 ||
		next.Revision() != current.Revision()+1 ||
		next.Fence() != fence ||
		next.Sequence() < current.Sequence() {
		return false
	}
	currentFrames := current.Frames()
	nextFrames := next.Frames()
	if len(nextFrames) < len(currentFrames) {
		return false
	}
	for index, frame := range currentFrames {
		if !equalFrame(frame, nextFrames[index]) {
			return false
		}
	}
	for index := len(currentFrames); index < len(nextFrames); index++ {
		if nextFrames[index].Sequence() != uint64(index+1) {
			return false
		}
	}
	return true
}

func equalFrame(first, second core.Frame) bool {
	return first.Sequence() == second.Sequence() &&
		first.Kind() == second.Kind() &&
		bytes.Equal(first.Payload(), second.Payload())
}

func readyIndexName(table string) string {
	base := strings.ReplaceAll(table, ".", "_") + "_ready_lease_idx"
	if len(base) <= 63 && validIdentifier(base) {
		return base
	}
	sum := sha256.Sum256([]byte(table))
	return fmt.Sprintf("keelith_continuation_%x_ready_idx", sum[:8])
}

func expiryIndexName(table string) string {
	base := strings.ReplaceAll(table, ".", "_") + "_expiry_idx"
	if len(base) <= 63 && validIdentifier(base) {
		return base
	}
	sum := sha256.Sum256([]byte(table))
	return fmt.Sprintf("keelith_continuation_%x_expiry_idx", sum[:8])
}

func timerReadyIndexName(table string) string {
	base := strings.ReplaceAll(table, ".", "_") + "_ready_timer_idx"
	if len(base) <= 63 && validIdentifier(base) {
		return base
	}
	sum := sha256.Sum256([]byte(table))
	return fmt.Sprintf("keelith_continuation_%x_timer_idx", sum[:8])
}

func quoteTable(value string) (string, error) {
	parts := strings.Split(value, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return "", fmt.Errorf("%w: table name", ErrInvalidOption)
	}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		if !validIdentifier(part) {
			return "", fmt.Errorf(
				"%w: table identifier",
				ErrInvalidOption,
			)
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
