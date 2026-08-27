// Package projection provides a durable bbolt-backed projection Store.
package projection

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	core "github.com/keelab/keelith/programmable/projection"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

const (
	defaultOpenTimeout    = time.Second
	maxPathBytes          = 4 * 1024
	maxProjectionBuckets  = 4 * 1024
	maxSnapshotStaging    = 64
	snapshotIdentifierLen = 16
)

var (
	// ErrInvalidOption reports an invalid path or Store option.
	ErrInvalidOption = errors.New("projection bbolt: invalid option")
	// ErrFileLocked reports that another process or Store owns the database.
	ErrFileLocked = errors.New("projection bbolt: database file is locked")
	// ErrStoreClosed reports access after Store.Close.
	ErrStoreClosed = errors.New("projection bbolt: store closed")
	// ErrCorrupt reports malformed or internally inconsistent persisted state.
	ErrCorrupt = errors.New("projection bbolt: corrupt state")
	// ErrBucketLimit reports an exhausted projection or staging bucket budget.
	ErrBucketLimit = errors.New("projection bbolt: bucket limit exceeded")

	bucketRoot        = []byte("keelith.projection.v1")
	bucketProjects    = []byte("projects")
	bucketMeta        = []byte("meta")
	bucketGenerations = []byte("generations")
	bucketStaging     = []byte("staging")
	bucketRows        = []byte("rows")

	metaSchemaid       = []byte("schema_id")
	metaFingerprint    = []byte("fingerprint")
	metaKeyFingerprint = []byte("key_fingerprint")
	metaCursor         = []byte("cursor")
	metaGeneration     = []byte("generation")
	metaSourceTime     = []byte("source_time")
	metaAppliedAt      = []byte("applied_at")
	metaLastDelta      = []byte("last_delta")

	stagingBaseGeneration = []byte("base_generation")
)

var _ core.Store = (*Store)(nil)

// Option configures Open.
type Option func(*settings) error

type settings struct {
	clock   func() time.Time
	timeout time.Duration
	mode    os.FileMode
}

// WithClock replaces the checkpoint application clock.
func WithClock(clock func() time.Time) Option {
	return func(settings *settings) error {
		if clock == nil {
			return fmt.Errorf("%w: clock is nil", ErrInvalidOption)
		}
		settings.clock = clock
		return nil
	}
}

// WithTimeout bounds how long Open waits for the database file lock.
func WithTimeout(timeout time.Duration) Option {
	return func(settings *settings) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: lock timeout", ErrInvalidOption)
		}
		settings.timeout = timeout
		return nil
	}
}

// Store persists immutable visible generations in one bbolt database.
type Store struct {
	mu     sync.RWMutex
	db     *bolt.DB
	now    func() time.Time
	closed bool
}

// Open opens or creates a Store and removes durable orphan staging buckets.
func Open(path string, options ...Option) (*Store, error) {
	if strings.TrimSpace(path) == "" || len(path) > maxPathBytes {
		return nil, fmt.Errorf("%w: database path", ErrInvalidOption)
	}
	config := settings{
		clock: func() time.Time {
			return time.Now().UTC()
		},
		timeout: defaultOpenTimeout,
		mode:    0o600,
	}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}

	database, err := bolt.Open(path, config.mode, &bolt.Options{
		Timeout: config.timeout,
	})
	if err != nil {
		if errors.Is(err, berrors.ErrTimeout) {
			return nil, fmt.Errorf("%w: %q", ErrFileLocked, path)
		}
		return nil, fmt.Errorf("projection bbolt: open %q: %w", path, err)
	}
	store := &Store{
		db:  database,
		now: config.clock,
	}
	if err := store.initialize(); err != nil {
		_ = database.Close()
		return nil, err
	}
	return store, nil
}

// Close releases the database and its process-level file lock.
func (store *Store) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	if err := store.db.Close(); err != nil &&
		!errors.Is(err, berrors.ErrDatabaseNotOpen) {
		return fmt.Errorf("projection bbolt: close: %w", err)
	}
	return nil
}

// BeginSnapshot creates an isolated durable staging generation.
func (store *Store) BeginSnapshot(
	ctx context.Context,
	schema core.Schema,
) (core.SnapshotTxn, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if len(schema.ID) > bolt.MaxKeySize {
		return nil, fmt.Errorf("%w: projection id", ErrInvalidOption)
	}
	identifier := make([]byte, snapshotIdentifierLen)
	if _, err := rand.Read(identifier); err != nil {
		return nil, fmt.Errorf("projection bbolt: snapshot ID: %w", err)
	}

	var baseGeneration uint64
	err := store.update(ctx, func(transaction *bolt.Tx) error {
		projects, err := projectsBucket(transaction)
		if err != nil {
			return err
		}
		project := projects.Bucket([]byte(schema.ID))
		if project == nil {
			count, err := childBucketCount(projects)
			if err != nil {
				return err
			}
			if count >= maxProjectionBuckets {
				return ErrBucketLimit
			}
			project, err = projects.CreateBucket([]byte(schema.ID))
			if err != nil {
				return err
			}
		}
		if err := ensureProjectBuckets(project); err != nil {
			return err
		}
		checkpoint, exists, _, err := readCheckpoint(project, schema.ID)
		if err != nil {
			return err
		}
		if exists {
			if err := compatibleSchema(checkpoint.Schema, schema); err != nil {
				return err
			}
			baseGeneration = checkpoint.Generation
		}

		staging := project.Bucket(bucketStaging)
		count, err := childBucketCount(staging)
		if err != nil {
			return err
		}
		if count >= maxSnapshotStaging {
			return ErrBucketLimit
		}
		pending, err := staging.CreateBucket(identifier)
		if err != nil {
			return err
		}
		if err := putUint64(
			pending,
			stagingBaseGeneration,
			baseGeneration,
		); err != nil {
			return err
		}
		if err := putSchema(pending, schema); err != nil {
			return err
		}
		_, err = pending.CreateBucket(bucketRows)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &snapshotTxn{
		store:          store,
		schema:         schema,
		identifier:     append([]byte(nil), identifier...),
		baseGeneration: baseGeneration,
	}, nil
}

// ApplyDelta atomically advances rows and their durable checkpoint.
func (store *Store) ApplyDelta(
	ctx context.Context,
	batch core.DeltaBatch,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := batch.Validate(); err != nil {
		return err
	}
	batch = batch.Clone()
	digest := deltaDigest(batch)

	return store.update(ctx, func(transaction *bolt.Tx) error {
		project := projectionBucket(transaction, batch.Schema.ID)
		if project == nil {
			return fmt.Errorf(
				"%w: %q",
				core.ErrProjectionNotFound,
				batch.Schema.ID,
			)
		}
		checkpoint, exists, lastDelta, err := readCheckpoint(
			project,
			batch.Schema.ID,
		)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf(
				"%w: %q",
				core.ErrProjectionNotFound,
				batch.Schema.ID,
			)
		}
		if err := compatibleSchema(
			checkpoint.Schema,
			batch.Schema,
		); err != nil {
			return err
		}
		if checkpoint.Cursor == batch.Cursor {
			if len(lastDelta) == sha256.Size &&
				bytes.Equal(lastDelta, digest[:]) {
				return nil
			}
			return fmt.Errorf(
				"%w: projection %q cursor %q",
				core.ErrReplayConflict,
				batch.Schema.ID,
				batch.Cursor,
			)
		}
		if checkpoint.Cursor != batch.Previous {
			return &core.CursorMismatchError{
				Projection: batch.Schema.ID,
				Expected:   checkpoint.Cursor,
				Actual:     batch.Previous,
			}
		}
		if checkpoint.Generation == math.MaxUint64 {
			return fmt.Errorf("%w: generation exhausted", ErrCorrupt)
		}

		nextGeneration := checkpoint.Generation + 1
		generations := project.Bucket(bucketGenerations)
		currentRows := generations.Bucket(
			encodeUint64(checkpoint.Generation),
		)
		if currentRows == nil {
			return fmt.Errorf(
				"%w: current generation %d is missing",
				ErrCorrupt,
				checkpoint.Generation,
			)
		}
		nextName := encodeUint64(nextGeneration)
		if generations.Bucket(nextName) != nil {
			return fmt.Errorf(
				"%w: generation %d already exists",
				ErrCorrupt,
				nextGeneration,
			)
		}
		nextRows, err := generations.CreateBucket(nextName)
		if err != nil {
			return err
		}
		if err := copyRows(currentRows, nextRows); err != nil {
			return err
		}
		for _, mutation := range batch.Mutations {
			key := mutation.Key()
			switch mutation.Kind() {
			case core.MutationUpsert:
				if err := nextRows.Put(key, mutation.Value()); err != nil {
					return err
				}
			case core.MutationDelete:
				if err := nextRows.Delete(key); err != nil {
					return err
				}
			default:
				return core.ErrInvalidMutation
			}
		}
		nextCheckpoint := core.Checkpoint{
			Schema:     batch.Schema,
			Cursor:     batch.Cursor,
			Generation: nextGeneration,
			SourceTime: batch.SourceTime,
			AppliedAt:  store.now().UTC(),
		}
		if err := writeCheckpoint(
			project,
			nextCheckpoint,
			digest[:],
		); err != nil {
			return err
		}
		return generations.DeleteBucket(
			encodeUint64(checkpoint.Generation),
		)
	})
}

// Get returns an independent copy from the current visible generation.
func (store *Store) Get(
	ctx context.Context,
	id core.ProjectionID,
	key []byte,
) ([]byte, bool, error) {
	if err := validateContext(ctx); err != nil {
		return nil, false, err
	}
	if err := id.Validate(); err != nil {
		return nil, false, err
	}
	if err := core.Upsert(key, nil).Validate(); err != nil {
		return nil, false, err
	}

	var result []byte
	var exists bool
	err := store.view(ctx, func(transaction *bolt.Tx) error {
		project := projectionBucket(transaction, id)
		if project == nil {
			return nil
		}
		checkpoint, visible, _, err := readCheckpoint(project, id)
		if err != nil {
			return err
		}
		if !visible {
			return nil
		}
		generations := project.Bucket(bucketGenerations)
		rows := generations.Bucket(
			encodeUint64(checkpoint.Generation),
		)
		if rows == nil {
			return fmt.Errorf(
				"%w: current generation %d is missing",
				ErrCorrupt,
				checkpoint.Generation,
			)
		}
		foundKey, value := rows.Cursor().Seek(key)
		if !bytes.Equal(foundKey, key) {
			return nil
		}
		result = append([]byte(nil), value...)
		exists = true
		return nil
	})
	return result, exists, err
}

// Checkpoint returns the current visible generation and freshness watermark.
func (store *Store) Checkpoint(
	ctx context.Context,
	id core.ProjectionID,
) (core.Checkpoint, bool, error) {
	if err := validateContext(ctx); err != nil {
		return core.Checkpoint{}, false, err
	}
	if err := id.Validate(); err != nil {
		return core.Checkpoint{}, false, err
	}

	var result core.Checkpoint
	var exists bool
	err := store.view(ctx, func(transaction *bolt.Tx) error {
		project := projectionBucket(transaction, id)
		if project == nil {
			return nil
		}
		var err error
		result, exists, _, err = readCheckpoint(project, id)
		return err
	})
	return result, exists, err
}

type snapshotTxn struct {
	mu             sync.Mutex
	store          *Store
	schema         core.Schema
	identifier     []byte
	baseGeneration uint64
	closed         bool
}

func (transaction *snapshotTxn) Stage(mutation core.Mutation) error {
	if transaction == nil {
		return core.ErrSnapshotClosed
	}
	if err := mutation.Validate(); err != nil {
		return err
	}
	mutation = mutation.Clone()

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return core.ErrSnapshotClosed
	}
	return transaction.store.update(context.Background(), func(databaseTxn *bolt.Tx) error {
		pending, err := transaction.pendingBucket(databaseTxn)
		if err != nil {
			return err
		}
		rows := pending.Bucket(bucketRows)
		if rows == nil {
			return fmt.Errorf("%w: staging rows missing", ErrCorrupt)
		}
		key := mutation.Key()
		switch mutation.Kind() {
		case core.MutationUpsert:
			return rows.Put(key, mutation.Value())
		case core.MutationDelete:
			return rows.Delete(key)
		default:
			return core.ErrInvalidMutation
		}
	})
}

func (transaction *snapshotTxn) Commit(
	ctx context.Context,
	cursor core.Cursor,
	sourceTime time.Time,
) error {
	if transaction == nil {
		return core.ErrSnapshotClosed
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := cursor.Validate(); err != nil {
		return err
	}
	if sourceTime.IsZero() {
		return fmt.Errorf("%w: source time is zero", ErrInvalidOption)
	}

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return core.ErrSnapshotClosed
	}

	var conflict error
	err := transaction.store.update(ctx, func(databaseTxn *bolt.Tx) error {
		project := projectionBucket(databaseTxn, transaction.schema.ID)
		if project == nil {
			return fmt.Errorf("%w: projection bucket missing", ErrCorrupt)
		}
		pending, err := transaction.pendingBucket(databaseTxn)
		if err != nil {
			return err
		}
		checkpoint, exists, _, err := readCheckpoint(
			project,
			transaction.schema.ID,
		)
		if err != nil {
			return err
		}
		currentGeneration := uint64(0)
		if exists {
			if err := compatibleSchema(
				checkpoint.Schema,
				transaction.schema,
			); err != nil {
				return err
			}
			currentGeneration = checkpoint.Generation
		}
		if currentGeneration != transaction.baseGeneration {
			conflict = fmt.Errorf(
				"%w: projection %q expected generation %d, got %d",
				core.ErrSnapshotConflict,
				transaction.schema.ID,
				transaction.baseGeneration,
				currentGeneration,
			)
			return project.Bucket(bucketStaging).DeleteBucket(
				transaction.identifier,
			)
		}
		if currentGeneration == math.MaxUint64 {
			return fmt.Errorf("%w: generation exhausted", ErrCorrupt)
		}

		nextGeneration := currentGeneration + 1
		generations := project.Bucket(bucketGenerations)
		nextName := encodeUint64(nextGeneration)
		if generations.Bucket(nextName) != nil {
			return fmt.Errorf(
				"%w: generation %d already exists",
				ErrCorrupt,
				nextGeneration,
			)
		}
		nextRows, err := generations.CreateBucket(nextName)
		if err != nil {
			return err
		}
		if err := copyRows(pending.Bucket(bucketRows), nextRows); err != nil {
			return err
		}
		nextCheckpoint := core.Checkpoint{
			Schema:     transaction.schema,
			Cursor:     cursor,
			Generation: nextGeneration,
			SourceTime: sourceTime,
			AppliedAt:  transaction.store.now().UTC(),
		}
		if err := writeCheckpoint(project, nextCheckpoint, nil); err != nil {
			return err
		}
		if err := project.Bucket(bucketStaging).DeleteBucket(
			transaction.identifier,
		); err != nil {
			return err
		}
		if currentGeneration != 0 {
			if err := generations.DeleteBucket(
				encodeUint64(currentGeneration),
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	transaction.closed = true
	if conflict != nil {
		return conflict
	}
	return nil
}

func (transaction *snapshotTxn) Abort() error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return nil
	}
	transaction.closed = true
	err := transaction.store.update(context.Background(), func(databaseTxn *bolt.Tx) error {
		project := projectionBucket(databaseTxn, transaction.schema.ID)
		if project == nil {
			return nil
		}
		staging := project.Bucket(bucketStaging)
		if staging == nil || staging.Bucket(transaction.identifier) == nil {
			return nil
		}
		if err := staging.DeleteBucket(transaction.identifier); err != nil {
			return err
		}
		count, err := childBucketCount(staging)
		if err != nil || count != 0 || project.Bucket(bucketMeta) != nil {
			return err
		}
		root := databaseTxn.Bucket(bucketRoot)
		if root == nil {
			return fmt.Errorf("%w: root bucket missing", ErrCorrupt)
		}
		projects := root.Bucket(bucketProjects)
		if projects == nil {
			return fmt.Errorf("%w: projects bucket missing", ErrCorrupt)
		}
		return projects.DeleteBucket([]byte(transaction.schema.ID))
	})
	return err
}

func (transaction *snapshotTxn) pendingBucket(
	databaseTxn *bolt.Tx,
) (*bolt.Bucket, error) {
	project := projectionBucket(databaseTxn, transaction.schema.ID)
	if project == nil {
		return nil, core.ErrSnapshotClosed
	}
	staging := project.Bucket(bucketStaging)
	if staging == nil {
		return nil, core.ErrSnapshotClosed
	}
	pending := staging.Bucket(transaction.identifier)
	if pending == nil {
		return nil, core.ErrSnapshotClosed
	}
	base, err := requiredUint64(pending, stagingBaseGeneration)
	if err != nil {
		return nil, err
	}
	if base != transaction.baseGeneration {
		return nil, fmt.Errorf("%w: staging base generation", ErrCorrupt)
	}
	schema, err := readSchema(pending, transaction.schema.ID)
	if err != nil {
		return nil, err
	}
	if err := compatibleSchema(transaction.schema, schema); err != nil {
		return nil, err
	}
	return pending, nil
}

func (store *Store) initialize() error {
	err := store.db.Update(func(transaction *bolt.Tx) error {
		projects, err := projectsBucket(transaction)
		if err != nil {
			return err
		}
		names := make([][]byte, 0)
		if err := projects.ForEach(func(name, value []byte) error {
			if value == nil {
				names = append(names, append([]byte(nil), name...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, name := range names {
			project := projects.Bucket(name)
			if project == nil {
				continue
			}
			if project.Bucket(bucketStaging) != nil {
				if err := project.DeleteBucket(bucketStaging); err != nil {
					return err
				}
			}
			if project.Bucket(bucketMeta) == nil {
				if err := projects.DeleteBucket(name); err != nil {
					return err
				}
				continue
			}
			if _, err := project.CreateBucket(bucketStaging); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("projection bbolt: initialize: %w", err)
	}
	return nil
}

func (store *Store) view(
	ctx context.Context,
	function func(*bolt.Tx) error,
) error {
	if store == nil {
		return ErrStoreClosed
	}
	if err := validateOptionalContext(ctx); err != nil {
		return err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed || store.db == nil {
		return ErrStoreClosed
	}
	if err := store.db.View(function); err != nil {
		return normalizeBoltError(err)
	}
	return nil
}

func (store *Store) update(
	ctx context.Context,
	function func(*bolt.Tx) error,
) error {
	if store == nil {
		return ErrStoreClosed
	}
	if err := validateOptionalContext(ctx); err != nil {
		return err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed || store.db == nil {
		return ErrStoreClosed
	}
	if err := store.db.Update(function); err != nil {
		return normalizeBoltError(err)
	}
	return nil
}

func projectsBucket(transaction *bolt.Tx) (*bolt.Bucket, error) {
	root, err := transaction.CreateBucketIfNotExists(bucketRoot)
	if err != nil {
		return nil, err
	}
	return root.CreateBucketIfNotExists(bucketProjects)
}

func projectionBucket(
	transaction *bolt.Tx,
	id core.ProjectionID,
) *bolt.Bucket {
	root := transaction.Bucket(bucketRoot)
	if root == nil {
		return nil
	}
	projects := root.Bucket(bucketProjects)
	if projects == nil {
		return nil
	}
	return projects.Bucket([]byte(id))
}

func ensureProjectBuckets(project *bolt.Bucket) error {
	if _, err := project.CreateBucketIfNotExists(
		bucketGenerations,
	); err != nil {
		return err
	}
	_, err := project.CreateBucketIfNotExists(bucketStaging)
	return err
}

func childBucketCount(bucket *bolt.Bucket) (int, error) {
	if bucket == nil {
		return 0, fmt.Errorf("%w: bucket missing", ErrCorrupt)
	}
	count := 0
	err := bucket.ForEach(func(_, value []byte) error {
		if value == nil {
			count++
		}
		return nil
	})
	return count, err
}

func copyRows(source, destination *bolt.Bucket) error {
	if source == nil || destination == nil {
		return fmt.Errorf("%w: rows bucket missing", ErrCorrupt)
	}
	return source.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("%w: nested rows bucket", ErrCorrupt)
		}
		if len(key) == 0 || len(key) > bolt.MaxKeySize {
			return fmt.Errorf("%w: invalid row key", ErrCorrupt)
		}
		return destination.Put(key, value)
	})
}

func writeCheckpoint(
	project *bolt.Bucket,
	checkpoint core.Checkpoint,
	lastDelta []byte,
) error {
	if checkpoint.Generation == 0 ||
		checkpoint.SourceTime.IsZero() ||
		checkpoint.AppliedAt.IsZero() {
		return fmt.Errorf("%w: invalid checkpoint", ErrCorrupt)
	}
	if err := checkpoint.Schema.Validate(); err != nil {
		return err
	}
	if err := checkpoint.Cursor.Validate(); err != nil {
		return err
	}
	meta, err := project.CreateBucketIfNotExists(bucketMeta)
	if err != nil {
		return err
	}
	if err := putSchema(meta, checkpoint.Schema); err != nil {
		return err
	}
	if err := meta.Put(metaCursor, []byte(checkpoint.Cursor)); err != nil {
		return err
	}
	if err := putUint64(
		meta,
		metaGeneration,
		checkpoint.Generation,
	); err != nil {
		return err
	}
	if err := putTime(meta, metaSourceTime, checkpoint.SourceTime); err != nil {
		return err
	}
	if err := putTime(meta, metaAppliedAt, checkpoint.AppliedAt); err != nil {
		return err
	}
	if len(lastDelta) == 0 {
		return meta.Delete(metaLastDelta)
	}
	if len(lastDelta) != sha256.Size {
		return fmt.Errorf("%w: invalid delta digest", ErrCorrupt)
	}
	return meta.Put(metaLastDelta, lastDelta)
}

func readCheckpoint(
	project *bolt.Bucket,
	id core.ProjectionID,
) (core.Checkpoint, bool, []byte, error) {
	meta := project.Bucket(bucketMeta)
	if meta == nil {
		return core.Checkpoint{}, false, nil, nil
	}
	schema, err := readSchema(meta, id)
	if err != nil {
		return core.Checkpoint{}, false, nil, err
	}
	cursorBytes, err := requiredValue(meta, metaCursor)
	if err != nil {
		return core.Checkpoint{}, false, nil, err
	}
	generation, err := requiredUint64(meta, metaGeneration)
	if err != nil {
		return core.Checkpoint{}, false, nil, err
	}
	sourceTime, err := requiredTime(meta, metaSourceTime)
	if err != nil {
		return core.Checkpoint{}, false, nil, err
	}
	appliedAt, err := requiredTime(meta, metaAppliedAt)
	if err != nil {
		return core.Checkpoint{}, false, nil, err
	}
	checkpoint := core.Checkpoint{
		Schema:     schema,
		Cursor:     core.Cursor(cursorBytes),
		Generation: generation,
		SourceTime: sourceTime,
		AppliedAt:  appliedAt,
	}
	if err := checkpoint.Cursor.Validate(); err != nil ||
		generation == 0 ||
		sourceTime.IsZero() ||
		appliedAt.IsZero() {
		return core.Checkpoint{}, false, nil,
			fmt.Errorf("%w: invalid checkpoint", ErrCorrupt)
	}
	lastDelta := append([]byte(nil), meta.Get(metaLastDelta)...)
	if len(lastDelta) != 0 && len(lastDelta) != sha256.Size {
		return core.Checkpoint{}, false, nil,
			fmt.Errorf("%w: invalid delta digest", ErrCorrupt)
	}
	return checkpoint, true, lastDelta, nil
}

func putSchema(bucket *bolt.Bucket, schema core.Schema) error {
	values := []struct {
		key   []byte
		value string
	}{
		{metaSchemaid, string(schema.ID)},
		{metaFingerprint, schema.Fingerprint},
		{metaKeyFingerprint, schema.KeyFingerprint},
	}
	for _, value := range values {
		if err := bucket.Put(value.key, []byte(value.value)); err != nil {
			return err
		}
	}
	return nil
}

func readSchema(
	bucket *bolt.Bucket,
	id core.ProjectionID,
) (core.Schema, error) {
	schemaid, err := requiredValue(bucket, metaSchemaid)
	if err != nil {
		return core.Schema{}, err
	}
	fingerprint, err := requiredValue(bucket, metaFingerprint)
	if err != nil {
		return core.Schema{}, err
	}
	keyFingerprint, err := requiredValue(bucket, metaKeyFingerprint)
	if err != nil {
		return core.Schema{}, err
	}
	schema := core.Schema{
		ID:             core.ProjectionID(schemaid),
		Fingerprint:    string(fingerprint),
		KeyFingerprint: string(keyFingerprint),
	}
	if err := schema.Validate(); err != nil {
		return core.Schema{}, fmt.Errorf("%w: schema: %w", ErrCorrupt, err)
	}
	if schema.ID != id {
		return core.Schema{}, fmt.Errorf(
			"%w: schema projection id %q, bucket %q",
			ErrCorrupt,
			schema.ID,
			id,
		)
	}
	return schema, nil
}

func requiredValue(bucket *bolt.Bucket, key []byte) ([]byte, error) {
	value := bucket.Get(key)
	if len(value) == 0 {
		return nil, fmt.Errorf("%w: metadata %q missing", ErrCorrupt, key)
	}
	return append([]byte(nil), value...), nil
}

func putUint64(bucket *bolt.Bucket, key []byte, value uint64) error {
	return bucket.Put(key, encodeUint64(value))
}

func encodeUint64(value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return encoded[:]
}

func requiredUint64(bucket *bolt.Bucket, key []byte) (uint64, error) {
	value := bucket.Get(key)
	if len(value) != 8 {
		return 0, fmt.Errorf("%w: invalid metadata %q", ErrCorrupt, key)
	}
	return binary.BigEndian.Uint64(value), nil
}

func putTime(bucket *bolt.Bucket, key []byte, value time.Time) error {
	encoded, err := value.MarshalBinary()
	if err != nil {
		return fmt.Errorf("projection bbolt: encode time: %w", err)
	}
	return bucket.Put(key, encoded)
}

func requiredTime(bucket *bolt.Bucket, key []byte) (time.Time, error) {
	value, err := requiredValue(bucket, key)
	if err != nil {
		return time.Time{}, err
	}
	var decoded time.Time
	if err := decoded.UnmarshalBinary(value); err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid time %q", ErrCorrupt, key)
	}
	return decoded, nil
}

func compatibleSchema(expected, actual core.Schema) error {
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
			Projection: actual.ID,
			Field:      "fingerprint",
			Expected:   expected.Fingerprint,
			Actual:     actual.Fingerprint,
		}
	}
	if expected.KeyFingerprint != actual.KeyFingerprint {
		return &core.SchemaMismatchError{
			Projection: actual.ID,
			Field:      "key_fingerprint",
			Expected:   expected.KeyFingerprint,
			Actual:     actual.KeyFingerprint,
		}
	}
	return nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}

func validateOptionalContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}

func normalizeBoltError(err error) error {
	if errors.Is(err, berrors.ErrDatabaseNotOpen) {
		return fmt.Errorf("%w: %w", ErrStoreClosed, err)
	}
	return err
}

func deltaDigest(batch core.DeltaBatch) [sha256.Size]byte {
	digest := sha256.New()
	writeDigestString(digest, string(batch.Schema.ID))
	writeDigestString(digest, batch.Schema.Fingerprint)
	writeDigestString(digest, batch.Schema.KeyFingerprint)
	writeDigestString(digest, string(batch.Previous))
	writeDigestString(digest, string(batch.Cursor))
	writeDigestUint64(digest, uint64(batch.SourceTime.UnixNano()))
	for _, mutation := range batch.Mutations {
		writeDigestUint64(digest, uint64(mutation.Kind()))
		writeDigestBytes(digest, mutation.Key())
		writeDigestBytes(digest, mutation.Value())
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeDigestString(digest hash.Hash, value string) {
	writeDigestBytes(digest, []byte(value))
}

func writeDigestBytes(digest hash.Hash, value []byte) {
	writeDigestUint64(digest, uint64(len(value)))
	_, _ = digest.Write(value)
}

func writeDigestUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}
