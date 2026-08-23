// Package memory provides an in-process projection Store.
package memory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"sync"
	"time"

	"github.com/keelab/keelith/programmable/projection"
)

var _ projection.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// WithClock replaces the Store's checkpoint clock.
func WithClock(clock func() time.Time) Option {
	return func(s *Store) {
		if clock != nil {
			s.now = clock
		}
	}
}

// Store keeps independent immutable projection generations in memory.
type Store struct {
	mu     sync.RWMutex
	states map[projection.ProjectionID]state
	now    func() time.Time
}

type state struct {
	schema       projection.Schema
	cursor       projection.Cursor
	generation   uint64
	sourceTime   time.Time
	appliedAt    time.Time
	values       map[string][]byte
	lastDelta    [sha256.Size]byte
	hasLastDelta bool
}

// New creates an empty Store.
func New(options ...Option) *Store {
	s := &Store{
		states: make(map[projection.ProjectionID]state),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
	for _, option := range options {
		if option != nil {
			option(s)
		}
	}
	return s
}

// BeginSnapshot creates an isolated empty staging generation.
func (s *Store) BeginSnapshot(
	ctx context.Context,
	schema projection.Schema,
) (projection.SnapshotTxn, error) {
	if s == nil {
		return nil, errors.New("projection memory: store is nil")
	}
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	current, exists := s.states[schema.ID]
	s.mu.RUnlock()
	if exists {
		if err := compatibleSchema(current.schema, schema); err != nil {
			return nil, err
		}
	}
	baseGeneration := uint64(0)
	if exists {
		baseGeneration = current.generation
	}
	return &snapshotTxn{
		store:          s,
		schema:         schema,
		baseGeneration: baseGeneration,
		values:         make(map[string][]byte),
	}, nil
}

// ApplyDelta atomically advances rows and their durable checkpoint.
func (s *Store) ApplyDelta(
	ctx context.Context,
	batch projection.DeltaBatch,
) error {
	if s == nil {
		return errors.New("projection memory: store is nil")
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := batch.Validate(); err != nil {
		return err
	}
	batch = batch.Clone()
	digest := deltaDigest(batch)

	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.states[batch.Schema.ID]
	if !exists {
		return fmt.Errorf(
			"%w: %q",
			projection.ErrProjectionNotFound,
			batch.Schema.ID,
		)
	}
	if err := compatibleSchema(current.schema, batch.Schema); err != nil {
		return err
	}
	if current.cursor == batch.Cursor {
		if current.hasLastDelta && current.lastDelta == digest {
			return nil
		}
		return fmt.Errorf(
			"%w: projection %q cursor %q",
			projection.ErrReplayConflict,
			batch.Schema.ID,
			batch.Cursor,
		)
	}
	if current.cursor != batch.Previous {
		return &projection.CursorMismatchError{
			Projection: batch.Schema.ID,
			Expected:   current.cursor,
			Actual:     batch.Previous,
		}
	}

	nextValues := cloneValues(current.values)
	for _, mutation := range batch.Mutations {
		key := string(mutation.Key())
		switch mutation.Kind() {
		case projection.MutationUpsert:
			nextValues[key] = mutation.Value()
		case projection.MutationDelete:
			delete(nextValues, key)
		default:
			return projection.ErrInvalidMutation
		}
	}
	s.states[batch.Schema.ID] = state{
		schema:       batch.Schema,
		cursor:       batch.Cursor,
		generation:   current.generation + 1,
		sourceTime:   batch.SourceTime,
		appliedAt:    s.now().UTC(),
		values:       nextValues,
		lastDelta:    digest,
		hasLastDelta: true,
	}
	return nil
}

// Get returns an independent visible value copy.
func (s *Store) Get(
	ctx context.Context,
	id projection.ProjectionID,
	key []byte,
) ([]byte, bool, error) {
	if s == nil {
		return nil, false, errors.New("projection memory: store is nil")
	}
	if err := validateContext(ctx); err != nil {
		return nil, false, err
	}
	if err := id.Validate(); err != nil {
		return nil, false, err
	}
	if err := projection.Upsert(key, nil).Validate(); err != nil {
		return nil, false, err
	}
	s.mu.RLock()
	current, exists := s.states[id]
	if !exists {
		s.mu.RUnlock()
		return nil, false, nil
	}
	value, exists := current.values[string(key)]
	result := append([]byte(nil), value...)
	s.mu.RUnlock()
	return result, exists, nil
}

// Checkpoint returns the current visible generation and freshness watermark.
func (s *Store) Checkpoint(
	ctx context.Context,
	id projection.ProjectionID,
) (projection.Checkpoint, bool, error) {
	if s == nil {
		return projection.Checkpoint{}, false,
			errors.New("projection memory: store is nil")
	}
	if err := validateContext(ctx); err != nil {
		return projection.Checkpoint{}, false, err
	}
	if err := id.Validate(); err != nil {
		return projection.Checkpoint{}, false, err
	}
	s.mu.RLock()
	current, exists := s.states[id]
	s.mu.RUnlock()
	if !exists {
		return projection.Checkpoint{}, false, nil
	}
	return projection.Checkpoint{
		Schema:     current.schema,
		Cursor:     current.cursor,
		Generation: current.generation,
		SourceTime: current.sourceTime,
		AppliedAt:  current.appliedAt,
	}, true, nil
}

type snapshotTxn struct {
	mu             sync.Mutex
	store          *Store
	schema         projection.Schema
	baseGeneration uint64
	values         map[string][]byte
	closed         bool
}

func (transaction *snapshotTxn) Stage(mutation projection.Mutation) error {
	if transaction == nil {
		return projection.ErrSnapshotClosed
	}
	if err := mutation.Validate(); err != nil {
		return err
	}
	mutation = mutation.Clone()
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return projection.ErrSnapshotClosed
	}
	key := string(mutation.Key())
	switch mutation.Kind() {
	case projection.MutationUpsert:
		transaction.values[key] = mutation.Value()
	case projection.MutationDelete:
		delete(transaction.values, key)
	default:
		return projection.ErrInvalidMutation
	}
	return nil
}

func (transaction *snapshotTxn) Commit(
	ctx context.Context,
	cursor projection.Cursor,
	sourceTime time.Time,
) error {
	if transaction == nil {
		return projection.ErrSnapshotClosed
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := cursor.Validate(); err != nil {
		return err
	}
	if sourceTime.IsZero() {
		return errors.New("projection memory: source time is zero")
	}

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return projection.ErrSnapshotClosed
	}
	transaction.store.mu.Lock()
	defer transaction.store.mu.Unlock()

	current, exists := transaction.store.states[transaction.schema.ID]
	currentGeneration := uint64(0)
	if exists {
		currentGeneration = current.generation
		if err := compatibleSchema(current.schema, transaction.schema); err != nil {
			return err
		}
	}
	if currentGeneration != transaction.baseGeneration {
		transaction.closed = true
		return fmt.Errorf(
			"%w: projection %q expected generation %d, got %d",
			projection.ErrSnapshotConflict,
			transaction.schema.ID,
			transaction.baseGeneration,
			currentGeneration,
		)
	}
	transaction.store.states[transaction.schema.ID] = state{
		schema:     transaction.schema,
		cursor:     cursor,
		generation: currentGeneration + 1,
		sourceTime: sourceTime,
		appliedAt:  transaction.store.now().UTC(),
		values:     cloneValues(transaction.values),
	}
	transaction.closed = true
	transaction.values = nil
	return nil
}

func (transaction *snapshotTxn) Abort() error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	transaction.closed = true
	transaction.values = nil
	transaction.mu.Unlock()
	return nil
}

func compatibleSchema(expected, actual projection.Schema) error {
	if expected.Fingerprint != actual.Fingerprint {
		return &projection.SchemaMismatchError{
			Projection: actual.ID,
			Field:      "fingerprint",
			Expected:   expected.Fingerprint,
			Actual:     actual.Fingerprint,
		}
	}
	if expected.KeyFingerprint != actual.KeyFingerprint {
		return &projection.SchemaMismatchError{
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
		return errors.New("projection memory: context is nil")
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}

func cloneValues(values map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(values))
	for key, value := range values {
		result[key] = append([]byte(nil), value...)
	}
	return result
}

func deltaDigest(batch projection.DeltaBatch) [sha256.Size]byte {
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
