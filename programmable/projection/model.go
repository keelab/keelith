// Package projection defines transport- and storage-neutral typed projection
// synchronization models.
package projection

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxProjectionIDBytes = 256
	maxCursorBytes       = 512
	maxKeyBytes          = 4 * 1024
	maxValueBytes        = 16 * 1024 * 1024
	maxDeltaMutations    = 10_000
	maxDeltaBytes        = 32 * 1024 * 1024
	fingerprintBytes     = 64
	maxCompatibleSchemas = 16
)

var (
	// ErrInvalidProjectionID reports an empty, unsafe, or unbounded identity.
	ErrInvalidProjectionID = errors.New("projection: invalid projection ID")
	// ErrInvalidSchema reports an incomplete or malformed projection schema.
	ErrInvalidSchema = errors.New("projection: invalid schema")
	// ErrInvalidCursor reports an empty, unsafe, or unbounded checkpoint cursor.
	ErrInvalidCursor = errors.New("projection: invalid cursor")
	// ErrInvalidMutation reports a malformed key, value, or mutation kind.
	ErrInvalidMutation = errors.New("projection: invalid mutation")
	// ErrInvalidDelta reports an incomplete or unbounded delta batch.
	ErrInvalidDelta = errors.New("projection: invalid delta")
	// ErrCursorMismatch reports that a delta does not continue the checkpoint.
	ErrCursorMismatch = errors.New("projection: cursor mismatch")
	// ErrSchemaMismatch reports incompatible schema or key fingerprints.
	ErrSchemaMismatch = errors.New("projection: schema mismatch")
	// ErrProjectionNotFound reports a delta for an uninitialized projection.
	ErrProjectionNotFound = errors.New("projection: projection not found")
	// ErrSnapshotClosed reports use of a committed or aborted snapshot.
	ErrSnapshotClosed = errors.New("projection: snapshot transaction closed")
	// ErrSnapshotConflict reports a generation change during snapshot staging.
	ErrSnapshotConflict = errors.New("projection: snapshot generation conflict")
	// ErrReplayConflict reports reuse of a cursor with different delta content.
	ErrReplayConflict = errors.New("projection: delta replay conflict")
)

// ProjectionID is the stable owner-qualified identity of one projection.
//
//nolint:revive // ProjectionID keeps identity explicit beside ComponentID and CallID.
type ProjectionID string

// Validate rejects ambiguous or unbounded projection identities.
func (id ProjectionID) Validate() error {
	if !validIdentity(string(id), maxProjectionIDBytes) {
		return fmt.Errorf("%w: %q", ErrInvalidProjectionID, id)
	}
	return nil
}

// Cursor is an opaque durable source checkpoint.
type Cursor string

// Validate rejects ambiguous or unbounded cursors.
func (cursor Cursor) Validate() error {
	if !validIdentity(string(cursor), maxCursorBytes) {
		return fmt.Errorf("%w: %q", ErrInvalidCursor, cursor)
	}
	return nil
}

// FingerprintSet is an immutable, canonical set of compatible value schemas.
//
// The zero value is an empty set.
type FingerprintSet struct {
	values [maxCompatibleSchemas]string
	count  uint8
}

// NewFingerprintSet validates, sorts, and snapshots compatible fingerprints.
func NewFingerprintSet(values ...string) (FingerprintSet, error) {
	if len(values) > maxCompatibleSchemas {
		return FingerprintSet{}, fmt.Errorf(
			"%w: too many compatible fingerprints",
			ErrInvalidSchema,
		)
	}
	canonical := append([]string(nil), values...)
	sort.Strings(canonical)
	for index, value := range canonical {
		if !validFingerprint(value) {
			return FingerprintSet{}, fmt.Errorf(
				"%w: compatible fingerprint",
				ErrInvalidSchema,
			)
		}
		if index > 0 && canonical[index-1] == value {
			return FingerprintSet{}, fmt.Errorf(
				"%w: duplicate compatible fingerprint",
				ErrInvalidSchema,
			)
		}
	}
	var result FingerprintSet
	copy(result.values[:], canonical)
	result.count = uint8(len(canonical))
	return result, nil
}

// Values returns an independent canonical fingerprint slice.
func (set FingerprintSet) Values() []string {
	return append([]string(nil), set.values[:set.count]...)
}

// Len returns the number of compatible fingerprints.
func (set FingerprintSet) Len() int {
	return int(set.count)
}

// Contains reports whether fingerprint belongs to the set.
func (set FingerprintSet) Contains(fingerprint string) bool {
	values := set.values[:set.count]
	index := sort.SearchStrings(values, fingerprint)
	return index < len(values) && values[index] == fingerprint
}

// Schema identifies the current value schema, explicitly accepted historical
// value schemas, and immutable key encoding.
type Schema struct {
	ID                     ProjectionID
	Fingerprint            string
	KeyFingerprint         string
	CompatibleFingerprints FingerprintSet
}

// Validate checks the projection identity and SHA-256 fingerprints.
func (schema Schema) Validate() error {
	if err := schema.ID.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}
	if !validFingerprint(schema.Fingerprint) {
		return fmt.Errorf("%w: fingerprint", ErrInvalidSchema)
	}
	if !validFingerprint(schema.KeyFingerprint) {
		return fmt.Errorf("%w: key fingerprint", ErrInvalidSchema)
	}
	if schema.CompatibleFingerprints.Len() > maxCompatibleSchemas {
		return fmt.Errorf(
			"%w: too many compatible fingerprints",
			ErrInvalidSchema,
		)
	}
	for index, fingerprint := range schema.CompatibleFingerprints.values[:schema.CompatibleFingerprints.count] {
		if !validFingerprint(fingerprint) ||
			fingerprint == schema.Fingerprint ||
			index > 0 &&
				schema.CompatibleFingerprints.values[index-1] >= fingerprint {
			return fmt.Errorf(
				"%w: compatible fingerprints",
				ErrInvalidSchema,
			)
		}
	}
	return nil
}

// WithCompatibleFingerprints returns a schema that explicitly accepts the
// supplied historical value schemas.
func (schema Schema) WithCompatibleFingerprints(
	fingerprints ...string,
) (Schema, error) {
	compatible, err := NewFingerprintSet(fingerprints...)
	if err != nil {
		return Schema{}, err
	}
	schema.CompatibleFingerprints = compatible
	if err := schema.Validate(); err != nil {
		return Schema{}, err
	}
	return schema, nil
}

// Accepts verifies the directional consumer-to-owner schema contract.
//
// Value fingerprints may use the consumer's explicit compatibility window.
// Projection identity and key encoding always require an exact match.
func (schema Schema) Accepts(owner Schema) error {
	if schema.ID != owner.ID {
		return &SchemaMismatchError{
			Projection: schema.ID,
			Field:      "projection_id",
			Expected:   string(schema.ID),
			Actual:     string(owner.ID),
		}
	}
	if schema.Fingerprint != owner.Fingerprint &&
		!schema.CompatibleFingerprints.Contains(owner.Fingerprint) {
		return &SchemaMismatchError{
			Projection: schema.ID,
			Field:      "fingerprint",
			Expected:   schema.Fingerprint,
			Actual:     owner.Fingerprint,
		}
	}
	if schema.KeyFingerprint != owner.KeyFingerprint {
		return &SchemaMismatchError{
			Projection: schema.ID,
			Field:      "key_fingerprint",
			Expected:   schema.KeyFingerprint,
			Actual:     owner.KeyFingerprint,
		}
	}
	return nil
}

// SchemaMismatchError identifies the incompatible schema dimension.
type SchemaMismatchError struct {
	Projection ProjectionID
	Field      string
	Expected   string
	Actual     string
}

// Error implements error without exposing projection payloads or keys.
func (mismatch *SchemaMismatchError) Error() string {
	if mismatch == nil {
		return ErrSchemaMismatch.Error()
	}
	return fmt.Sprintf(
		"%s: projection %q field %s",
		ErrSchemaMismatch,
		mismatch.Projection,
		mismatch.Field,
	)
}

// Unwrap supports errors.Is with ErrSchemaMismatch.
func (*SchemaMismatchError) Unwrap() error {
	return ErrSchemaMismatch
}

// CursorMismatchError reports the durable cursor the Store expected.
type CursorMismatchError struct {
	Projection ProjectionID
	Expected   Cursor
	Actual     Cursor
}

// Error implements error without exposing projection values or keys.
func (mismatch *CursorMismatchError) Error() string {
	if mismatch == nil {
		return ErrCursorMismatch.Error()
	}
	return fmt.Sprintf(
		"%s: projection %q expected %q, got %q",
		ErrCursorMismatch,
		mismatch.Projection,
		mismatch.Expected,
		mismatch.Actual,
	)
}

// Unwrap supports errors.Is with ErrCursorMismatch.
func (*CursorMismatchError) Unwrap() error {
	return ErrCursorMismatch
}

// MutationKind distinguishes replacement values from tombstones.
type MutationKind uint8

const (
	// MutationUpsert inserts or replaces one key.
	MutationUpsert MutationKind = iota + 1
	// MutationDelete removes one key.
	MutationDelete
)

// Mutation is one immutable projection row change.
type Mutation struct {
	kind  MutationKind
	key   []byte
	value []byte
}

// Upsert creates an immutable insert-or-replace mutation.
func Upsert(key, value []byte) Mutation {
	return Mutation{
		kind:  MutationUpsert,
		key:   cloneBytes(key),
		value: cloneBytes(value),
	}
}

// Delete creates an immutable tombstone mutation.
func Delete(key []byte) Mutation {
	return Mutation{
		kind: MutationDelete,
		key:  cloneBytes(key),
	}
}

// Kind returns the mutation operation.
func (mutation Mutation) Kind() MutationKind {
	return mutation.kind
}

// Key returns an independent key copy.
func (mutation Mutation) Key() []byte {
	return cloneBytes(mutation.key)
}

// Value returns an independent payload copy.
func (mutation Mutation) Value() []byte {
	return cloneBytes(mutation.value)
}

// Clone returns a deep immutable copy.
func (mutation Mutation) Clone() Mutation {
	return Mutation{
		kind:  mutation.kind,
		key:   mutation.Key(),
		value: mutation.Value(),
	}
}

// Validate checks mutation kind and byte budgets.
func (mutation Mutation) Validate() error {
	if len(mutation.key) == 0 || len(mutation.key) > maxKeyBytes {
		return fmt.Errorf("%w: key", ErrInvalidMutation)
	}
	switch mutation.kind {
	case MutationUpsert:
		if len(mutation.value) > maxValueBytes {
			return fmt.Errorf("%w: value", ErrInvalidMutation)
		}
	case MutationDelete:
		if len(mutation.value) != 0 {
			return fmt.Errorf("%w: delete value", ErrInvalidMutation)
		}
	default:
		return fmt.Errorf("%w: kind", ErrInvalidMutation)
	}
	return nil
}

// DeltaBatch advances one projection from Previous to Cursor atomically.
type DeltaBatch struct {
	Schema     Schema
	Previous   Cursor
	Cursor     Cursor
	SourceTime time.Time
	Mutations  []Mutation
}

// Validate checks continuity metadata and bounded mutations.
func (batch DeltaBatch) Validate() error {
	if err := batch.Schema.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDelta, err)
	}
	if err := batch.Previous.Validate(); err != nil {
		return fmt.Errorf("%w: previous cursor: %w", ErrInvalidDelta, err)
	}
	if err := batch.Cursor.Validate(); err != nil {
		return fmt.Errorf("%w: cursor: %w", ErrInvalidDelta, err)
	}
	if batch.Cursor == batch.Previous {
		return fmt.Errorf("%w: cursor did not advance", ErrInvalidDelta)
	}
	if batch.SourceTime.IsZero() {
		return fmt.Errorf("%w: source time", ErrInvalidDelta)
	}
	if len(batch.Mutations) == 0 ||
		len(batch.Mutations) > maxDeltaMutations {
		return fmt.Errorf("%w: mutation count", ErrInvalidDelta)
	}
	totalBytes := 0
	for index, mutation := range batch.Mutations {
		if err := mutation.Validate(); err != nil {
			return fmt.Errorf(
				"%w: mutation %d: %w",
				ErrInvalidDelta,
				index,
				err,
			)
		}
		totalBytes += len(mutation.key) + len(mutation.value)
		if totalBytes > maxDeltaBytes {
			return fmt.Errorf("%w: byte budget", ErrInvalidDelta)
		}
	}
	return nil
}

// Clone returns a deep immutable copy.
func (batch DeltaBatch) Clone() DeltaBatch {
	result := batch
	result.Mutations = make([]Mutation, len(batch.Mutations))
	for index, mutation := range batch.Mutations {
		result.Mutations[index] = mutation.Clone()
	}
	return result
}

// Checkpoint is the atomic visible generation and freshness watermark.
type Checkpoint struct {
	Schema     Schema
	Cursor     Cursor
	Generation uint64
	SourceTime time.Time
	AppliedAt  time.Time
}

// Freshness returns the conservative age of the source watermark.
func (checkpoint Checkpoint) Freshness(now time.Time) time.Duration {
	if checkpoint.SourceTime.IsZero() ||
		now.IsZero() ||
		now.Before(checkpoint.SourceTime) {
		return 0
	}
	return now.Sub(checkpoint.SourceTime)
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func validFingerprint(value string) bool {
	if len(value) != fingerprintBytes {
		return false
	}
	for _, r := range value {
		if r >= '0' && r <= '9' ||
			r >= 'a' && r <= 'f' {
			continue
		}
		return false
	}
	return true
}

func validIdentity(value string, maxBytes int) bool {
	if value == "" ||
		len(value) > maxBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
