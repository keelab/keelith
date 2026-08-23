// Package memory provides a process-local Saga Repository.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/keelab/keelith/saga"
)

// Repository is a revisioned, fence-aware in-memory store.
type Repository struct {
	mu      sync.Mutex
	records map[string]saga.Record
	fences  map[string]uint64
}

// New constructs an empty Repository.
func New() *Repository {
	return &Repository{
		records: make(map[string]saga.Record),
		fences:  make(map[string]uint64),
	}
}

// Load returns one immutable record snapshot.
func (repository *Repository) Load(
	ctx context.Context,
	id string,
) (saga.Record, error) {
	if repository == nil || ctx == nil {
		return saga.Record{}, fmt.Errorf(
			"%w: repository or context",
			saga.ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return saga.Record{}, cause
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	record, exists := repository.records[id]
	if !exists {
		return saga.Record{}, saga.ErrNotFound
	}
	return record, nil
}

// Create inserts one instance under its first ownership fence.
func (repository *Repository) Create(
	ctx context.Context,
	record saga.Record,
	fence uint64,
) (saga.Record, error) {
	if repository == nil || ctx == nil || fence == 0 {
		return saga.Record{}, fmt.Errorf(
			"%w: repository, context, or fence",
			saga.ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return saga.Record{}, cause
	}
	record.Revision = 1
	if err := record.Validate(); err != nil {
		return saga.Record{}, err
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if _, exists := repository.records[record.ID]; exists {
		return saga.Record{}, saga.ErrAlreadyExists
	}
	repository.records[record.ID] = record
	repository.fences[record.ID] = fence
	return record, nil
}

// Save atomically checks revision and ownership generation.
func (repository *Repository) Save(
	ctx context.Context,
	record saga.Record,
	expectedRevision uint64,
	fence uint64,
) (saga.Record, error) {
	if repository == nil ||
		ctx == nil ||
		expectedRevision == 0 ||
		fence == 0 {
		return saga.Record{}, fmt.Errorf(
			"%w: repository, context, revision, or fence",
			saga.ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return saga.Record{}, cause
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	current, exists := repository.records[record.ID]
	if !exists {
		return saga.Record{}, saga.ErrNotFound
	}
	if fence < repository.fences[record.ID] {
		return saga.Record{}, saga.ErrStaleFence
	}
	if current.Revision != expectedRevision {
		return saga.Record{}, saga.ErrConflict
	}
	if current.Definition != record.Definition ||
		current.Version != record.Version {
		return saga.Record{}, saga.ErrDefinitionMismatch
	}
	record.Revision = expectedRevision + 1
	if err := record.Validate(); err != nil {
		return saga.Record{}, err
	}
	repository.records[record.ID] = record
	repository.fences[record.ID] = fence
	return record, nil
}
