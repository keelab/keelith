// Package ownermemory provides a bounded single-owner projection Source.
package ownermemory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/programmable/projection"
)

const (
	cursorPrefix       = "memory/"
	cursorDigits       = 20
	maxChunkBudget     = 32 * 1024 * 1024
	maxLiveQueueBudget = 64
)

var (
	// ErrInvalidOption reports an invalid schema, retention, or byte budget.
	ErrInvalidOption = errors.New("projection owner memory: invalid option")
	// ErrNotSeeded reports an Open before the owner has established state.
	ErrNotSeeded = errors.New("projection owner memory: source is not seeded")
	// ErrAlreadySeeded reports a second Seed attempt.
	ErrAlreadySeeded = errors.New("projection owner memory: source already seeded")
	// ErrSourceClosed reports use after Source.Close.
	ErrSourceClosed = errors.New("projection owner memory: source closed")
	// ErrSessionClosed reports Next after an explicit Session.Close.
	ErrSessionClosed = errors.New("projection owner memory: session closed")
	// ErrSlowSubscriber reports a live subscriber exceeding its queue budget.
	ErrSlowSubscriber = errors.New("projection owner memory: slow subscriber")
	// ErrInvalidOwnerCursor reports a malformed or future owner cursor.
	ErrInvalidOwnerCursor = errors.New("projection owner memory: invalid cursor")
)

var _ projection.Source = (*Source)(nil)

// Source owns current rows, a retained delta log, and live subscribers.
type Source struct {
	mu sync.Mutex

	schema      projection.Schema
	retention   int
	chunkBudget int
	queueBudget int

	rows        map[string][]byte
	offset      uint64
	sourceTime  time.Time
	log         []logEntry
	subscribers map[*session]struct{}
	closed      bool
}

type logEntry struct {
	offset uint64
	batch  projection.DeltaBatch
}

// New constructs an empty bounded owner Source.
func New(
	schema projection.Schema,
	retention int,
	chunkBudget int,
) (*Source, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if retention <= 0 ||
		retention > 100_000 ||
		chunkBudget <= 0 ||
		chunkBudget > maxChunkBudget {
		return nil, fmt.Errorf(
			"%w: retention or chunk budget",
			ErrInvalidOption,
		)
	}
	queueBudget := retention
	if queueBudget > maxLiveQueueBudget {
		queueBudget = maxLiveQueueBudget
	}
	return &Source{
		schema:      schema,
		retention:   retention,
		chunkBudget: chunkBudget,
		queueBudget: queueBudget,
		rows:        make(map[string][]byte),
		subscribers: make(map[*session]struct{}),
	}, nil
}

// Seed establishes the initial owner rows and first monotonic cursor.
func (source *Source) Seed(
	mutations []projection.Mutation,
	sourceTime time.Time,
) (projection.Cursor, error) {
	if source == nil {
		return "", fmt.Errorf("%w: source is nil", ErrInvalidOption)
	}
	cloned, err := source.validateMutations(mutations)
	if err != nil {
		return "", err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return "", ErrSourceClosed
	}
	if source.offset != 0 {
		return "", ErrAlreadySeeded
	}
	return source.commitLocked(cloned, sourceTime)
}

// Replace atomically changes the complete owner row set and emits its diff.
func (source *Source) Replace(
	rows []projection.Mutation,
	sourceTime time.Time,
) (projection.Cursor, error) {
	if source == nil {
		return "", fmt.Errorf("%w: source is nil", ErrInvalidOption)
	}
	cloned, err := source.validateMutations(rows)
	if err != nil {
		return "", err
	}
	desired := make(map[string][]byte)
	for _, mutation := range cloned {
		key := string(mutation.Key())
		switch mutation.Kind() {
		case projection.MutationUpsert:
			desired[key] = mutation.Value()
		case projection.MutationDelete:
			delete(desired, key)
		}
	}

	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return "", ErrSourceClosed
	}
	if source.offset == 0 {
		return source.commitLocked(cloned, sourceTime)
	}
	changes := diffRows(source.rows, desired)
	if len(changes) == 0 {
		return encodeCursor(source.offset), nil
	}
	return source.commitLocked(changes, sourceTime)
}

// Commit atomically applies mutations, advances the cursor, retains the delta,
// and publishes it to every live subscriber.
func (source *Source) Commit(
	mutations []projection.Mutation,
	sourceTime time.Time,
) (projection.Cursor, error) {
	if source == nil {
		return "", fmt.Errorf("%w: source is nil", ErrInvalidOption)
	}
	cloned, err := source.validateMutations(mutations)
	if err != nil {
		return "", err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return "", ErrSourceClosed
	}
	if source.offset == 0 {
		return "", ErrNotSeeded
	}
	return source.commitLocked(cloned, sourceTime)
}

// Open returns a consistent snapshot session or a retained resume session.
func (source *Source) Open(
	ctx context.Context,
	request projection.SubscribeRequest,
) (projection.Session, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: source is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := request.Schema.Accepts(source.schema); err != nil {
		return nil, err
	}

	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return nil, ErrSourceClosed
	}
	if source.offset == 0 {
		return nil, ErrNotSeeded
	}
	if request.ForceSnapshot {
		created := newSession(
			source,
			source.snapshotFramesLocked(),
			source.queueBudget,
			source.offset,
		)
		source.subscribers[created] = struct{}{}
		return created, nil
	}

	after, err := decodeCursor(request.After)
	if err != nil || after > source.offset {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrInvalidOwnerCursor,
			request.After,
		)
	}
	floor := source.retentionFloorLocked()
	if after < floor {
		return newSession(source, []projection.Frame{
			projection.GapFrame{
				Requested: request.After,
				Floor:     encodeCursor(floor),
			},
		}, source.queueBudget, after), nil
	}
	replay := make([]projection.Frame, 0, len(source.log))
	for _, entry := range source.log {
		if entry.offset > after {
			replay = append(replay, projection.DeltaFrame{
				Batch: entry.batch.Clone(),
			})
		}
	}
	created := newSession(source, replay, source.queueBudget, after)
	source.subscribers[created] = struct{}{}
	return created, nil
}

// Cursor returns the current opaque owner checkpoint.
func (source *Source) Cursor() projection.Cursor {
	if source == nil {
		return ""
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return encodeCursor(source.offset)
}

// SubscriberCount returns the current bounded live subscription count.
func (source *Source) SubscriberCount() int {
	if source == nil {
		return 0
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return len(source.subscribers)
}

// Close rejects new work and unblocks every live Session.
func (source *Source) Close() error {
	if source == nil {
		return nil
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		return nil
	}
	source.closed = true
	sessions := make([]*session, 0, len(source.subscribers))
	for subscriber := range source.subscribers {
		sessions = append(sessions, subscriber)
		delete(source.subscribers, subscriber)
	}
	source.mu.Unlock()
	for _, subscriber := range sessions {
		subscriber.terminate(ErrSourceClosed)
	}
	return nil
}

func (source *Source) commitLocked(
	mutations []projection.Mutation,
	sourceTime time.Time,
) (projection.Cursor, error) {
	if sourceTime.IsZero() ||
		!source.sourceTime.IsZero() && sourceTime.Before(source.sourceTime) {
		return "", fmt.Errorf("%w: source time", ErrInvalidOption)
	}
	if source.offset == math.MaxUint64 {
		return "", fmt.Errorf("%w: cursor exhausted", ErrInvalidOwnerCursor)
	}
	nextOffset := source.offset + 1
	batch := projection.DeltaBatch{
		Schema:     source.schema,
		Previous:   encodeCursor(source.offset),
		Cursor:     encodeCursor(nextOffset),
		SourceTime: sourceTime,
		Mutations:  cloneMutations(mutations),
	}
	if err := batch.Validate(); err != nil {
		return "", err
	}
	nextRows := cloneRows(source.rows)
	for _, mutation := range mutations {
		key := string(mutation.Key())
		switch mutation.Kind() {
		case projection.MutationUpsert:
			nextRows[key] = mutation.Value()
		case projection.MutationDelete:
			delete(nextRows, key)
		}
	}

	source.rows = nextRows
	source.offset = nextOffset
	source.sourceTime = sourceTime
	source.log = append(source.log, logEntry{
		offset: nextOffset,
		batch:  batch.Clone(),
	})
	if len(source.log) > source.retention {
		source.log = append(
			[]logEntry(nil),
			source.log[len(source.log)-source.retention:]...,
		)
	}
	for subscriber := range source.subscribers {
		frame := projection.DeltaFrame{Batch: batch.Clone()}
		select {
		case subscriber.live <- frame:
		default:
			delete(source.subscribers, subscriber)
			subscriber.terminate(ErrSlowSubscriber)
		}
	}
	return batch.Cursor, nil
}

func (source *Source) snapshotFramesLocked() []projection.Frame {
	keys := make([]string, 0, len(source.rows))
	for key := range source.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	frames := []projection.Frame{
		projection.SnapshotBeginFrame{Schema: source.schema},
	}
	chunk := make([]projection.Mutation, 0)
	chunkBytes := 0
	flush := func() {
		if len(chunk) == 0 {
			return
		}
		frames = append(frames, projection.SnapshotChunkFrame{
			Mutations: cloneMutations(chunk),
		})
		chunk = chunk[:0]
		chunkBytes = 0
	}
	for _, key := range keys {
		value := source.rows[key]
		size := len(key) + len(value)
		if len(chunk) > 0 && chunkBytes+size > source.chunkBudget {
			flush()
		}
		chunk = append(chunk, projection.Upsert([]byte(key), value))
		chunkBytes += size
	}
	flush()
	frames = append(frames, projection.SnapshotEndFrame{
		Cursor:     encodeCursor(source.offset),
		SourceTime: source.sourceTime,
	})
	return frames
}

func (source *Source) retentionFloorLocked() uint64 {
	if len(source.log) == 0 {
		return source.offset
	}
	return source.log[0].offset - 1
}

func (source *Source) validateMutations(
	mutations []projection.Mutation,
) ([]projection.Mutation, error) {
	if len(mutations) == 0 {
		return nil, fmt.Errorf("%w: no mutations", ErrInvalidOption)
	}
	cloned := cloneMutations(mutations)
	for index, mutation := range cloned {
		if err := mutation.Validate(); err != nil {
			return nil, fmt.Errorf(
				"%w: mutation %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
		if mutation.Kind() == projection.MutationUpsert &&
			len(mutation.Key())+len(mutation.Value()) > source.chunkBudget {
			return nil, fmt.Errorf(
				"%w: mutation %d exceeds chunk budget",
				ErrInvalidOption,
				index,
			)
		}
	}
	return cloned, nil
}

type session struct {
	source *Source

	stateMu sync.Mutex
	initial []projection.Frame
	index   int
	runErr  error

	live      chan projection.Frame
	done      chan struct{}
	closeOnce sync.Once
	protected uint64
}

func newSession(
	source *Source,
	initial []projection.Frame,
	queueBudget int,
	protected uint64,
) *session {
	return &session{
		source:    source,
		initial:   append([]projection.Frame(nil), initial...),
		live:      make(chan projection.Frame, queueBudget),
		done:      make(chan struct{}),
		protected: protected,
	}
}

func (session *session) Next(ctx context.Context) (
	projection.Frame,
	error,
) {
	if session == nil {
		return nil, ErrSessionClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	select {
	case <-session.done:
		return nil, session.terminalError()
	default:
	}
	session.stateMu.Lock()
	if session.index < len(session.initial) {
		frame := session.initial[session.index]
		session.index++
		session.stateMu.Unlock()
		return frame, nil
	}
	session.stateMu.Unlock()
	select {
	case <-session.done:
		return nil, session.terminalError()
	case frame := <-session.live:
		return frame, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (session *session) Close() error {
	if session == nil {
		return nil
	}
	if session.source != nil {
		session.source.mu.Lock()
		delete(session.source.subscribers, session)
		session.source.mu.Unlock()
	}
	session.terminate(ErrSessionClosed)
	return nil
}

func (session *session) terminate(err error) {
	session.closeOnce.Do(func() {
		session.stateMu.Lock()
		session.runErr = err
		session.stateMu.Unlock()
		close(session.done)
	})
}

func (session *session) terminalError() error {
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	if session.runErr == nil {
		return ErrSessionClosed
	}
	return session.runErr
}

func diffRows(
	current map[string][]byte,
	desired map[string][]byte,
) []projection.Mutation {
	keys := make(map[string]struct{}, len(current)+len(desired))
	for key := range current {
		keys[key] = struct{}{}
	}
	for key := range desired {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	result := make([]projection.Mutation, 0, len(ordered))
	for _, key := range ordered {
		before, beforeExists := current[key]
		after, afterExists := desired[key]
		switch {
		case beforeExists && !afterExists:
			result = append(result, projection.Delete([]byte(key)))
		case afterExists && (!beforeExists || !bytes.Equal(before, after)):
			result = append(result, projection.Upsert([]byte(key), after))
		}
	}
	return result
}

func cloneRows(rows map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(rows))
	for key, value := range rows {
		result[key] = append([]byte(nil), value...)
	}
	return result
}

func cloneMutations(
	mutations []projection.Mutation,
) []projection.Mutation {
	result := make([]projection.Mutation, len(mutations))
	for index, mutation := range mutations {
		result[index] = mutation.Clone()
	}
	return result
}

func encodeCursor(offset uint64) projection.Cursor {
	return projection.Cursor(fmt.Sprintf(
		"%s%0*d",
		cursorPrefix,
		cursorDigits,
		offset,
	))
}

func decodeCursor(cursor projection.Cursor) (uint64, error) {
	value := string(cursor)
	if len(value) != len(cursorPrefix)+cursorDigits ||
		!strings.HasPrefix(value, cursorPrefix) {
		return 0, ErrInvalidOwnerCursor
	}
	offset, err := strconv.ParseUint(value[len(cursorPrefix):], 10, 64)
	if err != nil || encodeCursor(offset) != cursor {
		return 0, ErrInvalidOwnerCursor
	}
	return offset, nil
}
