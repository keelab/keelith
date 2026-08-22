package memory

import (
	"context"
	"sort"
	"time"

	"github.com/keelab/keelith/programmable/continuation"
)

var _ continuation.AdminStore = (*Store)(nil)

// ListCalls returns payload-free summaries in stable CallID order.
func (store *Store) ListCalls(
	ctx context.Context,
	request continuation.ListRequest,
) ([]continuation.CallSummary, error) {
	if store == nil ||
		ctx == nil ||
		request.Limit < 1 ||
		request.Limit > 1000 {
		return nil, continuation.ErrInvalidRetention
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureState()
	keys := make([]string, 0, len(store.records))
	for key := range store.records {
		if key > request.After {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > request.Limit {
		keys = keys[:request.Limit]
	}
	result := make([]continuation.CallSummary, 0, len(keys))
	for _, key := range keys {
		summary, err := continuation.NewCallSummary(
			store.records[key],
			store.expires[key],
		)
		if err != nil {
			return nil, err
		}
		result = append(result, summary)
	}
	return result, nil
}

// GetCall returns one payload-free summary.
func (store *Store) GetCall(
	ctx context.Context,
	callID continuation.CallID,
) (continuation.CallSummary, error) {
	if store == nil || ctx == nil || callID.String() == "" {
		return continuation.CallSummary{}, continuation.ErrInvalidRetention
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.CallSummary{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureState()
	snapshot, exists := store.records[callID.String()]
	if !exists {
		return continuation.CallSummary{}, continuation.ErrNotFound
	}
	return continuation.NewCallSummary(
		snapshot,
		store.expires[callID.String()],
	)
}

// PruneFrames physically removes terminal frames below floor.
func (store *Store) PruneFrames(
	ctx context.Context,
	callID continuation.CallID,
	floor uint64,
) (continuation.Snapshot, error) {
	if store == nil || ctx == nil || callID.String() == "" {
		return continuation.Snapshot{}, continuation.ErrInvalidRetention
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Snapshot{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureState()
	current, exists := store.records[callID.String()]
	if !exists {
		return continuation.Snapshot{}, continuation.ErrNotFound
	}
	if !current.Status().Terminal() {
		return continuation.Snapshot{}, continuation.ErrNotReady
	}
	pruned, err := continuation.PruneSnapshot(current, floor)
	if err != nil {
		return continuation.Snapshot{}, err
	}
	store.records[callID.String()] = pruned
	return pruned, nil
}

// Expire atomically terminates one non-terminal call and clears its lease.
func (store *Store) Expire(
	ctx context.Context,
	request continuation.ExpireRequest,
) (continuation.Snapshot, error) {
	if store == nil ||
		ctx == nil ||
		request.CallID.String() == "" ||
		request.ExpectedRevision == 0 ||
		request.ExpiresAt.IsZero() {
		return continuation.Snapshot{}, continuation.ErrInvalidRetention
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Snapshot{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureState()
	key := request.CallID.String()
	current, exists := store.records[key]
	if !exists {
		return continuation.Snapshot{}, continuation.ErrNotFound
	}
	if current.Status() == continuation.StatusExpired {
		return current, nil
	}
	if current.Status().Terminal() {
		return continuation.Snapshot{}, continuation.ErrTerminal
	}
	if current.Revision() != request.ExpectedRevision {
		return continuation.Snapshot{}, continuation.ErrConflict
	}
	frame, err := continuation.NewFrame(
		continuation.FrameExpired,
		nil,
	)
	if err != nil {
		return continuation.Snapshot{}, err
	}
	expired, err := continuation.Apply(
		current,
		continuation.Move(
			continuation.StatusExpired,
			current.Fence(),
			frame,
		),
	)
	if err != nil {
		return continuation.Snapshot{}, err
	}
	store.records[key] = expired
	store.expires[key] = request.ExpiresAt.UTC()
	delete(store.leases, key)
	return expired, nil
}

// DeleteExpired removes a bounded stable batch of elapsed terminal calls.
func (store *Store) DeleteExpired(
	ctx context.Context,
	before time.Time,
	limit int,
) (int, error) {
	if store == nil ||
		ctx == nil ||
		before.IsZero() ||
		limit < 1 ||
		limit > 1000 {
		return 0, continuation.ErrInvalidRetention
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureState()
	keys := make([]string, 0)
	for key, expiresAt := range store.expires {
		snapshot, exists := store.records[key]
		if exists &&
			snapshot.Status().Terminal() &&
			!expiresAt.After(before) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	for _, key := range keys {
		delete(store.records, key)
		delete(store.leases, key)
		delete(store.expires, key)
	}
	return len(keys), nil
}
