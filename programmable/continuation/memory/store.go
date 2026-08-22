// Package memory provides a process-local continuation Store.
package memory

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/keelab/keelith/programmable/continuation"
)

var (
	_ continuation.Store      = (*Store)(nil)
	_ continuation.LeaseStore = (*Store)(nil)
)

type leaseRecord struct {
	owner            string
	deadline         time.Time
	revision         uint64
	fence            uint64
	previousRevision uint64
}

// Store is an atomic in-memory Store for tests and local development.
type Store struct {
	mu      sync.Mutex
	records map[string]continuation.Snapshot
	leases  map[string]leaseRecord
	expires map[string]time.Time
	now     func() time.Time
}

// New constructs an empty Store.
func New() *Store {
	return NewWithClock(func() time.Time { return time.Now().UTC() })
}

// NewWithClock constructs an empty Store with an authoritative lease clock.
func NewWithClock(clock func() time.Time) *Store {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Store{
		records: make(map[string]continuation.Snapshot),
		leases:  make(map[string]leaseRecord),
		expires: make(map[string]time.Time),
		now:     clock,
	}
}

// Create atomically inserts one initial accepted Snapshot.
func (store *Store) Create(
	ctx context.Context,
	snapshot continuation.Snapshot,
) (continuation.Snapshot, error) {
	if store == nil || ctx == nil || !initialSnapshot(snapshot) {
		return continuation.Snapshot{}, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Snapshot{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := snapshot.CallID().String()
	if _, exists := store.records[key]; exists {
		return continuation.Snapshot{}, continuation.ErrAlreadyExists
	}
	store.ensureState()
	store.records[key] = snapshot
	return snapshot, nil
}

// Load returns one immutable Snapshot.
func (store *Store) Load(
	ctx context.Context,
	callID continuation.CallID,
) (continuation.Snapshot, error) {
	if store == nil || ctx == nil || callID.String() == "" {
		return continuation.Snapshot{}, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Snapshot{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	snapshot, exists := store.records[callID.String()]
	if !exists {
		return continuation.Snapshot{}, continuation.ErrNotFound
	}
	return snapshot, nil
}

// Acquire atomically moves a ready call under a strictly newer fence.
func (store *Store) Acquire(
	ctx context.Context,
	callID continuation.CallID,
	expectedRevision uint64,
) (continuation.Snapshot, error) {
	if store == nil ||
		ctx == nil ||
		callID.String() == "" ||
		expectedRevision == 0 {
		return continuation.Snapshot{}, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Snapshot{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.records[callID.String()]
	if !exists {
		return continuation.Snapshot{}, continuation.ErrNotFound
	}
	if current.Revision() != expectedRevision {
		return continuation.Snapshot{}, continuation.ErrConflict
	}
	now := store.now().UTC()
	if lease, claimed := store.leases[callID.String()]; claimed {
		if lease.deadline.After(now) {
			return continuation.Snapshot{}, continuation.ErrLeaseHeld
		}
		delete(store.leases, callID.String())
	}
	if !ready(current.Status()) {
		return continuation.Snapshot{}, continuation.ErrNotReady
	}
	if err := continuation.TimerNotReady(current, now); err != nil {
		return continuation.Snapshot{}, err
	}
	if current.Fence() == math.MaxUint64 {
		return continuation.Snapshot{}, continuation.ErrStaleFence
	}
	target := continuation.StatusRunning
	if current.Status() == continuation.StatusCancelRequested {
		target = continuation.StatusCancelRequested
	}
	next, err := continuation.Apply(
		current,
		continuation.Move(target, current.Fence()+1),
	)
	if err != nil {
		return continuation.Snapshot{}, err
	}
	store.records[callID.String()] = next
	return next, nil
}

// Transition atomically commits one direct Apply result.
func (store *Store) Transition(
	ctx context.Context,
	request continuation.CommitRequest,
) (continuation.Snapshot, error) {
	if store == nil ||
		ctx == nil ||
		request.ExpectedRevision == 0 ||
		request.Fence == 0 ||
		request.Snapshot.CallID().String() == "" ||
		!request.ExpiresAt.IsZero() &&
			!request.Snapshot.Status().Terminal() {
		return continuation.Snapshot{}, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Snapshot{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := request.Snapshot.CallID().String()
	current, exists := store.records[key]
	if !exists {
		return continuation.Snapshot{}, continuation.ErrNotFound
	}
	lease, claimed := store.leases[key]
	if request.LeaseOwner != "" {
		if !claimed ||
			lease.owner != request.LeaseOwner ||
			lease.revision != request.ExpectedRevision ||
			lease.fence != request.Fence ||
			!lease.deadline.After(store.now().UTC()) {
			return continuation.Snapshot{}, continuation.ErrLeaseLost
		}
	} else if claimed && lease.deadline.After(store.now().UTC()) {
		return continuation.Snapshot{}, continuation.ErrLeaseLost
	}
	if request.Fence != current.Fence() {
		return continuation.Snapshot{}, continuation.ErrStaleFence
	}
	if request.ExpectedRevision != current.Revision() {
		return continuation.Snapshot{}, continuation.ErrConflict
	}
	if !validSuccessor(current, request.Snapshot, request.Fence) {
		return continuation.Snapshot{}, continuation.ErrInvalidStore
	}
	store.records[key] = request.Snapshot
	if request.Snapshot.Status().Terminal() &&
		!request.ExpiresAt.IsZero() {
		store.expires[key] = request.ExpiresAt.UTC()
	} else {
		delete(store.expires, key)
	}
	if request.Snapshot.Status() == continuation.StatusRunning {
		lease.revision = request.Snapshot.Revision()
		lease.fence = request.Snapshot.Fence()
		store.leases[key] = lease
	} else {
		delete(store.leases, key)
	}
	return request.Snapshot, nil
}

// ListReady returns a stable bounded snapshot of executable calls.
func (store *Store) ListReady(
	ctx context.Context,
	limit int,
) ([]continuation.Snapshot, error) {
	if store == nil || ctx == nil || limit <= 0 || limit > 10_000 {
		return nil, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0, len(store.records))
	now := store.now().UTC()
	for key, snapshot := range store.records {
		if ready(snapshot.Status()) &&
			snapshot.TimerDue(now) &&
			!store.leaseActive(key, now) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	result := make([]continuation.Snapshot, len(keys))
	for index, key := range keys {
		result[index] = store.records[key]
	}
	return result, nil
}

// SubmitSignal atomically accepts one idempotent Signal command.
func (store *Store) SubmitSignal(
	ctx context.Context,
	request continuation.CommandRequest,
) (continuation.Snapshot, error) {
	frame, err := continuation.NewFrame(
		continuation.FrameSignal,
		request.Payload,
	)
	if err != nil {
		return continuation.Snapshot{}, err
	}
	return store.command(
		ctx,
		request,
		continuation.Signal(request.CommandID, frame),
	)
}

// RequestCancel atomically records one cooperative cancellation request.
func (store *Store) RequestCancel(
	ctx context.Context,
	request continuation.CommandRequest,
) (continuation.Snapshot, error) {
	frame, err := continuation.NewFrame(
		continuation.FrameCancelRequested,
		request.Payload,
	)
	if err != nil {
		return continuation.Snapshot{}, err
	}
	return store.command(
		ctx,
		request,
		continuation.Cancel(request.CommandID, frame),
	)
}

func (store *Store) command(
	ctx context.Context,
	request continuation.CommandRequest,
	transition continuation.Transition,
) (continuation.Snapshot, error) {
	if store == nil ||
		ctx == nil ||
		request.CallID.String() == "" ||
		request.ExpectedRevision == 0 {
		return continuation.Snapshot{}, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Snapshot{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.records[request.CallID.String()]
	if !exists {
		return continuation.Snapshot{}, continuation.ErrNotFound
	}
	next, err := continuation.Apply(current, transition)
	if err != nil {
		return continuation.Snapshot{}, err
	}
	if next.Revision() == current.Revision() {
		return current, nil
	}
	if request.ExpectedRevision != current.Revision() {
		return continuation.Snapshot{}, continuation.ErrConflict
	}
	store.records[request.CallID.String()] = next
	delete(store.leases, request.CallID.String())
	return next, nil
}

// Claim atomically owns one ready revision until its bounded deadline.
func (store *Store) Claim(
	ctx context.Context,
	request continuation.ClaimRequest,
) (continuation.Lease, error) {
	if store == nil || ctx == nil || request.Validate() != nil {
		return continuation.Lease{}, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Lease{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureState()
	key := request.CallID.String()
	current, exists := store.records[key]
	if !exists {
		return continuation.Lease{}, continuation.ErrNotFound
	}
	now := store.now().UTC()
	if lease, claimed := store.leases[key]; claimed &&
		lease.deadline.After(now) {
		if lease.owner == request.OwnerID &&
			lease.previousRevision == request.ExpectedRevision {
			return continuation.Lease{
				Snapshot: current,
				OwnerID:  lease.owner,
				Deadline: lease.deadline,
			}, nil
		}
		return continuation.Lease{}, continuation.ErrLeaseHeld
	}
	if current.Revision() != request.ExpectedRevision {
		return continuation.Lease{}, continuation.ErrConflict
	}
	if !ready(current.Status()) {
		return continuation.Lease{}, continuation.ErrNotReady
	}
	if err := continuation.TimerNotReady(current, now); err != nil {
		return continuation.Lease{}, err
	}
	if current.Fence() == math.MaxUint64 {
		return continuation.Lease{}, continuation.ErrStaleFence
	}
	target := continuation.StatusRunning
	if current.Status() == continuation.StatusCancelRequested {
		target = continuation.StatusCancelRequested
	}
	next, err := continuation.Apply(
		current,
		continuation.Move(target, current.Fence()+1),
	)
	if err != nil {
		return continuation.Lease{}, err
	}
	deadline := now.Add(request.LeaseDuration)
	store.records[key] = next
	store.leases[key] = leaseRecord{
		owner:            request.OwnerID,
		deadline:         deadline,
		revision:         next.Revision(),
		fence:            next.Fence(),
		previousRevision: current.Revision(),
	}
	return continuation.Lease{
		Snapshot: next,
		OwnerID:  request.OwnerID,
		Deadline: deadline,
	}, nil
}

// Renew extends a current, non-expired claim without changing its revision.
func (store *Store) Renew(
	ctx context.Context,
	request continuation.LeaseRequest,
) (continuation.Lease, error) {
	if store == nil || ctx == nil || request.Validate(true) != nil {
		return continuation.Lease{}, continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return continuation.Lease{}, cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := request.CallID.String()
	current, exists := store.records[key]
	if !exists {
		return continuation.Lease{}, continuation.ErrNotFound
	}
	lease, claimed := store.leases[key]
	now := store.now().UTC()
	if !claimed ||
		lease.owner != request.OwnerID ||
		lease.revision != request.Revision ||
		lease.fence != request.Fence ||
		!lease.deadline.After(now) {
		return continuation.Lease{}, continuation.ErrLeaseLost
	}
	lease.deadline = now.Add(request.LeaseDuration)
	store.leases[key] = lease
	return continuation.Lease{
		Snapshot: current,
		OwnerID:  lease.owner,
		Deadline: lease.deadline,
	}, nil
}

// Release makes one uncommitted revision immediately reclaimable.
func (store *Store) Release(
	ctx context.Context,
	request continuation.LeaseRequest,
) error {
	if store == nil || ctx == nil || request.Validate(false) != nil {
		return continuation.ErrInvalidStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := request.CallID.String()
	if _, exists := store.records[key]; !exists {
		return continuation.ErrNotFound
	}
	lease, claimed := store.leases[key]
	if !claimed ||
		lease.owner != request.OwnerID ||
		lease.revision != request.Revision ||
		lease.fence != request.Fence {
		return continuation.ErrLeaseLost
	}
	delete(store.leases, key)
	return nil
}

func (store *Store) leaseActive(key string, now time.Time) bool {
	lease, exists := store.leases[key]
	return exists && lease.deadline.After(now)
}

func (store *Store) ensureState() {
	if store.records == nil {
		store.records = make(map[string]continuation.Snapshot)
	}
	if store.leases == nil {
		store.leases = make(map[string]leaseRecord)
	}
	if store.expires == nil {
		store.expires = make(map[string]time.Time)
	}
	if store.now == nil {
		store.now = func() time.Time { return time.Now().UTC() }
	}
}

func initialSnapshot(snapshot continuation.Snapshot) bool {
	if snapshot.CallID().String() == "" ||
		snapshot.Operation().String() == "" ||
		snapshot.Status() != continuation.StatusAccepted ||
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
			frames[0].Kind() == continuation.FrameAccepted
	default:
		return false
	}
}

func ready(status continuation.Status) bool {
	switch status {
	case continuation.StatusAccepted,
		continuation.StatusRunning,
		continuation.StatusSuspended,
		continuation.StatusCancelRequested:
		return true
	default:
		return false
	}
}

func validSuccessor(
	current continuation.Snapshot,
	next continuation.Snapshot,
	fence uint64,
) bool {
	if next.CallID() != current.CallID() ||
		next.Operation() != current.Operation() ||
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

func equalFrame(first, second continuation.Frame) bool {
	if first.Sequence() != second.Sequence() ||
		first.Kind() != second.Kind() {
		return false
	}
	left := first.Payload()
	right := second.Payload()
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
