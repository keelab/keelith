package continuation

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

const (
	defaultTerminalRetention = 24 * time.Hour
	defaultAdminPageSize     = 100
	maxAdminPageSize         = 1000
	maxTerminalRetention     = 365 * 24 * time.Hour
)

// ListRequest is one bounded, lexicographically paged administration query.
type ListRequest struct {
	After string
	Limit int
}

// ExpireRequest atomically moves one non-terminal call to Expired.
type ExpireRequest struct {
	CallID           CallID
	ExpectedRevision uint64
	ExpiresAt        time.Time
}

// CallSummary is the payload-free administration view of one call.
type CallSummary struct {
	CallID              CallID
	Operation           Operation
	Status              Status
	Revision            uint64
	Fence               uint64
	Sequence            uint64
	FrameFloor          uint64
	ExpiresAt           time.Time
	WorkflowVersion     string
	WorkflowFingerprint string
	WorkflowNodes       int
	WorkflowFailed      int
}

// AdminStore is the optional retention and administration capability
// implemented by first-party continuation stores.
type AdminStore interface {
	Store
	ListCalls(context.Context, ListRequest) ([]CallSummary, error)
	GetCall(context.Context, CallID) (CallSummary, error)
	PruneFrames(context.Context, CallID, uint64) (Snapshot, error)
	Expire(context.Context, ExpireRequest) (Snapshot, error)
	DeleteExpired(context.Context, time.Time, int) (int, error)
}

// AdminConfig configures one payload-safe administration service.
type AdminConfig struct {
	Store             AdminStore
	TerminalRetention time.Duration
	MaxPageSize       int
	Clock             func() time.Time
}

// Admin provides bounded list/get/cancel/expire/prune/GC operations.
type Admin struct {
	store             AdminStore
	terminalRetention time.Duration
	maxPageSize       int
	now               func() time.Time
}

// NewAdmin validates and constructs one administration service.
func NewAdmin(config AdminConfig) (*Admin, error) {
	if isNilAdminStore(config.Store) {
		return nil, fmt.Errorf("%w: admin store is required", ErrInvalidRetention)
	}
	if config.TerminalRetention == 0 {
		config.TerminalRetention = defaultTerminalRetention
	}
	if config.MaxPageSize == 0 {
		config.MaxPageSize = defaultAdminPageSize
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	if config.TerminalRetention <= 0 ||
		config.TerminalRetention > maxTerminalRetention ||
		config.MaxPageSize < 1 ||
		config.MaxPageSize > maxAdminPageSize {
		return nil, ErrInvalidRetention
	}
	return &Admin{
		store:             config.Store,
		terminalRetention: config.TerminalRetention,
		maxPageSize:       config.MaxPageSize,
		now:               config.Clock,
	}, nil
}

// List returns payload-free summaries in stable CallID order.
func (admin *Admin) List(
	ctx context.Context,
	request ListRequest,
) ([]CallSummary, error) {
	if admin == nil || ctx == nil ||
		request.Limit < 1 ||
		request.Limit > admin.maxPageSize {
		return nil, ErrInvalidRetention
	}
	if request.After != "" && !validIdentity(request.After) {
		return nil, ErrInvalidRetention
	}
	return admin.store.ListCalls(ctx, request)
}

// Get returns one payload-free summary.
func (admin *Admin) Get(
	ctx context.Context,
	callID CallID,
) (CallSummary, error) {
	if admin == nil || ctx == nil {
		return CallSummary{}, ErrInvalidRetention
	}
	return admin.store.GetCall(ctx, callID)
}

// Cancel submits one idempotent cooperative cancellation without exposing
// the stored payload.
func (admin *Admin) Cancel(
	ctx context.Context,
	callID CallID,
	commandID string,
) (CallSummary, error) {
	if admin == nil || ctx == nil {
		return CallSummary{}, ErrInvalidRetention
	}
	current, err := admin.store.Load(ctx, callID)
	if err != nil {
		return CallSummary{}, err
	}
	if _, err := admin.store.RequestCancel(ctx, CommandRequest{
		CallID:           callID,
		ExpectedRevision: current.Revision(),
		CommandID:        commandID,
	}); err != nil {
		return CallSummary{}, err
	}
	return admin.store.GetCall(ctx, callID)
}

// Expire atomically terminates one call and schedules bounded GC.
func (admin *Admin) Expire(
	ctx context.Context,
	callID CallID,
) (CallSummary, error) {
	if admin == nil || ctx == nil {
		return CallSummary{}, ErrInvalidRetention
	}
	current, err := admin.store.Load(ctx, callID)
	if err != nil {
		return CallSummary{}, err
	}
	expiresAt := admin.now().UTC().Add(admin.terminalRetention)
	if _, err := admin.store.Expire(ctx, ExpireRequest{
		CallID:           callID,
		ExpectedRevision: current.Revision(),
		ExpiresAt:        expiresAt,
	}); err != nil {
		return CallSummary{}, err
	}
	return admin.store.GetCall(ctx, callID)
}

// Prune physically removes frames below floor.
func (admin *Admin) Prune(
	ctx context.Context,
	callID CallID,
	floor uint64,
) (CallSummary, error) {
	if admin == nil || ctx == nil {
		return CallSummary{}, ErrInvalidRetention
	}
	if _, err := admin.store.PruneFrames(ctx, callID, floor); err != nil {
		return CallSummary{}, err
	}
	return admin.store.GetCall(ctx, callID)
}

// Collect deletes at most limit terminal calls whose expiration has elapsed.
func (admin *Admin) Collect(ctx context.Context, limit int) (int, error) {
	if admin == nil || ctx == nil ||
		limit < 1 ||
		limit > admin.maxPageSize {
		return 0, ErrInvalidRetention
	}
	return admin.store.DeleteExpired(ctx, admin.now().UTC(), limit)
}

func isNilAdminStore(store AdminStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// NewCallSummary constructs a validated payload-free Store result.
func NewCallSummary(
	snapshot Snapshot,
	expiresAt time.Time,
) (CallSummary, error) {
	if err := validateSnapshot(snapshot); err != nil {
		return CallSummary{}, ErrInvalidRetention
	}
	summary := CallSummary{
		CallID:     snapshot.CallID(),
		Operation:  snapshot.Operation(),
		Status:     snapshot.Status(),
		Revision:   snapshot.Revision(),
		Fence:      snapshot.Fence(),
		Sequence:   snapshot.Sequence(),
		FrameFloor: snapshot.FrameFloor(),
		ExpiresAt:  expiresAt.UTC(),
	}
	if workflow, ok := snapshot.Workflow(); ok {
		summary.WorkflowVersion = workflow.Version()
		summary.WorkflowFingerprint = workflow.Fingerprint()
		nodes := workflow.Nodes()
		summary.WorkflowNodes = len(nodes)
		for _, node := range nodes {
			if node.Status() == WorkflowNodeFailed {
				summary.WorkflowFailed++
			}
		}
	}
	return summary, nil
}
