package coordination

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidOption reports an invalid coordinator, key, or lease budget.
	ErrInvalidOption = errors.New("coordination: invalid option")
	// ErrLeaseLost reports that ownership ended before explicit Release.
	ErrLeaseLost = errors.New("coordination: lease lost")
)

// Lease represents actively maintained ownership of one stable key.
//
// Done closes when ownership is lost or the lease is explicitly released.
// Err returns nil after a successful Release and a stable cause after loss.
type Lease interface {
	// Fence is a positive, monotonically increasing ownership generation for
	// this key. Downstream writes can reject a generation older than the last
	// accepted value.
	Fence() uint64
	Done() <-chan struct{}
	Err() error
	Release(context.Context) error
}

// Coordinator attempts to acquire one auto-maintained lease.
//
// acquired=false is normal contention and returns a nil Lease and nil error.
// ttl is the maximum backend outage window before ownership must be considered
// lost; implementations renew an acquired lease until Release.
type Coordinator interface {
	TryAcquire(context.Context, string, time.Duration) (lease Lease, acquired bool, err error)
}

type fenceContextKey struct{}

// WithFence adds one positive ownership generation to a Handler context.
func WithFence(ctx context.Context, fence uint64) context.Context {
	if ctx == nil || fence == 0 {
		return ctx
	}
	return context.WithValue(ctx, fenceContextKey{}, fence)
}

// FenceFromContext obtains an ownership generation without exposing a
// coordinator implementation to business code.
func FenceFromContext(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	fence, ok := ctx.Value(fenceContextKey{}).(uint64)
	return fence, ok && fence > 0
}
