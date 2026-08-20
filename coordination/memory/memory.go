// Package memory provides process-local coordination for development and
// contract verification. It is not a distributed ownership implementation.
package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/coordination"
)

// Description is a value-free coordinator snapshot.
type Description struct {
	Active   int
	Acquired uint64
	Rejected uint64
	Released uint64
}

// Coordinator owns in-process lease keys.
type Coordinator struct {
	mu       sync.Mutex
	leases   map[string]*lease
	fences   map[string]uint64
	acquired uint64
	rejected uint64
	released uint64
}

// New constructs an empty process-local Coordinator.
func New() *Coordinator {
	return &Coordinator{
		leases: make(map[string]*lease),
		fences: make(map[string]uint64),
	}
}

// TryAcquire atomically acquires key when no local owner exists.
func (c *Coordinator) TryAcquire(ctx context.Context, key string, ttl time.Duration) (coordination.Lease, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf("%w: coordinator is nil", coordination.ErrInvalidOption)
	}
	if ctx == nil || ttl <= 0 || !validKey(key) {
		return nil, false, fmt.Errorf("%w: context, key, or TTL", coordination.ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, false, cause
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.leases[key]; exists {
		c.rejected++
		return nil, false, nil
	}
	c.fences[key]++
	acquired := &lease{
		coordinator: c,
		key:         key,
		fence:       c.fences[key],
		done:        make(chan struct{}),
	}
	c.leases[key] = acquired
	c.acquired++
	return acquired, true, nil
}

// Description returns bounded aggregate counters.
func (c *Coordinator) Description() Description {
	if c == nil {
		return Description{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	return Description{
		Active:   len(c.leases),
		Acquired: c.acquired,
		Rejected: c.rejected,
		Released: c.released,
	}
}

type lease struct {
	coordinator *Coordinator
	key         string
	fence       uint64
	done        chan struct{}

	mu      sync.Mutex
	err     error
	release sync.Once
}

func (lease *lease) Fence() uint64 {
	if lease == nil {
		return 0
	}
	return lease.fence
}

func (lease *lease) Done() <-chan struct{} {
	if lease == nil {
		return closed()
	}
	return lease.done
}

func (lease *lease) Err() error {
	if lease == nil {
		return coordination.ErrLeaseLost
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.err
}

func (lease *lease) Release(ctx context.Context) error {
	if lease == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", coordination.ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	lease.release.Do(func() {
		lease.coordinator.mu.Lock()
		if lease.coordinator.leases[lease.key] == lease {
			delete(lease.coordinator.leases, lease.key)
			lease.coordinator.released++
		}
		lease.coordinator.mu.Unlock()
		close(lease.done)
	})

	return lease.Err()
}

func validKey(value string) bool {
	if value == "" || len(value) > 512 || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func closed() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
