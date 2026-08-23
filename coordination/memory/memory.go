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

func (l *lease) Fence() uint64 {
	if l == nil {
		return 0
	}
	return l.fence
}

func (l *lease) Done() <-chan struct{} {
	if l == nil {
		return closed()
	}
	return l.done
}

func (l *lease) Err() error {
	if l == nil {
		return coordination.ErrLeaseLost
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *lease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", coordination.ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	l.release.Do(func() {
		l.coordinator.mu.Lock()
		if l.coordinator.leases[l.key] == l {
			delete(l.coordinator.leases, l.key)
			l.coordinator.released++
		}
		l.coordinator.mu.Unlock()
		close(l.done)
	})

	return l.Err()
}

func validKey(value string) bool {
	if value == "" || len(value) > 512 || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
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
