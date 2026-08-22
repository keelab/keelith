// Package bulkhead provides dependency-scoped concurrency isolation.
package bulkhead

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/policy"
)

var (
	// ErrFull reports a fail-fast rejection because all concurrency and queue
	// slots are occupied.
	ErrFull = kerrors.New(503, "BULKHEAD_FULL", "dependency bulkhead is full")
	// ErrQueueTimeout reports expiration while waiting in the bounded queue.
	ErrQueueTimeout = kerrors.New(
		503,
		"BULKHEAD_QUEUE_TIMEOUT",
		"dependency bulkhead queue timed out",
	)
	// ErrInvalidOption reports an invalid dependency or resolved policy.
	ErrInvalidOption = errors.New("bulkhead: invalid option")
)

// Clock supplies deterministic queue timeout channels.
type Clock interface {
	After(time.Duration) <-chan time.Time
}

// Description is one bounded dependency-isolation snapshot.
type Description struct {
	Key                string
	MaxConcurrency     int
	MaxQueue           int
	Inflight           int
	Queued             int
	QueueHighWater     int
	Accepted           uint64
	Rejected           uint64
	QueueTimeouts      uint64
	QueueCancellations uint64
}

type waiterState uint8

const (
	waiterQueued waiterState = iota
	waiterGranted
	waiterCanceled
)

type waiter struct {
	ready chan struct{}
	state waiterState
}

// Bulkhead isolates concurrent logical calls to one dependency key.
//
// Waiting is FIFO. A slot is reserved before a queued waiter is awakened, so
// a newly arriving caller cannot overtake it.
type Bulkhead struct {
	clock Clock

	mu                 sync.Mutex
	maxConcurrency     int
	maxQueue           int
	inflight           int
	waiters            []*waiter
	queueHighWater     int
	accepted           uint64
	rejected           uint64
	queueTimeouts      uint64
	queueCancellations uint64
}

// Permit releases one logical-call slot exactly once.
type Permit struct {
	bulkhead *Bulkhead
	once     sync.Once
}

// NewBulkhead creates one isolated dependency gate.
func NewBulkhead(clock Clock) *Bulkhead {
	if isNil(clock) {
		clock = systemClock{}
	}
	return &Bulkhead{clock: clock}
}

// Acquire obtains a logical-call slot or waits in the bounded FIFO queue.
func (b *Bulkhead) Acquire(ctx context.Context, config policy.BulkheadPolicy) (*Permit, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: bulkhead is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if !config.Enabled {
		return &Permit{}, nil
	}
	if err := validate(config); err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.maxConcurrency = config.MaxConcurrency
	b.maxQueue = config.MaxQueue

	for b.inflight < b.maxConcurrency &&
		len(b.waiters) > 0 {
		b.grantNextLocked()
	}
	if b.inflight < config.MaxConcurrency && len(b.waiters) == 0 {
		b.inflight++
		b.accepted++
		b.mu.Unlock()
		return &Permit{bulkhead: b}, nil
	}
	if config.MaxQueue == 0 || len(b.waiters) >= config.MaxQueue {
		b.rejected++
		b.mu.Unlock()
		return nil, ErrFull
	}
	candidate := &waiter{ready: make(chan struct{}), state: waiterQueued}
	b.waiters = append(b.waiters, candidate)
	if len(b.waiters) > b.queueHighWater {
		b.queueHighWater = len(b.waiters)
	}
	b.mu.Unlock()

	select {
	case <-candidate.ready:
		if cause := context.Cause(ctx); cause != nil {
			b.cancelGranted(candidate, false)
			return nil, cause
		}
		return &Permit{bulkhead: b}, nil
	case <-ctx.Done():
		b.cancelGranted(candidate, false)
		return nil, context.Cause(ctx)
	case <-b.clock.After(config.QueueTimeout):
		b.cancelGranted(candidate, true)
		return nil, ErrQueueTimeout
	}
}

// Release returns one logical-call slot and grants the oldest queued waiter.
func (permit *Permit) Release() {
	if permit == nil || permit.bulkhead == nil {
		return
	}
	permit.once.Do(func() {
		permit.bulkhead.release()
	})
}

// Describe returns immutable gate state.
func (b *Bulkhead) Describe() Description {
	if b == nil {
		return Description{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return Description{
		MaxConcurrency:     b.maxConcurrency,
		MaxQueue:           b.maxQueue,
		Inflight:           b.inflight,
		Queued:             len(b.waiters),
		QueueHighWater:     b.queueHighWater,
		Accepted:           b.accepted,
		Rejected:           b.rejected,
		QueueTimeouts:      b.queueTimeouts,
		QueueCancellations: b.queueCancellations,
	}
}

func (b *Bulkhead) release() {
	b.mu.Lock()
	if b.inflight > 0 {
		b.inflight--
	}
	b.grantNextLocked()
	b.mu.Unlock()
}

// cancelGranted removes a queued waiter. If it had already received a slot,
// that reservation is released and handed to the next waiter.
//
// It returns true when the timeout/cancellation won while the waiter was
// still queued.
func (b *Bulkhead) cancelGranted(candidate *waiter, timedOut bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch candidate.state {
	case waiterQueued:
		candidate.state = waiterCanceled
		b.removeWaiterLocked(candidate)
		if timedOut {
			b.queueTimeouts++
		} else {
			b.queueCancellations++
		}
		b.grantNextLocked()
		return true
	case waiterGranted:
		candidate.state = waiterCanceled
		if b.inflight > 0 {
			b.inflight--
		}
		if timedOut {
			b.queueTimeouts++
		} else {
			b.queueCancellations++
		}
		b.grantNextLocked()
		return false
	default:
		return true
	}
}

func (b *Bulkhead) grantNextLocked() {
	if b.maxConcurrency <= 0 ||
		b.inflight >= b.maxConcurrency {
		return
	}
	for len(b.waiters) > 0 {
		candidate := b.waiters[0]
		b.waiters = b.waiters[1:]
		if candidate.state != waiterQueued {
			continue
		}
		candidate.state = waiterGranted
		b.inflight++
		b.accepted++
		close(candidate.ready)
		return
	}
}

func (b *Bulkhead) removeWaiterLocked(candidate *waiter) {
	for index, current := range b.waiters {
		if current != candidate {
			continue
		}
		copy(b.waiters[index:], b.waiters[index+1:])
		b.waiters[len(b.waiters)-1] = nil
		b.waiters = b.waiters[:len(b.waiters)-1]
		return
	}
}

func validate(config policy.BulkheadPolicy) error {
	if config.MaxConcurrency <= 0 ||
		config.MaxQueue < 0 ||
		config.QueueTimeout < 0 ||
		config.MaxQueue > 0 && config.QueueTimeout <= 0 ||
		config.MaxQueue == 0 && config.QueueTimeout > 0 {
		return fmt.Errorf("%w: resolved bulkhead policy is invalid", policy.ErrInvalidPolicy)
	}
	return nil
}

// Pool owns Bulkhead state by a stable dependency key.
type Pool struct {
	clock Clock

	mu        sync.Mutex
	bulkheads map[string]*Bulkhead
}

// NewPool creates an isolated dependency bulkhead pool.
func NewPool(clock Clock) *Pool {
	if isNil(clock) {
		clock = systemClock{}
	}
	return &Pool{
		clock:     clock,
		bulkheads: make(map[string]*Bulkhead),
	}
}

// Get returns the stable Bulkhead for key.
func (p *Pool) Get(key string) *Bulkhead {
	if p == nil {
		return nil
	}
	normalized := strings.TrimSpace(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	subject := p.bulkheads[normalized]
	if subject == nil {
		subject = NewBulkhead(p.clock)
		p.bulkheads[normalized] = subject
	}
	return subject
}

// Describe returns sorted low-cardinality snapshots.
func (p *Pool) Describe() []Description {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	entries := make(map[string]*Bulkhead, len(p.bulkheads))
	for key, subject := range p.bulkheads {
		entries[key] = subject
	}
	p.mu.Unlock()
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Description, 0, len(keys))
	for _, key := range keys {
		description := entries[key].Describe()
		description.Key = key
		result = append(result, description)
	}
	return result
}

type systemClock struct{}

func (systemClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
