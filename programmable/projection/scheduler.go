package projection

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrInvalidScheduler reports malformed DRR bounds or requests.
	ErrInvalidScheduler = errors.New("projection: invalid scheduler")
	// ErrSchedulerFull reports a bounded tenant or global wait queue.
	ErrSchedulerFull = errors.New("projection: scheduler queue full")
	// ErrSchedulerClosed reports admission after scheduler shutdown.
	ErrSchedulerClosed = errors.New("projection: scheduler closed")
)

// SchedulerConfig bounds deficit round robin queues and class quantums.
type SchedulerConfig struct {
	QuantumByClass     map[TenantClass]int64
	MaxQueuedPerTenant int
	MaxQueuedTotal     int
	MaxCostBytes       int64
	RetryAfter         time.Duration
}

// SchedulerFullError carries a bounded retry delay without tenant identity.
type SchedulerFullError struct {
	Class      TenantClass
	RetryAfter time.Duration
}

func (full *SchedulerFullError) Error() string {
	if full == nil {
		return ErrSchedulerFull.Error()
	}
	return fmt.Sprintf("%s: class %s", ErrSchedulerFull, full.Class)
}

func (*SchedulerFullError) Unwrap() error { return ErrSchedulerFull }

// FairScheduler serializes frame admission using bounded deficit round robin.
// It owns no background goroutine: enqueue, cancel, and Permit.Close all drive
// dispatch synchronously.
type FairScheduler struct {
	mu         sync.Mutex
	quantum    map[TenantClass]int64
	perTenant  int
	totalLimit int
	maxCost    int64
	retryAfter time.Duration
	queues     map[[sha256.Size]byte]*tenantQueue
	order      []*tenantQueue
	next       int
	queued     int
	active     bool
	closed     bool
}

type tenantQueue struct {
	key     [sha256.Size]byte
	class   TenantClass
	deficit int64
	waiters []*scheduleWaiter
}

type scheduleWaiter struct {
	cost    int64
	result  chan scheduleResult
	queue   *tenantQueue
	granted bool
	permit  *SchedulePermit
}

type scheduleResult struct {
	permit *SchedulePermit
	err    error
}

// SchedulePermit releases the single scheduler execution slot.
type SchedulePermit struct {
	scheduler *FairScheduler
	once      sync.Once
}

// NewFairScheduler validates and snapshots bounded DRR configuration.
func NewFairScheduler(config SchedulerConfig) (*FairScheduler, error) {
	if len(config.QuantumByClass) == 0 || len(config.QuantumByClass) > 3 ||
		config.MaxQueuedPerTenant <= 0 || config.MaxQueuedPerTenant > 1_000_000 ||
		config.MaxQueuedTotal < config.MaxQueuedPerTenant ||
		config.MaxQueuedTotal > 10_000_000 ||
		config.MaxCostBytes <= 0 || config.MaxCostBytes > maxDeltaBytes ||
		config.RetryAfter <= 0 || config.RetryAfter > time.Hour {
		return nil, ErrInvalidScheduler
	}
	quantum := make(map[TenantClass]int64, len(config.QuantumByClass))
	for class, value := range config.QuantumByClass {
		if !validTenantClass(class) || value <= 0 || value > config.MaxCostBytes {
			return nil, ErrInvalidScheduler
		}
		quantum[class] = value
	}
	if _, exists := quantum[TenantStandard]; !exists {
		return nil, ErrInvalidScheduler
	}
	return &FairScheduler{
		quantum:    quantum,
		perTenant:  config.MaxQueuedPerTenant,
		totalLimit: config.MaxQueuedTotal,
		maxCost:    config.MaxCostBytes,
		retryAfter: config.RetryAfter,
		queues:     make(map[[sha256.Size]byte]*tenantQueue),
	}, nil
}

// Acquire queues one cost and blocks until its DRR turn or context cancel.
func (f *FairScheduler) Acquire(ctx context.Context, tenant Tenant, costBytes int) (*SchedulePermit, error) {
	if f == nil || ctx == nil || !tenant.Valid() || costBytes <= 0 {
		return nil, ErrInvalidScheduler
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	cost := int64(costBytes)
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, ErrSchedulerClosed
	}
	quantum, exists := f.quantum[tenant.class]
	if !exists || cost > f.maxCost {
		f.mu.Unlock()
		return nil, ErrInvalidScheduler
	}
	queue := f.queues[tenant.key]
	if queue == nil {
		queue = &tenantQueue{key: tenant.key, class: tenant.class, deficit: quantum}
		f.queues[tenant.key] = queue
		f.order = append(f.order, queue)
	} else if queue.class != tenant.class {
		f.mu.Unlock()
		return nil, ErrInvalidScheduler
	}
	if len(queue.waiters) >= f.perTenant || f.queued >= f.totalLimit {
		if len(queue.waiters) == 0 {
			f.removeQueueLocked(queue)
		}
		f.mu.Unlock()
		return nil, &SchedulerFullError{Class: tenant.class, RetryAfter: f.retryAfter}
	}
	waiter := &scheduleWaiter{
		cost:   cost,
		result: make(chan scheduleResult, 1),
		queue:  queue,
	}
	queue.waiters = append(queue.waiters, waiter)
	f.queued++
	f.dispatchLocked()
	f.mu.Unlock()

	select {
	case result := <-waiter.result:
		return result.permit, result.err
	case <-ctx.Done():
		f.mu.Lock()
		if waiter.granted {
			permit := waiter.permit
			f.mu.Unlock()
			_ = permit.Close()
			return nil, context.Cause(ctx)
		}
		f.removeWaiterLocked(waiter)
		f.dispatchLocked()
		f.mu.Unlock()
		return nil, context.Cause(ctx)
	}
}

// Close releases the scheduler slot and dispatches the next tenant.
func (permit *SchedulePermit) Close() error {
	if permit == nil {
		return nil
	}
	permit.once.Do(func() {
		scheduler := permit.scheduler
		if scheduler == nil {
			return
		}
		scheduler.mu.Lock()
		scheduler.active = false
		scheduler.dispatchLocked()
		scheduler.mu.Unlock()
	})
	return nil
}

// Close rejects new work and unblocks every queued request.
func (f *FairScheduler) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	for _, queue := range f.order {
		for _, waiter := range queue.waiters {
			waiter.result <- scheduleResult{err: ErrSchedulerClosed}
		}
	}
	f.queues = make(map[[sha256.Size]byte]*tenantQueue)
	f.order = nil
	f.queued = 0
	f.mu.Unlock()
	return nil
}

// Queued returns the current bounded wait count.
func (f *FairScheduler) Queued() int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queued
}

func (f *FairScheduler) dispatchLocked() {
	if f.active || f.closed || f.queued == 0 {
		return
	}
	for len(f.order) > 0 {
		if f.next >= len(f.order) {
			f.next = 0
		}
		queue := f.order[f.next]
		if len(queue.waiters) == 0 {
			f.removeQueueAtLocked(f.next)
			continue
		}
		waiter := queue.waiters[0]
		if waiter.cost > queue.deficit {
			queue.deficit += f.quantum[queue.class]
			f.next = (f.next + 1) % len(f.order)
			continue
		}
		queue.deficit -= waiter.cost
		queue.waiters = queue.waiters[1:]
		f.queued--
		permit := &SchedulePermit{scheduler: f}
		waiter.granted = true
		waiter.permit = permit
		f.active = true
		f.next = (f.next + 1) % len(f.order)
		waiter.result <- scheduleResult{permit: permit}
		return
	}
}

func (f *FairScheduler) removeWaiterLocked(waiter *scheduleWaiter) {
	queue := waiter.queue
	if queue == nil {
		return
	}
	for index, candidate := range queue.waiters {
		if candidate == waiter {
			queue.waiters = append(queue.waiters[:index], queue.waiters[index+1:]...)
			f.queued--
			break
		}
	}
	if len(queue.waiters) == 0 {
		f.removeQueueLocked(queue)
	}
}

func (f *FairScheduler) removeQueueLocked(target *tenantQueue) {
	for index, queue := range f.order {
		if queue == target {
			f.removeQueueAtLocked(index)
			return
		}
	}
}

func (f *FairScheduler) removeQueueAtLocked(index int) {
	queue := f.order[index]
	delete(f.queues, queue.key)
	f.order = append(f.order[:index], f.order[index+1:]...)
	if len(f.order) == 0 {
		f.next = 0
		return
	}
	if index < f.next {
		f.next--
	}
	if f.next >= len(f.order) {
		f.next = 0
	}
}
