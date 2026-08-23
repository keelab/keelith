// Package owned decorates a worker Scheduler with distributed lease
// ownership.
package owned

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/coordination"
	"github.com/keelab/keelith/worker"
)

const defaultReleaseTimeout = 5 * time.Second

var (
	// ErrInvalidOption reports invalid ownership configuration.
	ErrInvalidOption = errors.New("owned scheduler: invalid option")
	// ErrOwnershipConflict reports a remote or already leased scheduler.
	ErrOwnershipConflict = errors.New("owned scheduler: ownership conflict")
)

// Config controls cluster ownership around one Scheduler.
type Config struct {
	Key            string
	TTL            time.Duration
	ReleaseTimeout time.Duration
	Coordinator    coordination.Coordinator
	Scheduler      worker.Scheduler
}

// Description is a value-free ownership snapshot.
type Description struct {
	Active          int
	Acquired        uint64
	Contended       uint64
	AcquireFailures uint64
	LeaseLosses     uint64
	ReleaseFailures uint64
	Capabilities    worker.SchedulerCapabilities
}

// Scheduler admits an execution only while this replica owns its lease.
type Scheduler struct {
	key            string
	ttl            time.Duration
	releaseTimeout time.Duration
	coordinator    coordination.Coordinator
	scheduler      worker.Scheduler

	mu           sync.Mutex
	description  Description
	capabilities worker.SchedulerCapabilities
}

// New creates a cluster-ownership Scheduler decorator.
func New(config Config) (*Scheduler, error) {
	if !validKey(config.Key) ||
		config.TTL <= 0 ||
		isNil(config.Coordinator) ||
		isNil(config.Scheduler) {
		return nil, fmt.Errorf(
			"%w: key, TTL, coordinator, or scheduler",
			ErrInvalidOption,
		)
	}
	releaseTimeout := config.ReleaseTimeout
	if releaseTimeout == 0 {
		releaseTimeout = defaultReleaseTimeout
		if config.TTL < releaseTimeout {
			releaseTimeout = config.TTL
		}
	}
	if releaseTimeout < 0 || releaseTimeout > config.TTL {
		return nil, fmt.Errorf(
			"%w: release timeout must be positive and not exceed TTL",
			ErrInvalidOption,
		)
	}
	wrapped := worker.CapabilitiesOf(config.Scheduler)
	if wrapped.TriggerAuthority == worker.TriggerAuthorityExternal ||
		wrapped.Ownership == worker.OwnershipExternal ||
		wrapped.Ownership == worker.OwnershipLease ||
		wrapped.Sharding {
		return nil, fmt.Errorf(
			"%w: scheduler is external, sharded, or already coordinated",
			ErrOwnershipConflict,
		)
	}
	capabilities := worker.SchedulerCapabilities{
		TriggerAuthority: wrapped.TriggerAuthority,
		Ownership:        worker.OwnershipLease,
		Fencing:          true,
		Sharding:         false,
		RemoteKill:       wrapped.RemoteKill,
	}
	return &Scheduler{
		key:            config.Key,
		ttl:            config.TTL,
		releaseTimeout: releaseTimeout,
		coordinator:    config.Coordinator,
		scheduler:      config.Scheduler,
		capabilities:   capabilities,
		description: Description{
			Capabilities: capabilities,
		},
	}, nil
}

// SchedulerCapabilities declares renewable lease ownership and fencing.
func (s *Scheduler) SchedulerCapabilities() worker.SchedulerCapabilities {
	if s == nil {
		return worker.SchedulerCapabilities{}
	}
	return s.capabilities
}

// Schedule starts the wrapped Scheduler with ownership admission.
func (s *Scheduler) Schedule(ctx context.Context, handler worker.JobHandler) error {
	if s == nil || handler == nil {
		return fmt.Errorf("%w: scheduler or handler is nil", ErrInvalidOption)
	}

	return s.scheduler.Schedule(ctx, func(ctx context.Context, execution worker.Execution) worker.Result {
		return s.runOwned(ctx, execution, handler)
	})
}

// StopPulling delegates to the wrapped Scheduler.
func (s *Scheduler) StopPulling(ctx context.Context) error {
	if s == nil {
		return nil
	}

	return s.scheduler.StopPulling(ctx)
}

// Drain delegates to the wrapped Scheduler. Active leases are part of wrapped
// Handler completion and therefore drain before this returns.
func (s *Scheduler) Drain(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.scheduler.Drain(ctx)
}

// Close delegates to the wrapped Scheduler.
func (s *Scheduler) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.scheduler.Close(ctx)
}

// Wait delegates to the wrapped Scheduler.
func (s *Scheduler) Wait() error {
	if s == nil {
		return nil
	}
	return s.scheduler.Wait()
}

// Description returns aggregate ownership state without lease keys or errors.
func (s *Scheduler) Description() Description {
	if s == nil {
		return Description{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.description
}

func (s *Scheduler) runOwned(ctx context.Context, execution worker.Execution, handler worker.JobHandler) (result worker.Result) {
	lease, acquired, err := s.coordinator.TryAcquire(ctx, s.key, s.ttl)

	if err != nil {
		s.record(func(description *Description) {
			description.AcquireFailures++
		})
		return worker.Nack(err)
	}
	if !acquired {
		s.record(func(description *Description) {
			description.Contended++
		})
		return worker.Ack()
	}
	if isNil(lease) {
		s.record(func(description *Description) {
			description.AcquireFailures++
		})
		return worker.Nack(fmt.Errorf("%w: acquired lease is nil", ErrInvalidOption))
	}
	if lease.Fence() == 0 {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.releaseTimeout)
		releaseErr := lease.Release(releaseCtx)
		cancel()
		s.record(func(description *Description) {
			description.AcquireFailures++
			if releaseErr != nil {
				description.ReleaseFailures++
			}
		})
		return worker.Nack(fmt.Errorf("%w: acquired lease has no fencing token", ErrInvalidOption))
	}
	s.record(func(description *Description) {
		description.Acquired++
		description.Active++
	})

	ownedCtx, cancel := context.WithCancelCause(coordination.WithFence(ctx, lease.Fence()))
	handlerDone := make(chan struct{})

	go func() {
		select {
		case <-lease.Done():
			if lease.Err() != nil {
				cancel(coordination.ErrLeaseLost)
			}
		case <-handlerDone:
		}
	}()

	defer func() {
		close(handlerDone)
		cancel(nil)
		recovered := recover()
		lost := lease.Err()
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), s.releaseTimeout)
		releaseErr := lease.Release(releaseCtx)
		releaseCancel()
		if lost == nil && errors.Is(releaseErr, coordination.ErrLeaseLost) {
			lost = releaseErr
		}
		s.record(func(description *Description) {
			description.Active--
			if lost != nil {
				description.LeaseLosses++
			}
			if releaseErr != nil {
				description.ReleaseFailures++
			}
		})
		if recovered != nil {
			panic(recovered)
		}
		if lost != nil {
			result = worker.Nack(errors.Join(coordination.ErrLeaseLost, lost))
		}
	}()

	return handler(ownedCtx, execution)
}

func (s *Scheduler) record(update func(*Description)) {
	s.mu.Lock()
	update(&s.description)
	s.mu.Unlock()
}

func validKey(value string) bool {
	if value == "" ||
		len(value) > 512 ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
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
