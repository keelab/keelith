package worker

import (
	"context"
	"strings"
	"time"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// Execution is an immutable scheduled or ad-hoc job input.
type Execution struct {
	id          string
	scheduledAt time.Time
	payload     []byte
	metadata    metadata.Metadata
}

// NewExecution defensively copies a job input.
func NewExecution(id string, scheduledAt time.Time, payload []byte, inbound metadata.Metadata) Execution {
	return Execution{
		id:          strings.TrimSpace(id),
		scheduledAt: scheduledAt,
		payload:     append([]byte(nil), payload...),
		metadata:    inbound.Clone(),
	}
}

// ID returns the scheduler-provided execution identity.
func (e Execution) ID() string {
	return e.id
}

// ScheduledAt returns the planned execution time.
func (e Execution) ScheduledAt() time.Time {
	return e.scheduledAt
}

// Payload returns a defensive copy of job input.
func (e Execution) Payload() []byte {
	return append([]byte(nil), e.payload...)
}

// Metadata returns an immutable clone of execution metadata.
func (e Execution) Metadata() metadata.Metadata {
	return e.metadata.Clone()
}

// JobHandler maps one execution to the same explicit disposition used by
// Consumer. Scheduler adapters decide how Retry and DeadLetter are realized.
type JobHandler func(context.Context, Execution) Result

// Scheduler is implemented by Cron, platform, and remote job adapters.
type Scheduler interface {
	Schedule(context.Context, JobHandler) error
	StopPulling(context.Context) error
	Drain(context.Context) error
	Close(context.Context) error
	Wait() error
}

// TriggerAuthority identifies who decides when a job execution exists.
type TriggerAuthority string

const (
	// TriggerAuthorityUnknown is used by third-party schedulers without a
	// capability declaration.
	TriggerAuthorityUnknown TriggerAuthority = ""
	// TriggerAuthorityLocal means this process owns the schedule clock.
	TriggerAuthorityLocal TriggerAuthority = "local"
	// TriggerAuthorityExternal means a remote platform dispatches executions.
	TriggerAuthorityExternal TriggerAuthority = "external"
)

// OwnershipMode describes how executions are coordinated across replicas.
type OwnershipMode string

const (
	// OwnershipUnknown means no capability declaration is available.
	OwnershipUnknown OwnershipMode = ""
	// OwnershipPerReplica means every process may execute the same schedule.
	OwnershipPerReplica OwnershipMode = "per-replica"
	// OwnershipExternal means a remote scheduler owns coordination/sharding.
	OwnershipExternal OwnershipMode = "external"
	// OwnershipLease means a Keelith renewable lease admits each execution.
	OwnershipLease OwnershipMode = "lease"
)

// SchedulerCapabilities prevents invalid ownership composition and gives Ops
// a stable, vendor-neutral description.
type SchedulerCapabilities struct {
	TriggerAuthority TriggerAuthority
	Ownership        OwnershipMode
	Fencing          bool
	Sharding         bool
	RemoteKill       bool
}

// SchedulerCapabilitiesProvider is implemented by schedulers and decorators
// that can declare ownership semantics.
type SchedulerCapabilitiesProvider interface {
	SchedulerCapabilities() SchedulerCapabilities
}

// CapabilitiesOf returns a safe unknown description for third-party adapters.
func CapabilitiesOf(scheduler Scheduler) SchedulerCapabilities {
	if scheduler == nil {
		return SchedulerCapabilities{}
	}
	provider, ok := scheduler.(SchedulerCapabilitiesProvider)
	if !ok {
		return SchedulerCapabilities{}
	}
	return provider.SchedulerCapabilities()
}

// JobConfig constructs a scheduled Worker.
type JobConfig struct {
	Name       string
	Operation  operation.Operation
	Scheduler  Scheduler
	Handler    JobHandler
	Middleware *middleware.Bundle
	Health     *health.Registry
}

// Job exposes a scheduler as a Server-compatible runtime component.
type Job struct {
	*Worker
}

// NewJob constructs a Job around a scheduler adapter.
func NewJob(config JobConfig) (*Job, error) {
	if isNil(config.Scheduler) {
		return nil, invalidOption("scheduler is nil")
	}
	if config.Handler == nil {
		return nil, invalidOption("job handler is nil")
	}

	source := runtimeSource{
		start: func(ctx context.Context, dispatch middleware.Handler) error {
			return config.Scheduler.Schedule(
				ctx,
				func(ctx context.Context, execution Execution) Result {
					ctx = metadata.WithInbound(ctx, execution.Metadata())
					response, err := dispatch(ctx, execution)
					return normalizeResult(response, err)
				},
			)
		},
		stopPulling: config.Scheduler.StopPulling,
		drain:       config.Scheduler.Drain,
		close:       config.Scheduler.Close,
		wait:        config.Scheduler.Wait,
	}

	final := middleware.Handler(func(ctx context.Context, request any) (any, error) {
		execution, ok := request.(Execution)
		if !ok {
			return nil, ErrInvalidResult
		}
		result := config.Handler(ctx, execution)
		return result, result.Cause()
	})

	runtime, err := newWorker(config.Name, config.Operation, operation.KindJob, source, final, config.Middleware, config.Health)
	if err != nil {
		return nil, err
	}

	return &Job{Worker: runtime}, nil
}
