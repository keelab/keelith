package gorm

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// Description is a bounded, value-free GORM runtime snapshot.
type Description struct {
	State        string
	Ready        bool
	Active       int
	Operations   uint64
	Failures     uint64
	Instrumented bool
	Tracing      bool
	Metrics      bool
}

// Describe returns lifecycle and operation counters without database names,
// SQL, tables, arguments, or row values.
func (database *Database) Describe() Description {
	if database == nil {
		return Description{State: "unavailable"}
	}
	description := Description{
		State:        "created",
		Instrumented: database.telemetry != nil,
	}
	if database.started.Load() {
		description.State = "running"
		description.Ready = true
	}
	if database.closed.Load() {
		description.State = "stopped"
		description.Ready = false
	}
	if database.telemetry != nil {
		description.Active = int(database.telemetry.active.Load())
		description.Operations = database.telemetry.operationCount.Load()
		description.Failures = database.telemetry.failureCount.Load()
		description.Tracing = database.telemetry.tracer != nil
		description.Metrics = database.telemetry.operations != nil
	}
	return description
}

// RuntimeStatus adapts Database to the low-sensitive Ops Runtime Catalog.
func RuntimeStatus(database *Database) ops.RuntimeStatusProvider {
	if database == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := database.Describe()
		capabilities := []string{
			"gorm",
			"pool-lifecycle",
			"transactions",
		}
		if description.Metrics {
			capabilities = append(capabilities, "operation-metrics")
		}
		if description.Tracing {
			capabilities = append(capabilities, "operation-tracing")
		}
		return ops.RuntimeStatus{
			State:  description.State,
			Ready:  description.Ready,
			Active: description.Active,
			Counters: []ops.RuntimeCounter{
				{Name: "operations", Value: description.Operations},
				{Name: "failures", Value: description.Failures},
			},
			Capabilities: capabilities,
		}, nil
	}
}
