package continuation

import (
	"context"
	"reflect"
)

// EventKind is one fixed, low-cardinality runtime observation.
type EventKind string

const (
	// EventClaim records a claim attempt.
	EventClaim EventKind = "claim"
	// EventRenew records a lease renewal.
	EventRenew EventKind = "renew"
	// EventTransition records a state transition.
	EventTransition EventKind = "transition"
	// EventGap records a detected history gap.
	EventGap EventKind = "gap"
	// EventAttachLag records attachment lag.
	EventAttachLag EventKind = "attach_lag"
	// EventMachineError records a machine failure.
	EventMachineError EventKind = "machine_error"
	// EventGC records garbage collection activity.
	EventGC EventKind = "gc"
)

// ErrorClass is one fixed Machine error disposition.
type ErrorClass string

const (
	// ErrorClassNone indicates that an event has no associated failure.
	ErrorClassNone ErrorClass = ""
	// ErrorClassRetryable identifies a retryable failure.
	ErrorClassRetryable ErrorClass = "retryable"
	// ErrorClassTerminal identifies a terminal failure.
	ErrorClassTerminal ErrorClass = "terminal"
	// ErrorClassInternal identifies an internal failure.
	ErrorClassInternal ErrorClass = "internal"
)

// Event intentionally excludes CallID, operation, command identity, and
// payload fields so observers cannot create high-cardinality or sensitive
// telemetry by accident.
type Event struct {
	Kind       EventKind
	Status     Status
	ErrorClass ErrorClass
	Count      uint64
}

// Observer receives low-cardinality continuation lifecycle events.
type Observer interface {
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

// Observe implements Observer.
func (function ObserverFunc) Observe(ctx context.Context, event Event) {
	function(ctx, event)
}

func (runtime *Runtime) observe(ctx context.Context, event Event) {
	if runtime == nil || isNilObserver(runtime.observer) {
		return
	}
	defer func() {
		_ = recover()
	}()
	runtime.observer.Observe(ctx, event)
}

func isNilObserver(observer Observer) bool {
	if observer == nil {
		return true
	}
	value := reflect.ValueOf(observer)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
