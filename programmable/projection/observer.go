package projection

import (
	"context"
	"reflect"
	"time"
)

// EventKind is a fixed low-cardinality synchronization lifecycle signal.
type EventKind string

const (
	// EventConnect records a source connection attempt.
	EventConnect EventKind = "connect"
	// EventReconnect records a source reconnection attempt.
	EventReconnect EventKind = "reconnect"
	// EventSnapshot records snapshot synchronization.
	EventSnapshot EventKind = "snapshot"
	// EventDelta records delta synchronization.
	EventDelta EventKind = "delta"
	// EventGap records a synchronization gap.
	EventGap EventKind = "gap"
	// EventLag records synchronization lag.
	EventLag EventKind = "lag"
	// EventError records a synchronization failure.
	EventError EventKind = "error"
)

// ErrorClass is a fixed failure category without error text or payload data.
type ErrorClass string

const (
	// ErrorNone indicates that an event has no associated failure.
	ErrorNone ErrorClass = ""
	// ErrorSource identifies a source failure.
	ErrorSource ErrorClass = "source"
	// ErrorStore identifies a store failure.
	ErrorStore ErrorClass = "store"
	// ErrorProtocol identifies a protocol failure.
	ErrorProtocol ErrorClass = "protocol"
	// ErrorContext identifies cancellation or timeout.
	ErrorContext ErrorClass = "context"
)

// Event is safe for metrics and traces. It intentionally has no cursor, key,
// payload, principal, endpoint, or raw error fields.
type Event struct {
	Kind       EventKind
	Projection ProjectionID
	ErrorClass ErrorClass
	Attempt    uint32
	Delay      time.Duration
	Lag        time.Duration
}

// Observer receives bounded projection synchronization events.
type Observer interface {
	ObserveProjection(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

// ObserveProjection calls function.
func (fn ObserverFunc) ObserveProjection(
	ctx context.Context,
	event Event,
) {
	fn(ctx, event)
}

func observeProjection(
	ctx context.Context,
	observer Observer,
	event Event,
) {
	if observer == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	observer.ObserveProjection(ctx, event)
}

func isNilObserver(observer Observer) bool {
	if observer == nil {
		return false
	}
	value := reflect.ValueOf(observer)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
