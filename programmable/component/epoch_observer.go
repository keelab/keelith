package component

import (
	"context"
	"reflect"

	"github.com/keelab/keelith/programmable/topology"
)

// EpochEventKind identifies one bounded epoch lifecycle observation.
type EpochEventKind string

const (
	// EpochEventStage indicates that a new epoch is being staged.
	EpochEventStage EpochEventKind = "stage"
	// EpochEventReady indicates that an epoch became ready.
	EpochEventReady EpochEventKind = "ready"
	// EpochEventDrain indicates that an epoch began draining.
	EpochEventDrain EpochEventKind = "drain"
	// EpochEventAcquire indicates that a lease was acquired.
	EpochEventAcquire EpochEventKind = "acquire"
	// EpochEventRelease indicates that a lease was released.
	EpochEventRelease EpochEventKind = "release"
	// EpochEventClose indicates that an epoch was closed.
	EpochEventClose EpochEventKind = "close"
)

// EpochEvent contains lifecycle metadata but never provider values or request
// payloads.
type EpochEvent struct {
	Kind   EpochEventKind
	Epoch  uint64
	State  topology.EpochState
	Failed bool
}

// EpochObserver receives epoch, lease, and provider-close observations.
type EpochObserver interface {
	ObserveEpoch(context.Context, EpochEvent)
}

// EpochObserverFunc adapts a function to EpochObserver.
type EpochObserverFunc func(context.Context, EpochEvent)

// ObserveEpoch implements EpochObserver.
func (fn EpochObserverFunc) ObserveEpoch(
	ctx context.Context,
	event EpochEvent,
) {
	fn(ctx, event)
}

func observeEpoch(
	ctx context.Context,
	observer EpochObserver,
	event EpochEvent,
) {
	if isNilEpochObserver(observer) {
		return
	}
	defer func() {
		_ = recover()
	}()
	observer.ObserveEpoch(ctx, event)
}

func isNilEpochObserver(observer EpochObserver) bool {
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
