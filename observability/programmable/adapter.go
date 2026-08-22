// Package programmable exports bounded OpenTelemetry observers for Keelith's
// continuation, topology, and projection runtimes.
package programmable

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/keelab/keelith/programmable/continuation"
	"github.com/keelab/keelith/programmable/projection"
	"github.com/keelab/keelith/programmable/topology/control"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/keelab/keelith/programmable"

// Stable metric names shared by the adapter, dashboards, and alert rules.
const (
	MetricEvents         = "keelith.programmable.events"
	MetricFailures       = "keelith.programmable.failures"
	MetricActive         = "keelith.programmable.active"
	MetricReconnectDelay = "keelith.programmable.reconnect_delay"
	MetricLag            = "keelith.programmable.lag"
)

// RuntimeKind is one fixed programmable runtime data plane.
type RuntimeKind string

const (
	// RuntimeContinuation identifies durable continuation execution.
	RuntimeContinuation RuntimeKind = "continuation"
	// RuntimeTopology identifies dynamic topology control.
	RuntimeTopology RuntimeKind = "topology"
	// RuntimeProjection identifies projection synchronization.
	RuntimeProjection RuntimeKind = "projection"
)

// ErrInvalidOption reports a nil provider or unknown runtime kind.
var ErrInvalidOption = errors.New("programmable observability: invalid option")

// Adapter owns stable instruments but not the caller's MeterProvider.
type Adapter struct {
	events         otelmetric.Int64Counter
	failures       otelmetric.Int64Counter
	active         otelmetric.Int64UpDownCounter
	reconnectDelay otelmetric.Float64Histogram
	lag            otelmetric.Float64Histogram
}

// New constructs one observer set from an instance-scoped MeterProvider.
func New(provider otelmetric.MeterProvider) (*Adapter, error) {
	if nilMeterProvider(provider) {
		return nil, ErrInvalidOption
	}
	meter := provider.Meter(instrumentationName)
	events, err := meter.Int64Counter(MetricEvents)
	if err != nil {
		return nil, err
	}
	failures, err := meter.Int64Counter(MetricFailures)
	if err != nil {
		return nil, err
	}
	active, err := meter.Int64UpDownCounter(MetricActive)
	if err != nil {
		return nil, err
	}
	reconnectDelay, err := meter.Float64Histogram(
		MetricReconnectDelay,
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	lag, err := meter.Float64Histogram(MetricLag, otelmetric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return &Adapter{
		events: events, failures: failures, active: active,
		reconnectDelay: reconnectDelay, lag: lag,
	}, nil
}

// ContinuationObserver returns a low-cardinality continuation adapter.
func (adapter *Adapter) ContinuationObserver() continuation.Observer {
	if adapter == nil {
		return nil
	}
	return continuation.ObserverFunc(adapter.observeContinuation)
}

// TopologyObserver returns a low-cardinality topology control adapter.
func (adapter *Adapter) TopologyObserver() control.Observer {
	if adapter == nil {
		return nil
	}
	return control.ObserverFunc(adapter.observeTopology)
}

// ProjectionObserver returns a low-cardinality projection adapter.
func (adapter *Adapter) ProjectionObserver() projection.Observer {
	if adapter == nil {
		return nil
	}
	return projection.ObserverFunc(adapter.observeProjection)
}

// StartActivity increments the active up/down counter and returns an
// idempotent completion function. It never accepts a business identity.
func (adapter *Adapter) StartActivity(
	ctx context.Context,
	kind RuntimeKind,
) (func(), error) {
	if adapter == nil || !kind.valid() {
		return nil, ErrInvalidOption
	}
	ctx = nonNilContext(ctx)
	attributes := otelmetric.WithAttributes(attribute.String("keelith.runtime.kind", string(kind)))
	adapter.active.Add(ctx, 1, attributes)
	var once sync.Once
	return func() {
		once.Do(func() {
			adapter.active.Add(context.Background(), -1, attributes)
		})
	}, nil
}

func (adapter *Adapter) observeContinuation(ctx context.Context, event continuation.Event) {
	if adapter == nil {
		return
	}
	count := event.Count
	if count == 0 {
		count = 1
	}
	errorClass := continuationErrorClass(event.ErrorClass)
	options := metricAttributes(
		RuntimeContinuation,
		continuationEventKind(event.Kind),
		continuationStatus(event.Status),
		errorClass,
	)
	adapter.events.Add(nonNilContext(ctx), saturatingInt64(count), options)
	if errorClass != "none" {
		adapter.failures.Add(nonNilContext(ctx), 1, options)
	}
}

func (adapter *Adapter) observeTopology(ctx context.Context, event control.Event) {
	if adapter == nil {
		return
	}
	errorClass := topologyFailureClass(event.FailureClass)
	options := metricAttributes(
		RuntimeTopology,
		topologyEventKind(event.Kind),
		"none",
		errorClass,
	)
	adapter.events.Add(nonNilContext(ctx), 1, options)
	if errorClass != "none" {
		adapter.failures.Add(nonNilContext(ctx), 1, options)
	}
}

func (adapter *Adapter) observeProjection(ctx context.Context, event projection.Event) {
	if adapter == nil {
		return
	}
	errorClass := projectionErrorClass(event.ErrorClass)
	options := metricAttributes(
		RuntimeProjection,
		projectionEventKind(event.Kind),
		"none",
		errorClass,
	)
	ctx = nonNilContext(ctx)
	adapter.events.Add(ctx, 1, options)
	if errorClass != "none" {
		adapter.failures.Add(ctx, 1, options)
	}
	if event.Delay >= 0 && event.Kind == projection.EventReconnect {
		adapter.reconnectDelay.Record(ctx, durationSeconds(event.Delay), options)
	}
	if event.Lag >= 0 && event.Kind == projection.EventLag {
		adapter.lag.Record(ctx, durationSeconds(event.Lag), options)
	}
}

func metricAttributes(
	kind RuntimeKind,
	eventKind string,
	state string,
	errorClass string,
) otelmetric.MeasurementOption {
	return otelmetric.WithAttributes(
		attribute.String("keelith.runtime.kind", string(kind)),
		attribute.String("keelith.event.kind", eventKind),
		attribute.String("keelith.state", state),
		attribute.String("keelith.error.class", errorClass),
	)
}

func (kind RuntimeKind) valid() bool {
	switch kind {
	case RuntimeContinuation, RuntimeTopology, RuntimeProjection:
		return true
	default:
		return false
	}
}

func continuationEventKind(kind continuation.EventKind) string {
	switch kind {
	case continuation.EventClaim, continuation.EventRenew,
		continuation.EventTransition, continuation.EventGap,
		continuation.EventAttachLag, continuation.EventMachineError,
		continuation.EventGC:
		return string(kind)
	default:
		return "unknown"
	}
}

func topologyEventKind(kind control.EventKind) string {
	switch kind {
	case control.EventObserved, control.EventApplied, control.EventRejected, control.EventReconnect:
		return string(kind)
	default:
		return "unknown"
	}
}

func projectionEventKind(kind projection.EventKind) string {
	switch kind {
	case projection.EventConnect, projection.EventReconnect, projection.EventSnapshot,
		projection.EventDelta, projection.EventGap, projection.EventLag, projection.EventError:
		return string(kind)
	default:
		return "unknown"
	}
}

func continuationStatus(status continuation.Status) string {
	switch status {
	case continuation.StatusAccepted, continuation.StatusRunning,
		continuation.StatusWaiting, continuation.StatusSuspended,
		continuation.StatusCancelRequested, continuation.StatusCompleted,
		continuation.StatusFailed, continuation.StatusCanceled, continuation.StatusExpired:
		return string(status)
	case "":
		return "none"
	default:
		return "unknown"
	}
}

func continuationErrorClass(class continuation.ErrorClass) string {
	switch class {
	case continuation.ErrorClassRetryable, continuation.ErrorClassTerminal, continuation.ErrorClassInternal:
		return string(class)
	case continuation.ErrorClassNone:
		return "none"
	default:
		return "unknown"
	}
}

func topologyFailureClass(class control.FailureClass) string {
	switch class {
	case control.FailureSource, control.FailureSignature, control.FailureRevision,
		control.FailureEpoch, control.FailureStage, control.FailureReady, control.FailureDrain:
		return string(class)
	case control.FailureNone:
		return "none"
	default:
		return "unknown"
	}
}

func projectionErrorClass(class projection.ErrorClass) string {
	switch class {
	case projection.ErrorSource, projection.ErrorStore, projection.ErrorProtocol, projection.ErrorContext:
		return string(class)
	case projection.ErrorNone:
		return "none"
	default:
		return "unknown"
	}
}

func saturatingInt64(value uint64) int64 {
	const maximum = ^uint64(0) >> 1
	if value > maximum {
		return int64(maximum)
	}
	return int64(value)
}

func durationSeconds(value time.Duration) float64 {
	return float64(value) / float64(time.Second)
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func nilMeterProvider(provider otelmetric.MeterProvider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
