// Package tracing provides instance-scoped OpenTelemetry tracing.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/attempt"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/observability/completion"
	kresource "github.com/keelab/keelith/observability/resource"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/placement"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/keelab/keelith"

const maxStableReasonBytes = 128

// ErrInvalidOption reports an invalid tracing dependency.
var ErrInvalidOption = errors.New("tracing: invalid option")

// Direction identifies inbound server or outbound client instrumentation.
type Direction string

const (
	// DirectionServer identifies an inbound invocation.
	DirectionServer Direction = "server"
	// DirectionClient identifies an outbound invocation.
	DirectionClient Direction = "client"
)

// Provider owns one SDK TracerProvider.
type Provider struct {
	provider *sdktrace.TracerProvider
}

// StreamMiddleware creates one span per stream and records a bounded number of
// payload-free message events.
func (provider *Provider) StreamMiddleware(
	direction Direction,
	maxEvents int,
) middleware.StreamMiddleware {
	return func(next middleware.StreamHandler) middleware.StreamHandler {
		var active atomic.Bool
		var recorded atomic.Int64
		var dropped atomic.Int64
		var streamContext context.Context
		var streamSpan trace.Span
		return func(
			ctx context.Context,
			event middleware.StreamEvent,
		) error {
			target, ok := operation.FromContext(ctx)
			if !ok {
				return next(ctx, event)
			}
			switch event.Phase {
			case middleware.StreamPhaseCreate:
				kind := trace.SpanKindServer
				if direction == DirectionClient {
					kind = trace.SpanKindClient
				}
				streamContext, streamSpan = provider.provider.
					Tracer(instrumentationName).
					Start(
						ctx,
						target.PolicyKey()+"/stream",
						trace.WithSpanKind(kind),
						trace.WithAttributes(operationAttributes(
							ctx, target,
							direction,
							attempt.FromContext(ctx),
						)...),
					)
				err := next(streamContext, event)
				if err != nil {
					recordSpanError(streamSpan, err, direction)
					streamSpan.End()
					return err
				}
				active.Store(true)
				return nil

			case middleware.StreamPhaseSend,
				middleware.StreamPhaseReceive:
				eventContext := ctx
				if active.Load() {
					eventContext = streamContext
				}
				err := next(eventContext, event)
				if active.Load() && maxEvents > 0 {
					number := recorded.Add(1)
					if number <= int64(maxEvents) {
						attributes := []attribute.KeyValue{
							attribute.Int64(
								"stream.sequence",
								int64(event.Sequence),
							),
						}
						if err != nil && !errors.Is(err, io.EOF) {
							result := completion.Classify(err)
							attributes = append(
								attributes,
								attribute.String(
									"keelith.outcome",
									string(result.Outcome()),
								),
								attribute.String(
									"failure.kind",
									string(result.FailureKind()),
								),
							)
						}
						streamSpan.AddEvent(
							"stream."+string(event.Phase),
							trace.WithAttributes(attributes...),
						)
					} else {
						dropped.Add(1)
					}
				}
				if active.Load() &&
					err != nil &&
					!errors.Is(err, io.EOF) {
					markSpanFailure(streamSpan, err, direction)
				}
				return err

			case middleware.StreamPhaseFinish:
				eventContext := ctx
				if active.Load() {
					eventContext = streamContext
				}
				err := next(eventContext, event)
				if active.CompareAndSwap(true, false) {
					if count := dropped.Load(); count > 0 {
						streamSpan.SetAttributes(attribute.Int64(
							"stream.events.dropped",
							count,
						))
					}
					terminalErr := errors.Join(event.Error, err)
					recordSpanCompletion(streamSpan, terminalErr, direction)
					streamSpan.End()
				}
				return err

			default:
				return next(ctx, event)
			}
		}
	}
}

func recordSpanError(span trace.Span, err error, direction Direction) {
	if span == nil || err == nil {
		return
	}
	recordSpanCompletion(span, err, direction)
}

func markSpanFailure(span trace.Span, err error, direction Direction) {
	if span == nil || err == nil {
		return
	}
	recordSpanCompletion(span, err, direction)
}

func recordSpanCompletion(
	span trace.Span,
	err error,
	direction Direction,
) {
	if span == nil {
		return
	}
	result := completion.Classify(err)
	attributes := []attribute.KeyValue{
		attribute.String("keelith.outcome", string(result.Outcome())),
	}
	if result.FailureKind() != failure.None {
		attributes = append(
			attributes,
			attribute.String("failure.kind", string(result.FailureKind())),
		)
	}
	attributes = append(attributes, stableApplicationErrorAttributes(err)...)
	span.SetAttributes(attributes...)
	if !result.CountsAsError(completionDirection(direction)) {
		return
	}
	span.SetStatus(codes.Error, string(result.Outcome()))
}

func stableApplicationErrorAttributes(err error) []attribute.KeyValue {
	if err == nil {
		return nil
	}
	var applicationError *kerrors.Error
	if !errors.As(err, &applicationError) {
		return nil
	}
	attributes := []attribute.KeyValue{
		attribute.Int64("error.code", int64(applicationError.Code())),
	}
	if reason := applicationError.Reason(); stableReason(reason) {
		attributes = append(
			attributes,
			attribute.String("error.reason", reason),
		)
	}
	return attributes
}

func stableReason(reason string) bool {
	if reason == "" || len(reason) > maxStableReasonBytes {
		return false
	}
	for _, character := range reason {
		if character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' {
			continue
		}
		return false
	}
	return true
}

// New creates an isolated provider. A nil exporter creates a valid no-export
// provider without mutating the OTel global.
func New(
	resource *kresource.Resource,
	exporter sdktrace.SpanExporter,
	processors ...sdktrace.SpanProcessor,
) (*Provider, error) {
	if resource == nil {
		return nil, fmt.Errorf("%w: resource is nil", ErrInvalidOption)
	}
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.OTel()),
	}
	if len(processors) > 1 {
		return nil, fmt.Errorf(
			"%w: more than one span processor",
			ErrInvalidOption,
		)
	}
	var processor sdktrace.SpanProcessor
	if len(processors) == 1 {
		processor = processors[0]
	}
	if exporter != nil && processor != nil {
		return nil, fmt.Errorf(
			"%w: span exporter and processor are mutually exclusive",
			ErrInvalidOption,
		)
	}
	if exporter != nil {
		if isNil(exporter) {
			return nil, fmt.Errorf("%w: exporter is typed nil", ErrInvalidOption)
		}
		options = append(options, sdktrace.WithSyncer(exporter))
	}
	if processor != nil {
		if isNil(processor) {
			return nil, fmt.Errorf("%w: processor is typed nil", ErrInvalidOption)
		}
		options = append(options, sdktrace.WithSpanProcessor(processor))
	}
	return &Provider{
		provider: sdktrace.NewTracerProvider(options...),
	}, nil
}

// TracerProvider returns this instance's provider.
func (provider *Provider) TracerProvider() trace.TracerProvider {
	return provider.provider
}

// Middleware creates low-cardinality server or client spans.
func (provider *Provider) Middleware(
	direction Direction,
) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			target, ok := operation.FromContext(ctx)
			if !ok {
				return next(ctx, request)
			}
			kind := trace.SpanKindServer
			if direction == DirectionClient {
				kind = trace.SpanKindClient
			}
			ctx, span := provider.provider.Tracer(instrumentationName).Start(
				ctx,
				target.PolicyKey(),
				trace.WithSpanKind(kind),
				trace.WithAttributes(operationAttributes(
					ctx, target,
					direction,
					attempt.FromContext(ctx),
				)...),
			)
			defer func() {
				if recovered := recover(); recovered != nil {
					span.SetStatus(codes.Error, "panic")
					span.SetAttributes(
						attribute.String("keelith.outcome", "error"),
						attribute.String("failure.kind", "panic"),
					)
					span.End()
					panic(recovered)
				}
				span.End()
			}()
			response, err := next(ctx, request)
			recordSpanCompletion(span, err, direction)
			return response, err
		}
	}
}

func completionDirection(direction Direction) completion.Direction {
	if direction == DirectionClient {
		return completion.DirectionClient
	}
	return completion.DirectionServer
}

// ForceFlush exports pending spans.
func (provider *Provider) ForceFlush(ctx context.Context) error {
	if provider == nil || provider.provider == nil {
		return nil
	}
	return provider.provider.ForceFlush(ctx)
}

// Shutdown releases span processors and exporters.
func (provider *Provider) Shutdown(ctx context.Context) error {
	if provider == nil || provider.provider == nil {
		return nil
	}
	return provider.provider.Shutdown(ctx)
}

func operationAttributes(
	ctx context.Context,
	target operation.Operation,
	direction Direction,
	attemptNumber int,
) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		attribute.String("operation.transport", target.Transport()),
		attribute.String("operation.service", target.Service()),
		attribute.String("operation.method", target.Method()),
		attribute.String("operation.kind", string(target.Kind())),
		attribute.String("keelith.direction", string(direction)),
		attribute.Int("keelith.attempt", attemptNumber),
	}
	if current, ok := placement.FromContext(ctx); ok {
		attributes = append(attributes,
			attribute.String("keelith.listener.name", current.Listener()),
			attribute.String("keelith.profile.name", current.Profile()),
			attribute.String("keelith.group.name", current.GroupAttribute()),
		)
	}
	return attributes
}

func isNil(value any) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
