// Package metrics provides instance-scoped OpenTelemetry RPC metrics.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/observability/completion"
	kresource "github.com/keelab/keelith/observability/resource"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/placement"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const instrumentationName = "github.com/keelab/keelith"

// ErrInvalidOption reports an invalid metrics dependency.
var ErrInvalidOption = errors.New("metrics: invalid option")

// Reader is an SDK metric reader.
type Reader = sdkmetric.Reader

// Direction identifies inbound server or outbound client metrics.
type Direction string

const (
	// DirectionServer identifies an inbound invocation.
	DirectionServer Direction = "server"
	// DirectionClient identifies an outbound invocation.
	DirectionClient Direction = "client"
)

// Provider owns one SDK MeterProvider and its stable instruments.
type Provider struct {
	provider *sdkmetric.MeterProvider

	requests otelmetric.Int64Counter
	errors   otelmetric.Int64Counter
	duration otelmetric.Float64Histogram
	inflight otelmetric.Int64UpDownCounter

	streamsActive         otelmetric.Int64UpDownCounter
	streamsCompleted      otelmetric.Int64Counter
	streamErrors          otelmetric.Int64Counter
	streamDuration        otelmetric.Float64Histogram
	streamMessages        otelmetric.Int64Counter
	streamMessageErrors   otelmetric.Int64Counter
	streamMessageDuration otelmetric.Float64Histogram
}

// New creates an isolated MeterProvider.
func New(
	resource *kresource.Resource,
	readers ...Reader,
) (*Provider, error) {
	if resource == nil {
		return nil, fmt.Errorf("%w: resource is nil", ErrInvalidOption)
	}
	options := []sdkmetric.Option{
		sdkmetric.WithResource(resource.OTel()),
		sdkmetric.WithCardinalityLimit(1_000),
	}
	for index, reader := range readers {
		if reader == nil {
			return nil, fmt.Errorf(
				"%w: reader %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		options = append(options, sdkmetric.WithReader(reader))
	}
	provider := sdkmetric.NewMeterProvider(options...)
	meter := provider.Meter(instrumentationName)
	requests, err := meter.Int64Counter("keelith.rpc.requests")
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	failures, err := meter.Int64Counter("keelith.rpc.errors")
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	duration, err := meter.Float64Histogram(
		"keelith.rpc.duration",
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	inflight, err := meter.Int64UpDownCounter("keelith.rpc.inflight")
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	streamsActive, err := meter.Int64UpDownCounter("keelith.stream.active")
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	streamsCompleted, err := meter.Int64Counter("keelith.stream.completed")
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	streamErrors, err := meter.Int64Counter("keelith.stream.errors")
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	streamDuration, err := meter.Float64Histogram(
		"keelith.stream.duration",
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	streamMessages, err := meter.Int64Counter("keelith.stream.messages")
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	streamMessageErrors, err := meter.Int64Counter(
		"keelith.stream.message.errors",
	)
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	streamMessageDuration, err := meter.Float64Histogram(
		"keelith.stream.message.duration",
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, err
	}
	return &Provider{
		provider:              provider,
		requests:              requests,
		errors:                failures,
		duration:              duration,
		inflight:              inflight,
		streamsActive:         streamsActive,
		streamsCompleted:      streamsCompleted,
		streamErrors:          streamErrors,
		streamDuration:        streamDuration,
		streamMessages:        streamMessages,
		streamMessageErrors:   streamMessageErrors,
		streamMessageDuration: streamMessageDuration,
	}, nil
}

// MeterProvider returns this instance's provider.
func (provider *Provider) MeterProvider() otelmetric.MeterProvider {
	return provider.provider
}

// StreamMiddleware records bounded stream and message lifecycle metrics.
func (provider *Provider) StreamMiddleware(
	direction Direction,
) middleware.StreamMiddleware {
	return func(next middleware.StreamHandler) middleware.StreamHandler {
		var active atomic.Bool
		var started time.Time
		return func(
			ctx context.Context,
			event middleware.StreamEvent,
		) error {
			target, ok := operation.FromContext(ctx)
			if !ok {
				return next(ctx, event)
			}
			attributes := operationAttributes(ctx, target, direction)
			options := otelmetric.WithAttributes(attributes...)
			switch event.Phase {
			case middleware.StreamPhaseCreate:
				start := time.Now()
				err := next(ctx, event)
				if err != nil {
					provider.recordStreamCompletion(
						ctx,
						direction,
						attributes,
						start,
						err,
					)
					return err
				}
				started = start
				active.Store(true)
				provider.streamsActive.Add(ctx, 1, options)
				return nil

			case middleware.StreamPhaseSend,
				middleware.StreamPhaseReceive:
				messageAttributes := append(
					append([]attribute.KeyValue(nil), attributes...),
					attribute.String(
						"stream.phase",
						string(event.Phase),
					),
				)
				start := time.Now()
				err := next(ctx, event)
				result := completion.Classify(err)
				messageAttributes = appendCompletionAttributes(
					messageAttributes,
					result,
				)
				messageOptions := otelmetric.WithAttributes(
					messageAttributes...,
				)
				provider.streamMessageDuration.Record(
					ctx,
					time.Since(start).Seconds(),
					messageOptions,
				)
				switch {
				case err == nil:
					provider.streamMessages.Add(
						ctx,
						1,
						messageOptions,
					)
				case !errors.Is(err, io.EOF) && result.CountsAsError(
					completionDirection(direction),
				):
					provider.streamMessageErrors.Add(
						ctx,
						1,
						messageOptions,
					)
				}
				return err

			case middleware.StreamPhaseFinish:
				err := next(ctx, event)
				if active.CompareAndSwap(true, false) {
					provider.streamsActive.Add(ctx, -1, options)
					provider.recordStreamCompletion(
						ctx,
						direction,
						attributes,
						started,
						errors.Join(event.Error, err),
					)
				}
				return err

			default:
				return next(ctx, event)
			}
		}
	}
}

func (provider *Provider) recordStreamCompletion(
	ctx context.Context,
	direction Direction,
	attributes []attribute.KeyValue,
	started time.Time,
	err error,
) {
	result := completion.Classify(err)
	completionAttributes := appendCompletionAttributes(attributes, result)
	options := otelmetric.WithAttributes(completionAttributes...)
	provider.streamsCompleted.Add(ctx, 1, options)
	provider.streamDuration.Record(
		ctx,
		time.Since(started).Seconds(),
		options,
	)
	if !result.CountsAsError(completionDirection(direction)) {
		return
	}
	provider.streamErrors.Add(
		ctx,
		1,
		options,
	)
}

// Middleware records request, error, duration, and inflight instruments.
func (provider *Provider) Middleware(
	direction Direction,
) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			target, ok := operation.FromContext(ctx)
			if !ok {
				return next(ctx, request)
			}
			attributes := operationAttributes(ctx, target, direction)
			baseOptions := otelmetric.WithAttributes(attributes...)
			provider.inflight.Add(ctx, 1, baseOptions)
			start := time.Now()
			defer func() {
				provider.inflight.Add(ctx, -1, baseOptions)
				if recovered := recover(); recovered != nil {
					panicAttributes := append(
						append([]attribute.KeyValue(nil), attributes...),
						attribute.String("keelith.outcome", "error"),
						attribute.String("failure.kind", "panic"),
					)
					panicOptions := otelmetric.WithAttributes(panicAttributes...)
					provider.requests.Add(ctx, 1, panicOptions)
					provider.duration.Record(
						ctx,
						time.Since(start).Seconds(),
						panicOptions,
					)
					provider.errors.Add(
						ctx,
						1,
						panicOptions,
					)
					panic(recovered)
				}
			}()
			response, err := next(ctx, request)
			result := completion.Classify(err)
			completionAttributes := appendCompletionAttributes(attributes, result)
			completionOptions := otelmetric.WithAttributes(completionAttributes...)
			provider.requests.Add(ctx, 1, completionOptions)
			provider.duration.Record(
				ctx,
				time.Since(start).Seconds(),
				completionOptions,
			)
			if result.CountsAsError(completionDirection(direction)) {
				provider.errors.Add(
					ctx,
					1,
					completionOptions,
				)
			}
			return response, err
		}
	}
}

func appendCompletionAttributes(
	attributes []attribute.KeyValue,
	result completion.Result,
) []attribute.KeyValue {
	completed := append(
		append([]attribute.KeyValue(nil), attributes...),
		attribute.String("keelith.outcome", string(result.Outcome())),
	)
	if result.FailureKind() != failure.None {
		completed = append(
			completed,
			attribute.String("failure.kind", string(result.FailureKind())),
		)
	}
	return completed
}

func completionDirection(direction Direction) completion.Direction {
	if direction == DirectionClient {
		return completion.DirectionClient
	}
	return completion.DirectionServer
}

// ForceFlush exports pending metrics.
func (provider *Provider) ForceFlush(ctx context.Context) error {
	if provider == nil || provider.provider == nil {
		return nil
	}
	return provider.provider.ForceFlush(ctx)
}

// Shutdown releases readers and exporters.
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
) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		attribute.String("operation.transport", target.Transport()),
		attribute.String("operation.service", target.Service()),
		attribute.String("operation.method", target.Method()),
		attribute.String("operation.kind", string(target.Kind())),
		attribute.String("keelith.direction", string(direction)),
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
