package grpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/keelab/keelith/metadata"
	"go.opentelemetry.io/otel/propagation"
	gmetadata "google.golang.org/grpc/metadata"
)

func inboundContext(
	ctx context.Context,
	policy metadata.Policy,
	propagator propagation.TextMapPropagator,
) (context.Context, error) {
	carrier := grpcMetadataCarrier{}
	if incoming, ok := gmetadata.FromIncomingContext(ctx); ok {
		carrier = grpcMetadataCarrier(incoming.Copy())
	}
	if propagator != nil {
		ctx = propagator.Extract(
			ctx,
			grpcPropagationCarrier(gmetadata.MD(carrier)),
		)
	}
	extracted, err := policy.Extract(carrier)
	if err != nil {
		return nil, fmt.Errorf("grpc transport: extract metadata: %w", err)
	}
	return metadata.WithInbound(ctx, extracted), nil
}

func outboundContext(
	ctx context.Context,
	policy metadata.Policy,
	propagator propagation.TextMapPropagator,
) (context.Context, error) {
	carrier := grpcMetadataCarrier{}
	if outgoing, ok := gmetadata.FromOutgoingContext(ctx); ok {
		carrier = grpcMetadataCarrier(outgoing.Copy())
	}
	if outbound, ok := metadata.Outbound(ctx); ok {
		if err := policy.Inject(outbound, carrier); err != nil {
			return nil, fmt.Errorf("grpc transport: inject metadata: %w", err)
		}
	}
	if propagator != nil {
		propagator.Inject(
			ctx,
			grpcPropagationCarrier(gmetadata.MD(carrier)),
		)
	}
	return gmetadata.NewOutgoingContext(ctx, gmetadata.MD(carrier)), nil
}

func filterMetadata(
	source gmetadata.MD,
	policy metadata.Policy,
) (gmetadata.MD, error) {
	extracted, err := policy.Extract(grpcMetadataCarrier(source.Copy()))
	if err != nil {
		return nil, err
	}
	result := make(gmetadata.MD)
	if err := policy.Inject(extracted, grpcMetadataCarrier(result)); err != nil {
		return nil, err
	}
	return result, nil
}

type grpcMetadataCarrier gmetadata.MD

func (carrier grpcMetadataCarrier) Values(key string) []string {
	return append([]string(nil), gmetadata.MD(carrier).Get(key)...)
}

func (carrier grpcMetadataCarrier) Set(key string, values []string) {
	gmetadata.MD(carrier).Set(key, append([]string(nil), values...)...)
}

type grpcPropagationCarrier gmetadata.MD

func (carrier grpcPropagationCarrier) Get(key string) string {
	values := gmetadata.MD(carrier).Get(strings.ToLower(key))
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (carrier grpcPropagationCarrier) Set(key string, value string) {
	gmetadata.MD(carrier).Set(strings.ToLower(key), value)
}

func (carrier grpcPropagationCarrier) Keys() []string {
	keys := make([]string, 0, len(carrier))
	for key := range carrier {
		keys = append(keys, key)
	}
	return keys
}
