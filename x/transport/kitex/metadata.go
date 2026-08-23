package kitex

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/pkg/remote"
	kitexmetadata "github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/metadata"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/transport"
	"github.com/keelab/keelith/metadata"
	"go.opentelemetry.io/otel/propagation"
)

const (
	metadataWirePrefix = "keelith-metadata-"
	traceWirePrefix    = "keelith-trace-"
	maxWireValueBytes  = 64 * 1024
)

func inboundContext(
	ctx context.Context,
	policy metadata.Policy,
	propagator propagation.TextMapPropagator,
) (context.Context, error) {
	if _, ok := metadata.Inbound(ctx); ok {
		return ctx, nil
	}
	if propagator != nil {
		ctx = propagator.Extract(
			ctx,
			&metainfoPropagationCarrier{context: ctx},
		)
	}
	carrier := &metainfoMetadataCarrier{context: ctx}
	inbound, err := policy.Extract(carrier)
	if err == nil {
		err = carrier.err
	}
	if err != nil {
		return nil, fmt.Errorf("kitex profile: extract metadata: %w", err)
	}
	return metadata.WithInbound(ctx, inbound), nil
}

type clientStreamMetaHandler struct {
	policy     metadata.Policy
	propagator propagation.TextMapPropagator
}

var (
	_ remote.MetaHandler          = (*clientStreamMetaHandler)(nil)
	_ remote.StreamingMetaHandler = (*clientStreamMetaHandler)(nil)
)

func (*clientStreamMetaHandler) WriteMeta(
	ctx context.Context,
	_ remote.Message,
) (context.Context, error) {
	return ctx, nil
}

func (*clientStreamMetaHandler) ReadMeta(
	ctx context.Context,
	_ remote.Message,
) (context.Context, error) {
	return ctx, nil
}

func (handler *clientStreamMetaHandler) OnConnectStream(
	ctx context.Context,
) (context.Context, error) {
	if !grpcStreamContext(ctx) {
		return ctx, nil
	}
	md, ok := kitexmetadata.FromOutgoingContext(ctx)
	if !ok {
		md = kitexmetadata.MD{}
	} else {
		md = md.Copy()
	}
	carrier := grpcMetadataCarrier{metadata: md}
	if outbound, exists := metadata.Outbound(ctx); exists {
		if err := handler.policy.Inject(outbound, carrier); err != nil {
			return nil, fmt.Errorf(
				"kitex profile: inject stream metadata: %w",
				err,
			)
		}
	}
	if handler.propagator != nil {
		handler.propagator.Inject(
			ctx,
			grpcPropagationCarrier{metadata: md},
		)
	}
	return kitexmetadata.NewOutgoingContext(ctx, md), nil
}

func (*clientStreamMetaHandler) OnReadStream(
	ctx context.Context,
) (context.Context, error) {
	return ctx, nil
}

type serverStreamMetaHandler struct {
	policy     metadata.Policy
	propagator propagation.TextMapPropagator
}

var (
	_ remote.MetaHandler          = (*serverStreamMetaHandler)(nil)
	_ remote.StreamingMetaHandler = (*serverStreamMetaHandler)(nil)
)

func (*serverStreamMetaHandler) WriteMeta(
	ctx context.Context,
	_ remote.Message,
) (context.Context, error) {
	return ctx, nil
}

func (*serverStreamMetaHandler) ReadMeta(
	ctx context.Context,
	_ remote.Message,
) (context.Context, error) {
	return ctx, nil
}

func (*serverStreamMetaHandler) OnConnectStream(
	ctx context.Context,
) (context.Context, error) {
	return ctx, nil
}

func (handler *serverStreamMetaHandler) OnReadStream(
	ctx context.Context,
) (context.Context, error) {
	if !grpcStreamContext(ctx) {
		return ctx, nil
	}
	md, ok := kitexmetadata.FromIncomingContext(ctx)
	if !ok {
		md = kitexmetadata.MD{}
	}
	if handler.propagator != nil {
		ctx = handler.propagator.Extract(
			ctx,
			grpcPropagationCarrier{metadata: md},
		)
	}
	inbound, err := handler.policy.Extract(
		grpcMetadataCarrier{metadata: md},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"kitex profile: extract stream metadata: %w",
			err,
		)
	}
	return metadata.WithInbound(ctx, inbound), nil
}

type grpcMetadataCarrier struct {
	metadata kitexmetadata.MD
}

func (carrier grpcMetadataCarrier) Values(key string) []string {
	return append(
		[]string(nil),
		carrier.metadata.Get(
			metadataWirePrefix+strings.ToLower(key),
		)...,
	)
}

func (carrier grpcMetadataCarrier) Set(
	key string,
	values []string,
) {
	carrier.metadata.Set(
		metadataWirePrefix+strings.ToLower(key),
		values...,
	)
}

type grpcPropagationCarrier struct {
	metadata kitexmetadata.MD
}

func (carrier grpcPropagationCarrier) Get(key string) string {
	values := carrier.metadata.Get(
		traceWirePrefix + strings.ToLower(key),
	)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (carrier grpcPropagationCarrier) Set(key, value string) {
	if value == "" {
		return
	}
	carrier.metadata.Set(
		traceWirePrefix+strings.ToLower(key),
		value,
	)
}

func (carrier grpcPropagationCarrier) Keys() []string {
	keys := make([]string, 0, len(carrier.metadata))
	for key := range carrier.metadata {
		if name, ok := strings.CutPrefix(key, traceWirePrefix); ok {
			keys = append(keys, name)
		}
	}
	sort.Strings(keys)
	return keys
}

func grpcStreamContext(ctx context.Context) bool {
	rpc := rpcinfo.GetRPCInfo(ctx)
	return rpc != nil &&
		rpc.Config() != nil &&
		rpc.Config().TransportProtocol()&transport.GRPC != 0
}

func outboundContext(
	ctx context.Context,
	policy metadata.Policy,
	propagator propagation.TextMapPropagator,
) (context.Context, error) {
	carrier := &metainfoMetadataCarrier{context: ctx, mutable: &ctx}
	if outbound, ok := metadata.Outbound(ctx); ok {
		if err := policy.Inject(outbound, carrier); err != nil {
			return nil, fmt.Errorf("kitex profile: inject metadata: %w", err)
		}
	}
	if carrier.err != nil {
		return nil, carrier.err
	}
	if propagator != nil {
		propagator.Inject(
			ctx,
			&metainfoPropagationCarrier{
				context: ctx,
				mutable: &ctx,
			},
		)
	}
	return ctx, nil
}

type metainfoMetadataCarrier struct {
	context context.Context
	mutable *context.Context
	err     error
}

func (carrier *metainfoMetadataCarrier) Values(key string) []string {
	if carrier == nil || carrier.err != nil {
		return nil
	}
	raw, ok := metainfo.GetValue(
		carrier.context,
		metadataWirePrefix+strings.ToLower(key),
	)
	if !ok {
		return nil
	}
	if len(raw) > maxWireValueBytes {
		carrier.err = metadata.ErrTooLarge
		return nil
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		carrier.err = fmt.Errorf(
			"kitex profile: metadata %q is malformed",
			key,
		)
		return nil
	}
	return values
}

func (carrier *metainfoMetadataCarrier) Set(
	key string,
	values []string,
) {
	if carrier == nil || carrier.mutable == nil || carrier.err != nil {
		return
	}
	encoded, err := json.Marshal(values)
	if err != nil || len(encoded) > maxWireValueBytes {
		carrier.err = metadata.ErrTooLarge
		return
	}
	next := metainfo.WithValue(
		*carrier.mutable,
		metadataWirePrefix+strings.ToLower(key),
		string(encoded),
	)
	*carrier.mutable = next
	carrier.context = next
}

type metainfoPropagationCarrier struct {
	context context.Context
	mutable *context.Context
}

func (carrier *metainfoPropagationCarrier) Get(key string) string {
	if carrier == nil {
		return ""
	}
	value, _ := metainfo.GetValue(
		carrier.context,
		traceWirePrefix+strings.ToLower(key),
	)
	if len(value) > maxWireValueBytes {
		return ""
	}
	return value
}

func (carrier *metainfoPropagationCarrier) Set(key, value string) {
	if carrier == nil ||
		carrier.mutable == nil ||
		value == "" ||
		len(value) > maxWireValueBytes {
		return
	}
	next := metainfo.WithValue(
		*carrier.mutable,
		traceWirePrefix+strings.ToLower(key),
		value,
	)
	*carrier.mutable = next
	carrier.context = next
}

func (carrier *metainfoPropagationCarrier) Keys() []string {
	if carrier == nil {
		return nil
	}
	values := metainfo.GetAllValues(carrier.context)
	keys := make([]string, 0, len(values))
	for key := range values {
		if name, ok := strings.CutPrefix(key, traceWirePrefix); ok {
			keys = append(keys, name)
		}
	}
	sort.Strings(keys)
	return keys
}
