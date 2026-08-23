package grpc

import (
	"context"

	"github.com/keelab/keelith/operation"
	gpeer "google.golang.org/grpc/peer"
)

func withInboundRequestInfo(
	ctx context.Context,
	target operation.Operation,
) (context.Context, error) {
	options := make([]operation.RequestInfoOption, 0, 1)
	if remote, ok := gpeer.FromContext(ctx); ok && remote.Addr != nil {
		peer, err := operation.NewPeer(
			remote.Addr.Network(),
			remote.Addr.String(),
		)
		if err != nil {
			return nil, err
		}
		options = append(options, operation.WithPeer(peer))
	}
	info, err := operation.NewRequestInfo(target, options...)
	if err != nil {
		return nil, err
	}
	return operation.WithRequestInfo(ctx, info), nil
}

func withOutboundRequestInfo(
	ctx context.Context,
	target operation.Operation,
) (context.Context, error) {
	info, err := operation.NewRequestInfo(target)
	if err != nil {
		return nil, err
	}
	return operation.WithRequestInfo(ctx, info), nil
}
