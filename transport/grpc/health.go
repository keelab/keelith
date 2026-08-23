package grpc

import (
	"context"
	"time"

	"github.com/keelab/keelith/health"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const healthWatchInterval = 20 * time.Millisecond

type healthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	registry *health.Registry
}

func (server *healthServer) Check(
	ctx context.Context,
	request *grpc_health_v1.HealthCheckRequest,
) (*grpc_health_v1.HealthCheckResponse, error) {
	if request.GetService() != "" {
		return nil, status.Error(codes.NotFound, "unknown health service")
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: server.status(ctx),
	}, nil
}

func (server *healthServer) List(
	ctx context.Context,
	_ *grpc_health_v1.HealthListRequest,
) (*grpc_health_v1.HealthListResponse, error) {
	return &grpc_health_v1.HealthListResponse{
		Statuses: map[string]*grpc_health_v1.HealthCheckResponse{
			"": {Status: server.status(ctx)},
		},
	}, nil
}

func (server *healthServer) Watch(
	request *grpc_health_v1.HealthCheckRequest,
	stream ggrpc.ServerStreamingServer[grpc_health_v1.HealthCheckResponse],
) error {
	unknownService := request.GetService() != ""
	last := grpc_health_v1.HealthCheckResponse_ServingStatus(-1)
	ticker := time.NewTicker(healthWatchInterval)
	defer ticker.Stop()
	for {
		current := grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN
		if !unknownService {
			current = server.status(stream.Context())
		}
		if current != last {
			if err := stream.Send(&grpc_health_v1.HealthCheckResponse{
				Status: current,
			}); err != nil {
				return err
			}
			last = current
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (server *healthServer) status(ctx context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
	report := server.registry.Check(ctx, health.KindReadiness)
	if report.Status == health.StatusPass {
		return grpc_health_v1.HealthCheckResponse_SERVING
	}
	return grpc_health_v1.HealthCheckResponse_NOT_SERVING
}
