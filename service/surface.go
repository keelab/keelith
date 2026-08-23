package service

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/placement"
	transporthttp "github.com/keelab/keelith/transport/http"
	"google.golang.org/grpc"
)

// Transport identifies one listener protocol surface.
type Transport string

const (
	// TransportHTTP identifies an HTTP listener surface.
	TransportHTTP Transport = "http"
	// TransportGRPC identifies a gRPC listener surface.
	TransportGRPC Transport = "grpc"
)

// Surface binds one stable listener identity to one Profile transport.
type Surface struct {
	name      string
	transport Transport
	profile   *Profile
	context   *middleware.Bundle
	services  []string
}

// SurfaceDescription is the bounded listener-to-profile identity snapshot.
type SurfaceDescription struct {
	Name      string    `json:"name"`
	Transport Transport `json:"transport"`
	Profile   string    `json:"profile"`
	Services  []string  `json:"services"`
}

// HTTP creates an HTTP listener surface for profile.
func (profile *Profile) HTTP(name string) (*Surface, error) {
	return newSurface(name, TransportHTTP, profile)
}

// GRPC creates a gRPC listener surface for profile.
func (profile *Profile) GRPC(name string) (*Surface, error) {
	return newSurface(name, TransportGRPC, profile)
}

func newSurface(name string, transport Transport, profile *Profile) (*Surface, error) {
	if profile == nil || !validGroupName(name) {
		return nil, fmt.Errorf("%w: listener or profile is invalid", ErrInvalidBinding)
	}
	entries := make([]middleware.Entry, 0, len(profile.bindings))
	services := make([]string, 0, len(profile.bindings))
	for _, binding := range profile.bindings {
		if transport == TransportHTTP && binding.registerHTTP == nil ||
			transport == TransportGRPC && binding.registerGRPC == nil {
			continue
		}
		services = append(services, binding.name)
		value, err := placement.New(name, profile.name, binding.group, binding.name)
		if err != nil {
			return nil, fmt.Errorf("build placement for %q: %w", binding.name, err)
		}
		serviceName := binding.name
		placementValue := value
		entries = append(entries, middleware.Entry{
			Name:   "surface/" + name + "/" + serviceName + "/placement",
			Source: "profile",
			Middleware: func(next middleware.Handler) middleware.Handler {
				return func(ctx context.Context, request any) (any, error) {
					target, ok := operation.FromContext(ctx)
					if !ok || target.Service() != serviceName {
						return next(ctx, request)
					}
					return next(placement.WithContext(ctx, placementValue), request)
				}
			},
		})
	}
	if len(services) == 0 {
		return nil, fmt.Errorf(
			"%w: profile %q has no %s-capable services",
			ErrInvalidBinding,
			profile.name,
			transport,
		)
	}
	contextBundle, err := middleware.NewBundle(entries...)
	if err != nil {
		return nil, fmt.Errorf("build surface context: %w", err)
	}
	return &Surface{
		name: name, transport: transport, profile: profile,
		context: contextBundle, services: services,
	}, nil
}

// Name returns the stable listener identity.
func (surface *Surface) Name() string {
	if surface == nil {
		return ""
	}
	return surface.name
}

// Compose returns Placement, global, Group, and Binding middleware in that
// fixed execution order.
func (surface *Surface) Compose(global *middleware.Bundle) (*middleware.Bundle, error) {
	if surface == nil || surface.profile == nil {
		return nil, fmt.Errorf("%w: surface is nil", ErrInvalidBinding)
	}
	policy := surface.profile.grpcBundle
	if surface.transport == TransportHTTP {
		policy = surface.profile.httpBundle
	}
	return middleware.CombineBundles(surface.context, global, policy)
}

// RegisterHTTP registers an HTTP Surface on router.
func (surface *Surface) RegisterHTTP(router *transporthttp.Router) error {
	if surface == nil || surface.profile == nil || surface.transport != TransportHTTP {
		return fmt.Errorf("%w: surface is not http", ErrInvalidBinding)
	}
	return surface.profile.RegisterHTTP(router)
}

// RegisterGRPC registers a gRPC Surface on registrar.
func (surface *Surface) RegisterGRPC(registrar grpc.ServiceRegistrar) error {
	if surface == nil || surface.profile == nil || surface.transport != TransportGRPC {
		return fmt.Errorf("%w: surface is not grpc", ErrInvalidBinding)
	}
	return surface.profile.RegisterGRPC(registrar)
}

// Describe returns a defensive listener identity snapshot.
func (surface *Surface) Describe() SurfaceDescription {
	if surface == nil || surface.profile == nil {
		return SurfaceDescription{}
	}
	return SurfaceDescription{
		Name: surface.name, Transport: surface.transport, Profile: surface.profile.name,
		Services: append([]string(nil), surface.services...),
	}
}

// ProfileDescription returns the defensive Profile topology owned by surface.
func (surface *Surface) ProfileDescription() Description {
	if surface == nil || surface.profile == nil {
		return Description{}
	}
	return surface.profile.Describe()
}
