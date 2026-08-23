package service

import (
	"context"
	"log/slog"

	"github.com/keelab/keelith/server"
)

// Listener is the complete lifecycle contract required for an observed
// network Surface. Requiring Wait avoids changing App termination semantics.
type Listener interface {
	server.Server
	server.Waiter
	server.Named
}

// ObservedListener emits bounded lifecycle events enriched with the immutable
// Surface identity. It never logs returned error text.
type ObservedListener struct {
	listener Listener
	surface  *Surface
	logger   *slog.Logger
}

// ObserveListener validates and wraps one transport listener.
func ObserveListener(listener Listener, surface *Surface, logger *slog.Logger) (*ObservedListener, error) {
	if listener == nil || surface == nil || surface.profile == nil || logger == nil {
		return nil, ErrInvalidBinding
	}
	if listener.Name() != surface.Name() {
		return nil, ErrInvalidBinding
	}
	return &ObservedListener{listener: listener, surface: surface, logger: logger}, nil
}

// Start starts the listener and emits its static identity and topology counts.
func (listener *ObservedListener) Start(ctx context.Context) error {
	description := listener.surface.Describe()
	listener.logger.InfoContext(ctx, "service listener starting", listener.attributes()...)
	if err := listener.listener.Start(ctx); err != nil {
		listener.logger.ErrorContext(ctx, "service listener start failed",
			"listener.name", description.Name,
			"listener.transport", description.Transport,
			"profile.name", description.Profile,
			"failure.kind", "start",
		)
		return err
	}
	attributes := listener.attributes()
	if addressed, ok := listener.listener.(interface{ Address() (string, bool) }); ok {
		if address, exists := addressed.Address(); exists {
			attributes = append(attributes, "listener.address", address)
		}
	}
	listener.logger.InfoContext(ctx, "service listener started", attributes...)
	for _, group := range listener.surface.ProfileDescription().Groups {
		listener.logger.InfoContext(ctx, "service group configured",
			"listener.name", description.Name,
			"listener.transport", description.Transport,
			"profile.name", description.Profile,
			"group.name", group.Name,
			"group.services", len(group.Services),
			"group.http_middleware", len(group.HTTPMiddleware),
			"group.grpc_middleware", len(group.GRPCMiddleware),
		)
	}
	return nil
}

// Stop gracefully stops the listener and records completion or failure kind.
func (listener *ObservedListener) Stop(ctx context.Context) error {
	description := listener.surface.Describe()
	listener.logger.InfoContext(ctx, "service listener stopping", listener.attributes()...)
	err := listener.listener.Stop(ctx)
	attributes := []any{
		"listener.name", description.Name,
		"listener.transport", description.Transport,
		"profile.name", description.Profile,
	}
	if err != nil {
		attributes = append(attributes, "failure.kind", "stop")
		listener.logger.ErrorContext(ctx, "service listener stop failed", attributes...)
		return err
	}
	listener.logger.InfoContext(ctx, "service listener stopped", attributes...)
	return nil
}

// Wait reports listener termination and preserves the underlying error.
func (listener *ObservedListener) Wait() error {
	err := listener.listener.Wait()
	attributes := listener.attributes()
	if err != nil {
		attributes = append(attributes, "failure.kind", "serve")
		listener.logger.Error("service listener terminated", attributes...)
		return err
	}
	listener.logger.Info("service listener terminated", attributes...)
	return nil
}

// Name returns the validated Surface listener identity.
func (listener *ObservedListener) Name() string { return listener.surface.Name() }

// Address forwards an optional active listener address.
func (listener *ObservedListener) Address() (string, bool) {
	addressed, ok := listener.listener.(interface{ Address() (string, bool) })
	if !ok {
		return "", false
	}
	return addressed.Address()
}

func (listener *ObservedListener) attributes() []any {
	description := listener.surface.Describe()
	profile := listener.surface.ProfileDescription()
	return []any{
		"listener.name", description.Name,
		"listener.transport", description.Transport,
		"profile.name", description.Profile,
		"profile.groups", len(profile.Groups),
		"profile.services", len(description.Services),
	}
}
