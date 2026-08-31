package keelith

import (
	"context"
	"errors"
	"fmt"

	kapp "github.com/keelab/keelith/app"
	"github.com/keelab/keelith/config"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/service"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	khttp "github.com/keelab/keelith/transport/http"
)

// Run starts the application and releases construction-owned resources after
// the lifecycle reaches a terminal state.
func (application *Application) Run(ctx context.Context) error {
	if application == nil || application.app == nil {
		return fmt.Errorf("%w: application is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return kapp.ErrNilContext
	}
	application.closeMu.Lock()
	if application.closed {
		closeErr := application.closeErr
		application.closeMu.Unlock()
		if closeErr != nil {
			return errors.Join(ErrClosed, closeErr)
		}
		return ErrClosed
	}
	if application.runClaim || application.app.State() != kapp.StateNew {
		application.closeMu.Unlock()
		return kapp.ErrAlreadyRun
	}
	application.runClaim = true
	application.closeMu.Unlock()

	runErr := func() error {
		defer application.runStart.Signal()
		return application.app.Run(ctx)
	}()
	application.closeMu.Lock()
	application.runClaim = false
	application.closeMu.Unlock()
	cleanupCtx, cleanupCancel := application.cleanupContext()
	cleanupErr := application.close(cleanupCtx)
	cleanupCancel()
	return errors.Join(runErr, cleanupErr)
}

// Stop requests graceful shutdown and releases construction-owned resources.
func (application *Application) Stop(ctx context.Context) error {
	if application == nil || application.app == nil {
		return nil
	}
	if ctx == nil {
		return kapp.ErrNilContext
	}
	application.closeMu.Lock()
	runClaimed := application.runClaim
	runStart := application.runStart
	application.closeMu.Unlock()
	if runClaimed {
		if err := runStart.wait(ctx); err != nil {
			return err
		}
	}
	stopErr := application.app.Stop(ctx)
	if !isTerminalState(application.app.State()) {
		return stopErr
	}
	cleanupCtx, cleanupCancel := application.cleanupContext()
	closeErr := application.close(cleanupCtx)
	cleanupCancel()
	return errors.Join(stopErr, closeErr)
}

// Close is an idempotent convenience alias for Stop.
func (application *Application) Close(ctx context.Context) error {
	return application.Stop(ctx)
}

// App returns the underlying lifecycle object for advanced integrations.
func (application *Application) App() *kapp.App {
	if application == nil {
		return nil
	}
	return application.app
}

// Profile returns the immutable service profile created by New.
func (application *Application) Profile() *service.Profile {
	if application == nil {
		return nil
	}
	return application.profile
}

// Config returns the configured Manager, if the application enables config.
// The returned Manager is application-owned and must not be closed directly.
func (application *Application) Config() *config.Manager {
	if application == nil {
		return nil
	}
	return application.config
}

// ConfigRuntime returns the application-owned configuration watcher, if one
// is enabled.
func (application *Application) ConfigRuntime() *config.Runtime {
	if application == nil {
		return nil
	}
	return application.configRun
}

// Health returns the application health registry.
func (application *Application) Health() *health.Registry {
	if application == nil {
		return nil
	}
	return application.health
}

// HTTPServer returns the generated HTTP server when WithHTTP was used.
func (application *Application) HTTPServer() *khttp.Server {
	if application == nil {
		return nil
	}
	return application.http
}

// GRPCServer returns the generated gRPC server when WithGRPC was used.
func (application *Application) GRPCServer() *kgrpc.Server {
	if application == nil {
		return nil
	}
	return application.grpc
}

// OpsServer returns the generated operational server when WithOps was used.
func (application *Application) OpsServer() *ops.Server {
	if application == nil {
		return nil
	}
	return application.ops
}

// Describe returns a defensive, secret-free snapshot of the facade plan and
// current lifecycle state.
func (application *Application) Describe() Description {
	if application == nil || application.app == nil {
		return Description{}
	}
	description := Description{
		Name:       application.name,
		State:      application.app.State(),
		Routes:     append([]RouteDescription(nil), application.routes...),
		Servers:    append([]string(nil), application.servers...),
		Components: append([]string(nil), application.components...),
		Graph:      application.graph != nil,
	}
	if application.http != nil {
		description.HTTP = ListenerDescription{
			Enabled: true,
			Address: application.httpAddress,
		}
	}
	if application.grpc != nil {
		description.GRPC = ListenerDescription{
			Enabled: true,
			Address: application.grpcAddress,
		}
	}
	if application.ops != nil {
		description.Ops = ListenerDescription{Enabled: true}
		if address, ok := application.ops.Address(); ok {
			description.Ops.Address = address
		}
	}
	if application.profile != nil {
		description.Profile = application.profile.Describe()
	}
	if application.configDesc != nil {
		configDescription := *application.configDesc
		if application.configRun != nil {
			configDescription.Runtime = application.configRun.Description()
		}
		description.Config = &configDescription
	}
	return description
}

func (application *Application) close(ctx context.Context) error {
	application.closeMu.Lock()
	defer application.closeMu.Unlock()
	if application.closed {
		return application.closeErr
	}
	application.closed = true
	var failures []error
	if application.graphClose != nil {
		if err := application.graphClose.Close(ctx); err != nil {
			failures = append(failures, fmt.Errorf("close dependency graph: %w", err))
		}
	}
	if application.cleanup != nil {
		if err := application.cleanup.Close(ctx); err != nil {
			failures = append(failures, fmt.Errorf("close construction cleanups: %w", err))
		}
	}
	if application.telemetry != nil {
		if err := application.telemetry.Shutdown(ctx); err != nil {
			failures = append(failures, fmt.Errorf("shutdown observability: %w", err))
		}
	}
	application.closeErr = errors.Join(failures...)
	return application.closeErr
}

func (application *Application) cleanupContext() (context.Context, context.CancelFunc) {
	timeout := application.stopTimeout
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

func isTerminalState(state kapp.State) bool {
	return state == kapp.StateNew ||
		state == kapp.StateStopped ||
		state == kapp.StateFailed
}
