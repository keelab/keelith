package keelith

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	kapp "github.com/keelab/keelith/app"
	"github.com/keelab/keelith/config"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/observability"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/service"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	khttp "github.com/keelab/keelith/transport/http"
)

// Application is a high-level lifecycle wrapper around app.App. It owns the
// defaults created by New and exposes the lower-level App for advanced code.
type Application struct {
	app         *kapp.App
	name        string
	profile     *service.Profile
	health      *health.Registry
	telemetry   *observability.Bundle
	graph       Graph
	config      *config.Manager
	configRun   *config.Runtime
	http        *khttp.Server
	grpc        *kgrpc.Server
	ops         *ops.Server
	routes      []RouteDescription
	servers     []string
	components  []string
	configDesc  *ConfigDescription
	httpAddress string
	grpcAddress string
	stopTimeout time.Duration

	closeMu    sync.Mutex
	closed     bool
	closeErr   error
	runClaim   bool
	runStart   *startGate
	graphClose *graphCloser
	cleanup    *cleanupCloser
}

// startGate closes when the underlying App has entered its startup sequence.
// Facade Stop waits for this boundary before calling App.Stop so a concurrent
// Close cannot observe StateNew and race with the first App.Run call.
type startGate struct {
	done chan struct{}
	once sync.Once
}

func newStartGate() *startGate {
	return &startGate{done: make(chan struct{})}
}

func (gate *startGate) signal(context.Context) error {
	if gate == nil {
		return nil
	}
	gate.once.Do(func() { close(gate.done) })
	return nil
}

func (gate *startGate) Signal() {
	if gate == nil {
		return
	}
	gate.once.Do(func() { close(gate.done) })
}

func (gate *startGate) wait(ctx context.Context) error {
	if gate == nil {
		return nil
	}
	select {
	case <-gate.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// graphCloser provides exactly-once cleanup for a construction graph. The
// same closer is used by the App hook and the facade fallback, so advanced
// callers using Application.App() cannot bypass graph cleanup.
type graphCloser struct {
	graph Graph
	once  sync.Once
	mu    sync.Mutex
	err   error
}

type cleanupCloser struct {
	cleanups []func(context.Context) error
	once     sync.Once
	mu       sync.Mutex
	err      error
}

func newCleanupCloser(cleanups []func(context.Context) error) *cleanupCloser {
	if len(cleanups) == 0 {
		return nil
	}
	return &cleanupCloser{cleanups: append([]func(context.Context) error(nil), cleanups...)}
}

func (closer *cleanupCloser) Close(ctx context.Context) error {
	if closer == nil {
		return nil
	}
	closer.once.Do(func() {
		var failures []error
		for index := len(closer.cleanups) - 1; index >= 0; index-- {
			if err := closer.cleanups[index](ctx); err != nil {
				failures = append(failures, fmt.Errorf("cleanup %d: %w", index, err))
			}
		}
		closer.mu.Lock()
		closer.err = errors.Join(failures...)
		closer.mu.Unlock()
	})
	closer.mu.Lock()
	defer closer.mu.Unlock()
	return closer.err
}

func newGraphCloser(graph Graph) *graphCloser {
	if graph == nil {
		return nil
	}
	return &graphCloser{graph: graph}
}

func (closer *graphCloser) Close(ctx context.Context) error {
	if closer == nil || closer.graph == nil {
		return nil
	}
	closer.once.Do(func() {
		err := closer.graph.Close(ctx)
		closer.mu.Lock()
		closer.err = err
		closer.mu.Unlock()
	})
	closer.mu.Lock()
	defer closer.mu.Unlock()
	return closer.err
}
