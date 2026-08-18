// Package registration coordinates service registration after listeners start
// and deregistration before listeners drain.
package registration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/app/drain"
	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/service"
)

var (
	// ErrInvalidOption reports an incomplete manager configuration.
	ErrInvalidOption = errors.New("registration: invalid option")
	// ErrAlreadyStarted reports a repeated Start call.
	ErrAlreadyStarted = errors.New("registration: already started")
)

// State is the observable registration lifecycle.
type State string

const (
	// StateNew means no backend mutation has occurred.
	StateNew State = "new"
	// StateRegistering means Register is in progress.
	StateRegistering State = "registering"
	// StateRegistered means the instance is discoverable.
	StateRegistered State = "registered"
	// StateDraining means deregistration or propagation wait is in progress.
	StateDraining State = "draining"
	// StateStopped means the instance is no longer registered.
	StateStopped State = "stopped"
	// StateFailed means Register or Drain failed.
	StateFailed State = "failed"
)

// Config defines one registration ownership boundary.
type Config struct {
	Name            string             // the service name
	Registrar       registry.Registrar // the registry to use
	Instance        registry.Instance  // the instance to register
	PropagationWait time.Duration      // the duration to wait before deregistration
	Clock           drain.Clock        // the clock to use for time-based operations
}

// Description is a bounded diagnostic snapshot.
type Description struct {
	Name      string            // the service name
	State     State             // the current state of the registration
	StartedAt time.Time         // the time the registration was started
	StoppedAt time.Time         // the time the registration was stopped
	LastError string            // the last error encountered during the registration
	Drain     drain.Description // the drain manager's diagnostic snapshot
}

// BuildInstances creates one registry Instance per exposed service contract.
//
// A profile may host more than one Proto service. Each registration therefore
// receives a stable service-derived suffix while retaining the shared
// application identity in metadata.
func BuildInstances(identity service.Identity, services []string, endpoints []string) ([]registry.Instance, error) {
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("%w: identity: %w", ErrInvalidOption, err)
	}
	if len(services) == 0 || len(services) > 256 || len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: services or endpoints are empty or oversized", ErrInvalidOption)
	}
	metadata := identity.Metadata()
	metadata["service.instance.id"] = identity.ID()
	if identity.Version() != "" {
		metadata["service.version"] = identity.Version()
	}
	if identity.Environment() != "" {
		metadata["deployment.environment.name"] = identity.Environment()
	}
	if identity.Region() != "" {
		metadata["cloud.region"] = identity.Region()
	}
	if identity.Zone() != "" {
		metadata["cloud.availability_zone"] = identity.Zone()
	}

	seen := make(map[string]struct{}, len(services))
	result := make([]registry.Instance, 0, len(services))

	for _, serviceName := range services {
		if _, duplicate := seen[serviceName]; duplicate {
			return nil, fmt.Errorf("%w: duplicate service %q", ErrInvalidOption, serviceName)
		}
		seen[serviceName] = struct{}{}
		sum := sha256.Sum256([]byte(serviceName))
		suffix := hex.EncodeToString(sum[:6])
		id := identity.ID() + "-" + suffix
		if len(id) > 256 {
			identitySum := sha256.Sum256([]byte(identity.ID()))
			id = hex.EncodeToString(identitySum[:12]) + "-" + suffix
		}
		instance, err := registry.NewInstance(id, serviceName, endpoints, metadata)
		if err != nil {
			return nil, fmt.Errorf("%w: service %q: %w", ErrInvalidOption, serviceName, err)
		}
		result = append(result, instance)
	}
	return result, nil
}

// Manager implements the Server lifecycle contract. Place it after business
// listeners in app.WithServers so registration starts last and stops first.
type Manager struct {
	name      string
	registrar registry.Registrar // the registrar to use for registration
	instance  registry.Instance  // the instance to register
	drain     *drain.Manager     // the drain manager to use for draining

	mu        sync.Mutex
	state     State     // the current state of the registration manager
	startedAt time.Time // the time the registration manager was started
	stoppedAt time.Time // the time the registration manager was stopped
	err       error
	stopDone  chan struct{}
	stopOnce  sync.Once
}

// New validates and constructs a Manager without touching the backend.
func New(config Config) (*Manager, error) {
	name := strings.TrimSpace(config.Name)
	if name == "" || strings.ContainsAny(name, "\r\n\x00") {
		return nil, fmt.Errorf("%w: name is malformed", ErrInvalidOption)
	}
	if isNil(config.Registrar) {
		return nil, fmt.Errorf("%w: registrar is nil", ErrInvalidOption)
	}
	if err := config.Instance.Validate(); err != nil {
		return nil, fmt.Errorf("%w: instance: %w", ErrInvalidOption, err)
	}
	drainManager, err := drain.New(drain.Config{
		Registrar:       config.Registrar,
		Instance:        config.Instance,
		PropagationWait: config.PropagationWait,
		Clock:           config.Clock,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: drain: %w", ErrInvalidOption, err)
	}
	return &Manager{
		name:      name,
		registrar: config.Registrar,
		instance:  config.Instance.Clone(),
		drain:     drainManager,
		state:     StateNew,
		stopDone:  make(chan struct{}),
	}, nil
}

// Name returns the stable App server name.
func (manager *Manager) Name() string {
	if manager == nil {
		return ""
	}
	return manager.name
}

// Start registers the instance after earlier App servers have started.
func (manager *Manager) Start(ctx context.Context) error {
	if manager == nil || ctx == nil {
		return fmt.Errorf("%w: manager or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	manager.mu.Lock()
	if manager.state != StateNew {
		manager.mu.Unlock()
		return ErrAlreadyStarted
	}
	manager.state = StateRegistering
	manager.startedAt = time.Now()
	manager.mu.Unlock()

	err := manager.registrar.Register(ctx, manager.instance)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if err != nil {
		manager.state = StateFailed
		manager.err = fmt.Errorf("registration: register: %w", err)
		return manager.err
	}
	manager.state = StateRegistered
	return nil
}

// Stop deregisters once, waits for discovery propagation, and is safe for
// repeated or concurrent calls.
func (manager *Manager) Stop(ctx context.Context) error {
	if manager == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	manager.stopOnce.Do(func() {
		manager.mu.Lock()
		state := manager.state
		switch state {
		case StateNew:
			manager.state = StateStopped
			manager.stoppedAt = time.Now()
			manager.mu.Unlock()
			close(manager.stopDone)
			return
		case StateRegistered:
			manager.state = StateDraining
			manager.mu.Unlock()
			go manager.runStop(ctx)
		default:
			manager.mu.Unlock()
			close(manager.stopDone)
		}
	})
	select {
	case <-manager.stopDone:
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Describe returns state without instance endpoints or metadata.
func (manager *Manager) Describe() Description {
	if manager == nil {
		return Description{
			State:     StateFailed,
			LastError: "manager is nil",
		}
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	description := Description{
		Name:      manager.name,
		State:     manager.state,
		StartedAt: manager.startedAt,
		StoppedAt: manager.stoppedAt,
		Drain:     manager.drain.Describe(),
	}
	if manager.err != nil {
		description.LastError = manager.err.Error()
	}
	return description
}

func (manager *Manager) runStop(ctx context.Context) {
	err := manager.drain.Drain(ctx)
	manager.mu.Lock()
	manager.err = err
	manager.stoppedAt = time.Now()
	if err == nil {
		manager.state = StateStopped
	} else {
		manager.state = StateFailed
	}
	manager.mu.Unlock()
	close(manager.stopDone)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
