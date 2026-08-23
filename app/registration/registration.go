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
	Name            string
	Registrar       registry.Registrar
	Instance        registry.Instance
	PropagationWait time.Duration
	Clock           drain.Clock
}

// Description is a bounded diagnostic snapshot.
type Description struct {
	Name      string
	State     State
	StartedAt time.Time
	StoppedAt time.Time
	LastError string
	Drain     drain.Description
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

	metadata := instanceMetadata(identity)
	seen := make(map[string]struct{}, len(services))
	instances := make([]registry.Instance, 0, len(services))

	for _, serviceName := range services {
		if _, duplicate := seen[serviceName]; duplicate {
			return nil, fmt.Errorf("%w: duplicate service %q", ErrInvalidOption, serviceName)
		}
		seen[serviceName] = struct{}{}

		instance, err := registry.NewInstance(instanceID(identity.ID(), serviceName), serviceName, endpoints, metadata)
		if err != nil {
			return nil, fmt.Errorf("%w: service %q: %w", ErrInvalidOption, serviceName, err)
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

// Manager implements the Server lifecycle contract. Place it after business
// listeners in app.WithServers so registration starts last and stops first.
type Manager struct {
	name      string
	registrar registry.Registrar
	instance  registry.Instance
	drain     *drain.Manager

	mu        sync.Mutex
	state     State
	startedAt time.Time
	stoppedAt time.Time
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
func (m *Manager) Name() string {
	if m == nil {
		return ""
	}
	return m.name
}

// Start registers the instance after earlier App servers have started.
func (m *Manager) Start(ctx context.Context) error {
	if m == nil || ctx == nil {
		return fmt.Errorf("%w: manager or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	m.mu.Lock()
	if m.state != StateNew {
		m.mu.Unlock()
		return ErrAlreadyStarted
	}
	m.state = StateRegistering
	m.startedAt = time.Now()
	m.mu.Unlock()

	err := m.registrar.Register(ctx, m.instance)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.state = StateFailed
		m.err = fmt.Errorf("registration: register: %w", err)
		return m.err
	}
	m.state = StateRegistered
	return nil
}

// Stop deregisters once, waits for discovery propagation, and is safe for
// repeated or concurrent calls.
func (m *Manager) Stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}

	m.stopOnce.Do(func() {
		m.mu.Lock()
		switch m.state {
		case StateNew:
			m.state = StateStopped
			m.stoppedAt = time.Now()
			m.mu.Unlock()
			close(m.stopDone)
		case StateRegistered:
			m.state = StateDraining
			m.mu.Unlock()
			go m.runStop(ctx)
		default:
			m.mu.Unlock()
			close(m.stopDone)
		}
	})

	select {
	case <-m.stopDone:
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Describe returns state without instance endpoints or metadata.
func (m *Manager) Describe() Description {
	if m == nil {
		return Description{State: StateFailed, LastError: "manager is nil"}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	description := Description{
		Name:      m.name,
		State:     m.state,
		StartedAt: m.startedAt,
		StoppedAt: m.stoppedAt,
		Drain:     m.drain.Describe(),
	}
	if m.err != nil {
		description.LastError = m.err.Error()
	}
	return description
}

func (m *Manager) runStop(ctx context.Context) {
	err := m.drain.Drain(ctx)

	m.mu.Lock()
	m.err = err
	m.stoppedAt = time.Now()
	if err == nil {
		m.state = StateStopped
	} else {
		m.state = StateFailed
	}
	m.mu.Unlock()

	close(m.stopDone)
}

func instanceMetadata(identity service.Identity) map[string]string {
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
	return metadata
}

func instanceID(identityID string, serviceName string) string {
	sum := sha256.Sum256([]byte(serviceName))
	suffix := hex.EncodeToString(sum[:6])

	id := identityID + "-" + suffix
	if len(id) <= 256 {
		return id
	}

	identitySum := sha256.Sum256([]byte(identityID))
	return hex.EncodeToString(identitySum[:12]) + "-" + suffix
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
