package ops

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

const (
	maxRuntimeComponents   = 256
	maxRuntimeCounters     = 64
	maxRuntimeCapabilities = 32
	maxRuntimeTokenBytes   = 256
)

// RuntimeCounter is one low-cardinality, monotonically interpreted component
// counter. Names are stable schema, never user or resource identifiers.
type RuntimeCounter struct {
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

// RuntimeStatus is the constrained value-free snapshot returned by one
// component provider.
type RuntimeStatus struct {
	State        string           `json:"state"`
	Ready        bool             `json:"ready"`
	Degraded     bool             `json:"degraded"`
	Active       int              `json:"active"`
	Counters     []RuntimeCounter `json:"counters,omitempty"`
	Capabilities []string         `json:"capabilities,omitempty"`
}

// RuntimeComponent is one named component in the Ops runtime catalog.
type RuntimeComponent struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Available bool   `json:"available"`
	RuntimeStatus
}

// RuntimeDescription is the bounded output exposed at /debug/runtime.
type RuntimeDescription struct {
	Components []RuntimeComponent `json:"components"`
}

// RuntimeStatusProvider returns one bounded component status.
type RuntimeStatusProvider func(context.Context) (RuntimeStatus, error)

// RuntimeStatusRegistration describes one component status without coupling
// the component owner to RuntimeCatalog mutation.
type RuntimeStatusRegistration struct {
	Name     string
	Kind     string
	Provider RuntimeStatusProvider
}

// RuntimeDescriptionProvider obtains one complete runtime catalog.
type RuntimeDescriptionProvider func(context.Context) (RuntimeDescription, error)

type runtimeEntry struct {
	name     string
	kind     string
	provider RuntimeStatusProvider
}

// RuntimeCatalog collects heterogeneous component status through one
// value-free schema.
type RuntimeCatalog struct {
	mu      sync.RWMutex
	entries map[string]runtimeEntry
}

// NewRuntimeCatalog creates an empty component catalog.
func NewRuntimeCatalog() *RuntimeCatalog {
	return &RuntimeCatalog{entries: make(map[string]runtimeEntry)}
}

// Register adds one stable component identity and its status provider.
func (catalog *RuntimeCatalog) Register(
	name string,
	kind string,
	provider RuntimeStatusProvider,
) error {
	return catalog.RegisterAll(RuntimeStatusRegistration{
		Name:     name,
		Kind:     kind,
		Provider: provider,
	})
}

// RegisterAll validates and installs a component-owned registration batch
// atomically. A rejected batch leaves the catalog unchanged.
func (catalog *RuntimeCatalog) RegisterAll(
	registrations ...RuntimeStatusRegistration,
) error {
	if catalog == nil {
		return fmt.Errorf("%w: runtime catalog is nil", ErrInvalidOption)
	}
	normalized := make(
		[]RuntimeStatusRegistration,
		len(registrations),
	)
	batchKeys := make(map[string]struct{}, len(registrations))
	for index, registration := range registrations {
		registration.Name = strings.TrimSpace(registration.Name)
		registration.Kind = strings.TrimSpace(registration.Kind)
		if !validRuntimeToken(registration.Name) ||
			!validRuntimeToken(registration.Kind) {
			return fmt.Errorf(
				"%w: runtime component name or kind is invalid",
				ErrInvalidOption,
			)
		}
		if registration.Provider == nil {
			return fmt.Errorf(
				"%w: runtime status provider is nil",
				ErrInvalidOption,
			)
		}
		key := registration.Kind + "\x00" + registration.Name
		if _, duplicate := batchKeys[key]; duplicate {
			return fmt.Errorf(
				"%w: runtime component %s/%s is duplicated",
				ErrInvalidOption,
				registration.Kind,
				registration.Name,
			)
		}
		batchKeys[key] = struct{}{}
		normalized[index] = registration
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if len(catalog.entries)+len(normalized) > maxRuntimeComponents {
		return fmt.Errorf(
			"%w: runtime component count exceeds %d",
			ErrInvalidOption,
			maxRuntimeComponents,
		)
	}
	for _, registration := range normalized {
		key := registration.Kind + "\x00" + registration.Name
		if _, duplicate := catalog.entries[key]; duplicate {
			return fmt.Errorf(
				"%w: runtime component %s/%s is duplicated",
				ErrInvalidOption,
				registration.Kind,
				registration.Name,
			)
		}
	}
	for _, registration := range normalized {
		key := registration.Kind + "\x00" + registration.Name
		catalog.entries[key] = runtimeEntry{
			name:     registration.Name,
			kind:     registration.Kind,
			provider: registration.Provider,
		}
	}
	return nil
}

// Describe returns a deterministic snapshot. A failed, panicking, or invalid
// provider is represented as unavailable without exposing its error.
func (catalog *RuntimeCatalog) Describe(
	ctx context.Context,
) (RuntimeDescription, error) {
	if catalog == nil {
		return RuntimeDescription{}, fmt.Errorf(
			"ops: runtime catalog is nil",
		)
	}
	if ctx == nil {
		return RuntimeDescription{}, fmt.Errorf(
			"ops: runtime catalog context is nil",
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return RuntimeDescription{}, cause
	}
	catalog.mu.RLock()
	entries := make([]runtimeEntry, 0, len(catalog.entries))
	for _, entry := range catalog.entries {
		entries = append(entries, entry)
	}
	catalog.mu.RUnlock()
	sort.Slice(entries, func(first, second int) bool {
		if entries[first].kind == entries[second].kind {
			return entries[first].name < entries[second].name
		}
		return entries[first].kind < entries[second].kind
	})
	description := RuntimeDescription{
		Components: make([]RuntimeComponent, 0, len(entries)),
	}
	for _, entry := range entries {
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeDescription{}, cause
		}
		status, err := readRuntimeComponent(ctx, entry.provider)
		component := RuntimeComponent{
			Name:      entry.name,
			Kind:      entry.kind,
			Available: err == nil && validateRuntimeStatus(status) == nil,
		}
		if component.Available {
			component.RuntimeStatus = cloneRuntimeStatus(status)
		} else {
			component.RuntimeStatus = RuntimeStatus{State: "unavailable"}
		}
		description.Components = append(description.Components, component)
	}
	return description, nil
}

// RuntimeCatalogStatus adapts a catalog to an Ops endpoint provider.
func RuntimeCatalogStatus(
	catalog *RuntimeCatalog,
) RuntimeDescriptionProvider {
	if catalog == nil {
		return nil
	}
	return catalog.Describe
}

// WithRuntimeStatus exposes a bounded component catalog at GET /debug/runtime.
func WithRuntimeStatus(provider RuntimeDescriptionProvider) Option {
	return optionFunc(func(options *options) error {
		if provider == nil {
			return fmt.Errorf("runtime status provider is nil")
		}
		options.runtimeStatus = provider
		return nil
	})
}

func runtimeStatusHandler(provider RuntimeDescriptionProvider) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		description, err := readRuntimeDescription(request.Context(), provider)
		if err != nil || validateRuntimeDescription(description) != nil {
			writeDiagnosticError(
				writer,
				http.StatusServiceUnavailable,
				"unavailable",
			)
			return
		}
		writeDiagnosticJSON(writer, http.StatusOK, description)
	})
}

func readRuntimeDescription(
	ctx context.Context,
	provider RuntimeDescriptionProvider,
) (description RuntimeDescription, err error) {
	defer func() {
		if recover() != nil {
			description = RuntimeDescription{}
			err = fmt.Errorf("ops: runtime status provider panic")
		}
	}()
	return provider(ctx)
}

func readRuntimeComponent(
	ctx context.Context,
	provider RuntimeStatusProvider,
) (status RuntimeStatus, err error) {
	defer func() {
		if recover() != nil {
			status = RuntimeStatus{}
			err = fmt.Errorf("ops: runtime component provider panic")
		}
	}()
	return provider(ctx)
}

func validateRuntimeDescription(description RuntimeDescription) error {
	if len(description.Components) > maxRuntimeComponents {
		return fmt.Errorf("runtime component count exceeds %d", maxRuntimeComponents)
	}
	seen := make(map[string]struct{}, len(description.Components))
	previous := ""
	for _, component := range description.Components {
		if !validRuntimeToken(component.Name) ||
			!validRuntimeToken(component.Kind) {
			return fmt.Errorf("runtime component identity is invalid")
		}
		key := component.Kind + "\x00" + component.Name
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("runtime component is duplicated")
		}
		seen[key] = struct{}{}
		if previous != "" && key < previous {
			return fmt.Errorf("runtime components are not sorted")
		}
		previous = key
		if component.Available {
			if err := validateRuntimeStatus(component.RuntimeStatus); err != nil {
				return err
			}
		} else if component.State != "unavailable" ||
			component.Ready ||
			component.Degraded ||
			component.Active != 0 ||
			len(component.Counters) != 0 ||
			len(component.Capabilities) != 0 {
			return fmt.Errorf("unavailable runtime component has status data")
		}
	}
	return nil
}

func validateRuntimeStatus(status RuntimeStatus) error {
	if !validRuntimeToken(status.State) || status.Active < 0 {
		return fmt.Errorf("runtime status state or active count is invalid")
	}
	if len(status.Counters) > maxRuntimeCounters ||
		len(status.Capabilities) > maxRuntimeCapabilities {
		return fmt.Errorf("runtime status exceeds item limits")
	}
	previous := ""
	for _, counter := range status.Counters {
		if !validRuntimeToken(counter.Name) ||
			previous != "" && counter.Name <= previous {
			return fmt.Errorf("runtime counters are invalid or not sorted")
		}
		previous = counter.Name
	}
	previous = ""
	for _, capability := range status.Capabilities {
		if !validRuntimeToken(capability) ||
			previous != "" && capability <= previous {
			return fmt.Errorf("runtime capabilities are invalid or not sorted")
		}
		previous = capability
	}
	return nil
}

func validRuntimeToken(value string) bool {
	if value == "" ||
		len(value) > maxRuntimeTokenBytes ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z',
			character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9',
			character == '.',
			character == '_',
			character == '-',
			character == '/',
			character == ':':
		default:
			return false
		}
	}
	return true
}

func cloneRuntimeStatus(status RuntimeStatus) RuntimeStatus {
	status.Counters = append([]RuntimeCounter(nil), status.Counters...)
	status.Capabilities = append([]string(nil), status.Capabilities...)
	return status
}
