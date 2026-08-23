package service

import (
	"fmt"
	"strings"

	"github.com/keelab/keelith/middleware"
	transporthttp "github.com/keelab/keelith/transport/http"
	"google.golang.org/grpc"
)

const (
	maxGroupsPerProfile   = 64
	maxServicesPerProfile = 1024
	maxProfileNameBytes   = 256
)

// Profile is an immutable, auditable set of generated service bindings that
// share one deployment/listener surface.
type Profile struct {
	name        string
	bindings    []Binding
	description Description
	httpBundle  *middleware.Bundle
	grpcBundle  *middleware.Bundle
}

// ProfileEntry is a sealed immutable Binding or Group accepted by NewProfile.
type ProfileEntry interface {
	appendProfile(*profileEntries) error
}

type profileEntries struct {
	bindings          []Binding
	groups            map[string]struct{}
	groupDescriptions []GroupDescription
}

// NewProfile validates and flattens Bindings and Groups in declaration order.
func NewProfile(name string, entryList ...ProfileEntry) (*Profile, error) {
	entries := profileEntries{groups: make(map[string]struct{})}
	for index, entry := range entryList {
		if entry == nil {
			return nil, fmt.Errorf("%w: profile entry %d is nil", ErrInvalidBinding, index)
		}
		if err := entry.appendProfile(&entries); err != nil {
			return nil, fmt.Errorf("profile %q entry %d: %w", name, index, err)
		}
	}
	if len(entries.groupDescriptions) > maxGroupsPerProfile {
		return nil, fmt.Errorf("%w: profile %q group count exceeds %d", ErrInvalidGroup, name, maxGroupsPerProfile)
	}
	return newProfile(name, entries.bindings, entries.groupDescriptions)
}

// NewProfileFromBindings preserves the convenient []Binding variadic form.
func NewProfileFromBindings(name string, bindings ...Binding) (*Profile, error) {
	return newProfile(name, bindings, nil)
}

func newProfile(name string, bindings []Binding, groups []GroupDescription) (*Profile, error) {
	normalized := strings.TrimSpace(name)
	if normalized == "" || normalized != name || len(name) > maxProfileNameBytes {
		return nil, fmt.Errorf("%w: profile name is empty or not normalized", ErrInvalidBinding)
	}
	if len(bindings) == 0 {
		return nil, fmt.Errorf("%w: profile %q has no bindings", ErrInvalidBinding, name)
	}
	if len(bindings) > maxServicesPerProfile {
		return nil, fmt.Errorf("%w: profile %q service count exceeds %d", ErrInvalidBinding, name, maxServicesPerProfile)
	}
	snapshot := make([]Binding, len(bindings))
	copy(snapshot, bindings)
	names := make(map[string]struct{}, len(snapshot))
	httpBundles := make([]*middleware.Bundle, 0, len(snapshot))
	grpcBundles := make([]*middleware.Bundle, 0, len(snapshot))
	services := make([]ServiceDescription, 0, len(snapshot))
	for index, binding := range snapshot {
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("profile %q binding %d: %w", name, index, err)
		}
		if _, duplicate := names[binding.name]; duplicate {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateBinding, binding.name)
		}
		names[binding.name] = struct{}{}
		httpBundles = append(httpBundles, binding.httpBundle)
		grpcBundles = append(grpcBundles, binding.grpcBundle)
		services = append(services, ServiceDescription{
			Name:           binding.name,
			Group:          binding.group,
			HTTP:           binding.registerHTTP != nil,
			GRPC:           binding.registerGRPC != nil,
			HTTPMiddleware: describeBundle(binding.httpBundle),
			GRPCMiddleware: describeBundle(binding.grpcBundle),
		})
	}
	httpBundle, err := middleware.CombineBundles(httpBundles...)
	if err != nil {
		return nil, fmt.Errorf("profile %q http middleware: %w", name, err)
	}
	grpcBundle, err := middleware.CombineBundles(grpcBundles...)
	if err != nil {
		return nil, fmt.Errorf("profile %q grpc middleware: %w", name, err)
	}
	return &Profile{
		name:        normalized,
		bindings:    snapshot,
		description: cloneDescription(Description{Name: normalized, Groups: groups, Services: services}),
		httpBundle:  httpBundle,
		grpcBundle:  grpcBundle,
	}, nil
}

// GroupNames returns declared Group names in declaration order.
func (profile *Profile) GroupNames() []string {
	if profile == nil {
		return nil
	}
	names := make([]string, len(profile.description.Groups))
	for index, group := range profile.description.Groups {
		names[index] = group.Name
	}
	return names
}

// Describe returns a defensive static service-topology snapshot.
func (profile *Profile) Describe() Description {
	if profile == nil {
		return Description{}
	}
	return cloneDescription(profile.description)
}

// Name returns the stable deployment profile name.
func (profile *Profile) Name() string {
	if profile == nil {
		return ""
	}
	return profile.name
}

// ServiceNames returns service names in declaration order.
func (profile *Profile) ServiceNames() []string {
	if profile == nil {
		return nil
	}
	names := make([]string, len(profile.bindings))
	for index, binding := range profile.bindings {
		names[index] = binding.name
	}
	return names
}

// HTTPBundle returns service-scoped HTTP middleware in Binding declaration order.
func (profile *Profile) HTTPBundle() *middleware.Bundle {
	if profile == nil {
		return nil
	}
	return profile.httpBundle
}

// GRPCBundle returns service-scoped gRPC middleware in Binding declaration order.
func (profile *Profile) GRPCBundle() *middleware.Bundle {
	if profile == nil {
		return nil
	}
	return profile.grpcBundle
}

// RegisterHTTP registers every HTTP-capable binding in declaration order.
func (profile *Profile) RegisterHTTP(router *transporthttp.Router) error {
	if profile == nil || router == nil {
		return fmt.Errorf("%w: profile or http router is nil", ErrInvalidBinding)
	}
	for _, binding := range profile.bindings {
		if binding.registerHTTP == nil {
			continue
		}
		if err := binding.registerHTTP(router); err != nil {
			return fmt.Errorf("register http service %q: %w", binding.name, err)
		}
	}
	return nil
}

// RegisterGRPC registers every gRPC-capable binding in declaration order.
func (profile *Profile) RegisterGRPC(registrar grpc.ServiceRegistrar) error {
	if profile == nil || registrar == nil {
		return fmt.Errorf("%w: profile or grpc registrar is nil", ErrInvalidBinding)
	}
	for _, binding := range profile.bindings {
		if binding.registerGRPC == nil {
			continue
		}
		if err := binding.registerGRPC(registrar); err != nil {
			return fmt.Errorf("register grpc service %q: %w", binding.name, err)
		}
	}
	return nil
}
