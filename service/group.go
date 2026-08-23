package service

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/keelab/keelith/middleware"
)

// Group is an immutable collection of generated service Bindings that inherit
// the same HTTP and gRPC middleware chains.
type Group struct {
	name             string
	bindings         []Binding
	httpBundles      []*middleware.Bundle
	grpcBundles      []*middleware.Bundle
	httpCapabilities []Capability
	grpcCapabilities []Capability
	requiredHTTP     []Capability
	requiredGRPC     []Capability
	err              error
}

// NewGroup creates an empty immutable service Group.
func NewGroup(name string) Group {
	return Group{name: name}
}

// Name returns the stable Group name.
func (group Group) Name() string { return group.name }

// UseHTTP returns a new Group with reusable HTTP middleware appended.
func (group Group) UseHTTP(bundles ...*middleware.Bundle) Group {
	return group.withBundles("HTTP", bundles, true)
}

// UseGRPC returns a new Group with reusable gRPC middleware appended.
func (group Group) UseGRPC(bundles ...*middleware.Bundle) Group {
	return group.withBundles("gRPC", bundles, false)
}

// UseHTTPPolicies appends executable HTTP policies and their auditable
// capabilities. Capability names never enable behavior by themselves.
func (group Group) UseHTTPPolicies(policies ...Policy) Group {
	return group.withPolicies("HTTP", policies, true)
}

// UseGRPCPolicies appends executable gRPC policies and their auditable
// capabilities. Capability names never enable behavior by themselves.
func (group Group) UseGRPCPolicies(policies ...Policy) Group {
	return group.withPolicies("gRPC", policies, false)
}

// RequireHTTP declares capabilities that the final HTTP group policy chain
// must provide at startup.
func (group Group) RequireHTTP(capabilities ...Capability) Group {
	result := group.clone()
	normalized, err := normalizeCapabilities(capabilities)
	if err != nil {
		result.err = errors.Join(result.err, fmt.Errorf("required HTTP capabilities: %w", err))
		return result
	}
	result.requiredHTTP = append(result.requiredHTTP, normalized...)
	return result
}

// RequireGRPC declares capabilities that the final gRPC group policy chain
// must provide at startup.
func (group Group) RequireGRPC(capabilities ...Capability) Group {
	result := group.clone()
	normalized, err := normalizeCapabilities(capabilities)
	if err != nil {
		result.err = errors.Join(result.err, fmt.Errorf("required gRPC capabilities: %w", err))
		return result
	}
	result.requiredGRPC = append(result.requiredGRPC, normalized...)
	return result
}

func (group Group) withPolicies(transport string, policies []Policy, http bool) Group {
	result := group.clone()
	for index, policy := range policies {
		if policy.err != nil {
			result.err = errors.Join(result.err, fmt.Errorf("%s policy %d: %w", transport, index, policy.err))
			continue
		}
		if http {
			result.httpBundles = append(result.httpBundles, policy.bundle)
			result.httpCapabilities = append(result.httpCapabilities, policy.capabilities...)
		} else {
			result.grpcBundles = append(result.grpcBundles, policy.bundle)
			result.grpcCapabilities = append(result.grpcCapabilities, policy.capabilities...)
		}
	}
	return result
}

// Bind returns a new Group with generated service Bindings appended.
func (group Group) Bind(bindings ...Binding) Group {
	result := group.clone()
	result.bindings = append(result.bindings, bindings...)
	return result
}

func (group Group) withBundles(
	transport string,
	bundles []*middleware.Bundle,
	http bool,
) Group {
	result := group.clone()
	for index, bundle := range bundles {
		if bundle == nil {
			result.err = errors.Join(
				result.err,
				fmt.Errorf("%s middleware bundle %d is nil", transport, index),
			)
		}
	}
	if http {
		result.httpBundles = append(result.httpBundles, bundles...)
	} else {
		result.grpcBundles = append(result.grpcBundles, bundles...)
	}
	return result
}

func (group Group) clone() Group {
	group.bindings = append([]Binding(nil), group.bindings...)
	group.httpBundles = append([]*middleware.Bundle(nil), group.httpBundles...)
	group.grpcBundles = append([]*middleware.Bundle(nil), group.grpcBundles...)
	group.httpCapabilities = append([]Capability(nil), group.httpCapabilities...)
	group.grpcCapabilities = append([]Capability(nil), group.grpcCapabilities...)
	group.requiredHTTP = append([]Capability(nil), group.requiredHTTP...)
	group.requiredGRPC = append([]Capability(nil), group.requiredGRPC...)
	return group
}

func (group Group) appendProfile(entries *profileEntries) error {
	if group.err != nil {
		return fmt.Errorf("%w: group %q: %w", ErrInvalidGroup, group.name, group.err)
	}
	if !validGroupName(group.name) {
		return fmt.Errorf("%w: name is empty or not normalized", ErrInvalidGroup)
	}
	if len(group.bindings) == 0 {
		return fmt.Errorf("%w: group %q has no bindings", ErrInvalidGroup, group.name)
	}
	if _, exists := entries.groups[group.name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateGroup, group.name)
	}
	normalized := group.clone()
	var err error
	normalized.requiredHTTP, err = normalizeDeclaredCapabilities(group.requiredHTTP)
	if err != nil {
		return fmt.Errorf("%w: group %q required HTTP capabilities: %w", ErrInvalidGroup, group.name, err)
	}
	normalized.requiredGRPC, err = normalizeDeclaredCapabilities(group.requiredGRPC)
	if err != nil {
		return fmt.Errorf("%w: group %q required gRPC capabilities: %w", ErrInvalidGroup, group.name, err)
	}
	normalized.httpCapabilities, err = normalizeDeclaredCapabilities(group.httpCapabilities)
	if err != nil {
		return fmt.Errorf("%w: group %q provided HTTP capabilities: %w", ErrInvalidGroup, group.name, err)
	}
	normalized.grpcCapabilities, err = normalizeDeclaredCapabilities(group.grpcCapabilities)
	if err != nil {
		return fmt.Errorf("%w: group %q provided gRPC capabilities: %w", ErrInvalidGroup, group.name, err)
	}
	entries.groups[group.name] = struct{}{}
	hasHTTP, hasGRPC := groupTransports(group.bindings)
	if hasHTTP {
		if err := validateRequiredCapabilities(normalized.requiredHTTP, normalized.httpCapabilities); err != nil {
			return fmt.Errorf("%w: group %q HTTP capabilities: %w", ErrInvalidGroup, group.name, err)
		}
	}
	if hasGRPC {
		if err := validateRequiredCapabilities(normalized.requiredGRPC, normalized.grpcCapabilities); err != nil {
			return fmt.Errorf("%w: group %q gRPC capabilities: %w", ErrInvalidGroup, group.name, err)
		}
	}

	httpBundle, err := middleware.CombineBundles(group.httpBundles...)
	if err != nil {
		return fmt.Errorf("%w: group %q HTTP middleware: %w", ErrInvalidGroup, group.name, err)
	}
	grpcBundle, err := middleware.CombineBundles(group.grpcBundles...)
	if err != nil {
		return fmt.Errorf("%w: group %q gRPC middleware: %w", ErrInvalidGroup, group.name, err)
	}
	for index, binding := range group.bindings {
		derived, err := binding.withGroup(group.name, httpBundle, grpcBundle)
		if err != nil {
			return fmt.Errorf(
				"%w: group %q binding %d: %w",
				ErrInvalidGroup,
				group.name,
				index,
				err,
			)
		}
		entries.bindings = append(entries.bindings, derived)
	}
	entries.groupDescriptions = append(entries.groupDescriptions, groupDescription(normalized))
	return nil
}

func normalizeDeclaredCapabilities(values []Capability) ([]Capability, error) {
	if len(values) == 0 {
		return nil, nil
	}
	return normalizeCapabilities(values)
}

func groupTransports(bindings []Binding) (bool, bool) {
	var http, grpc bool
	for _, binding := range bindings {
		http = http || binding.registerHTTP != nil
		grpc = grpc || binding.registerGRPC != nil
	}
	return http, grpc
}

func validateRequiredCapabilities(required, provided []Capability) error {
	providedSet := make(map[Capability]struct{}, len(provided))
	for _, capability := range provided {
		providedSet[capability] = struct{}{}
	}
	for _, capability := range required {
		if _, ok := providedSet[capability]; !ok {
			return fmt.Errorf("required capability %q is not provided", capability)
		}
	}
	return nil
}

func validGroupName(name string) bool {
	if name == "" || len(name) > maxProfileNameBytes || strings.TrimSpace(name) != name || strings.Contains(name, "/") {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (binding Binding) withGroup(
	group string,
	httpBundle *middleware.Bundle,
	grpcBundle *middleware.Bundle,
) (Binding, error) {
	result := binding
	result.group = group
	if binding.registerHTTP != nil && httpBundle != nil {
		scoped, err := middleware.ScopeToServiceWithNamespace(
			"group/"+group+"/"+binding.name,
			binding.name,
			httpBundle,
		)
		if err != nil {
			return Binding{}, fmt.Errorf("HTTP middleware: %w", err)
		}
		combined, err := middleware.CombineBundles(scoped, binding.httpBundle)
		if err != nil {
			return Binding{}, fmt.Errorf("HTTP middleware: %w", err)
		}
		result.httpBundle = combined
	}
	if binding.registerGRPC != nil && grpcBundle != nil {
		scoped, err := middleware.ScopeToServiceWithNamespace(
			"group/"+group+"/"+binding.name,
			binding.name,
			grpcBundle,
		)
		if err != nil {
			return Binding{}, fmt.Errorf("gRPC middleware: %w", err)
		}
		combined, err := middleware.CombineBundles(scoped, binding.grpcBundle)
		if err != nil {
			return Binding{}, fmt.Errorf("gRPC middleware: %w", err)
		}
		result.grpcBundle = combined
	}
	return result, nil
}
