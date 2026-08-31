package di

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const manifestSchema = "keelith.di/v1"

// Manifest is the portable, value-free dependency graph contract consumed by
// CLI, CI, documentation, and code generation.
type Manifest struct {
	Schema      string      `json:"schema"`
	Description Description `json:"description"`
}

// NewManifest creates a portable graph manifest.
func NewManifest(description Description) Manifest {
	return Manifest{Schema: manifestSchema, Description: description}
}

// ParseManifest strictly validates a serialized graph manifest.
func ParseManifest(document []byte) (Manifest, error) {
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("di: parse manifest: %w", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return Manifest{}, err
	}
	if manifest.Schema != manifestSchema {
		return Manifest{}, fmt.Errorf("di: unsupported manifest schema %q", manifest.Schema)
	}
	normalizeDescription(&manifest.Description)
	if err := validateDescription(manifest.Description); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// MarshalManifest serializes a stable portable graph manifest.
func MarshalManifest(description Description) ([]byte, error) {
	normalizeDescription(&description)
	if err := validateDescription(description); err != nil {
		return nil, err
	}
	return json.MarshalIndent(NewManifest(description), "", "  ")
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("di: parse manifest: multiple json values")
		}
		return fmt.Errorf("di: parse manifest: %w", err)
	}
	return nil
}

func normalizeDescription(description *Description) {
	for index := range description.Providers {
		provider := &description.Providers[index]
		if provider.State == "" {
			provider.State = "registered"
		}
		provider.Dependencies = append([]string(nil), provider.Dependencies...)
		sort.Strings(provider.Dependencies)
	}
	sort.Slice(description.Components, func(i, j int) bool {
		if description.Components[i].Name != description.Components[j].Name {
			return description.Components[i].Name < description.Components[j].Name
		}
		return description.Components[i].Kind < description.Components[j].Kind
	})
	sort.Slice(description.Roots, func(i, j int) bool {
		return description.Roots[i].Name < description.Roots[j].Name
	})
	sort.Slice(description.Services, func(i, j int) bool {
		return description.Services[i].Name < description.Services[j].Name
	})
	for index := range description.Services {
		sort.Strings(description.Services[index].Operations)
		sort.Strings(description.Services[index].Transports)
	}
	sort.Slice(description.Providers, func(i, j int) bool {
		return description.Providers[i].ID < description.Providers[j].ID
	})
	sort.Slice(description.Edges, func(i, j int) bool {
		if description.Edges[i].From != description.Edges[j].From {
			return description.Edges[i].From < description.Edges[j].From
		}
		if description.Edges[i].To != description.Edges[j].To {
			return description.Edges[i].To < description.Edges[j].To
		}
		return description.Edges[i].Type < description.Edges[j].Type
	})
}

func validateDescription(description Description) error {
	if strings.TrimSpace(description.Root) == "" || strings.TrimSpace(description.Root) != description.Root {
		return fmt.Errorf("di: manifest root is invalid")
	}
	providers := make(map[string]struct{}, len(description.Providers))
	for index, provider := range description.Providers {
		if strings.TrimSpace(provider.ID) == "" || strings.TrimSpace(provider.ID) != provider.ID {
			return fmt.Errorf("di: manifest provider %d has invalid id", index)
		}
		if _, duplicate := providers[provider.ID]; duplicate {
			return fmt.Errorf("di: manifest provider id %q is duplicated", provider.ID)
		}
		providers[provider.ID] = struct{}{}
		if strings.TrimSpace(provider.Module) == "" || strings.TrimSpace(provider.Type) == "" {
			return fmt.Errorf("di: manifest provider %q has empty module or type", provider.ID)
		}
		if provider.Scope != "application" && provider.Scope != "transient" {
			return fmt.Errorf("di: manifest provider %q has invalid scope %q", provider.ID, provider.Scope)
		}
		if provider.State != "registered" && provider.State != "ready" && provider.State != "failed" {
			return fmt.Errorf("di: manifest provider %q has invalid state %q", provider.ID, provider.State)
		}
		if provider.State == "ready" && (!provider.Constructed || provider.Constructs == 0) {
			return fmt.Errorf("di: manifest provider %q is ready without construction", provider.ID)
		}
		if provider.Constructed && provider.Constructs == 0 {
			return fmt.Errorf("di: manifest provider %q is constructed without a construction count", provider.ID)
		}
		for dependencyIndex, dependency := range provider.Dependencies {
			if strings.TrimSpace(dependency) == "" {
				return fmt.Errorf("di: manifest provider %q dependency %d is empty", provider.ID, dependencyIndex)
			}
			if dependencyIndex > 0 && dependency == provider.Dependencies[dependencyIndex-1] {
				return fmt.Errorf("di: manifest provider %q dependency %q is duplicated", provider.ID, dependency)
			}
		}
	}
	for index, edge := range description.Edges {
		if _, exists := providers[edge.From]; !exists {
			return fmt.Errorf("di: manifest edge %d has unknown source %q", index, edge.From)
		}
		if _, exists := providers[edge.To]; !exists {
			return fmt.Errorf("di: manifest edge %d has unknown target %q", index, edge.To)
		}
		if strings.TrimSpace(edge.Type) == "" {
			return fmt.Errorf("di: manifest edge %d has empty type", index)
		}
		if index > 0 && edge == description.Edges[index-1] {
			return fmt.Errorf("di: manifest edge %s -> %s for %s is duplicated", edge.From, edge.To, edge.Type)
		}
	}
	components := make(map[string]struct{}, len(description.Components))
	for index, component := range description.Components {
		nameValid := strings.TrimSpace(component.Name) != "" && strings.TrimSpace(component.Name) == component.Name
		kindValid := strings.TrimSpace(component.Kind) != "" && strings.TrimSpace(component.Kind) == component.Kind
		if !nameValid || !kindValid {
			return fmt.Errorf("di: manifest component %d has invalid name or kind", index)
		}
		if _, exists := components[component.Name]; exists {
			return fmt.Errorf("di: manifest component %q is duplicated", component.Name)
		}
		components[component.Name] = struct{}{}
	}
	roots := make(map[string]struct{}, len(description.Roots))
	for index, root := range description.Roots {
		nameValid := strings.TrimSpace(root.Name) != "" && strings.TrimSpace(root.Name) == root.Name
		kindValid := strings.TrimSpace(root.Kind) != "" && strings.TrimSpace(root.Kind) == root.Kind
		if !nameValid || !kindValid {
			return fmt.Errorf("di: manifest root %d has invalid name or kind", index)
		}
		if _, exists := roots[root.Name]; exists {
			return fmt.Errorf("di: manifest root %q is duplicated", root.Name)
		}
		roots[root.Name] = struct{}{}
	}
	services := make(map[string]struct{}, len(description.Services))
	for index, service := range description.Services {
		if strings.TrimSpace(service.Name) == "" || strings.TrimSpace(service.Name) != service.Name || service.HTTPRoutes < 0 {
			return fmt.Errorf("di: manifest service %d is invalid", index)
		}
		if _, exists := services[service.Name]; exists {
			return fmt.Errorf("di: manifest service %q is duplicated", service.Name)
		}
		services[service.Name] = struct{}{}
	}
	return nil
}
