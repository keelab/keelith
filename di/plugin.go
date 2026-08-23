package di

import (
	"fmt"
	"sort"
	"strings"
)

// Plugin describes one stable, low-cardinality application extension.
type Plugin struct {
	ID           string   `json:"id"`
	Priority     int      `json:"priority"`
	Capabilities []string `json:"capabilities,omitempty"`
}

// ModulePlugin binds stable plugin metadata to the ordinary DI Module that
// implements it. Plugins never bypass Module visibility or graph validation.
type ModulePlugin struct {
	Plugin Plugin
	Module Module
}

// IncludePlugins validates, deterministically orders, and includes plugin
// modules in a parent Module.
func IncludePlugins(plugins ...ModulePlugin) Option {
	snapshot := append([]ModulePlugin(nil), plugins...)
	return optionFunc(func(builder *moduleBuilder) error {
		metadata := make([]Plugin, len(snapshot))
		byID := make(map[string]Module, len(snapshot))
		for index, item := range snapshot {
			metadata[index] = item.Plugin
			if item.Module.name == "" {
				return fmt.Errorf("%w: plugin %d has an invalid module", ErrInvalidModule, index)
			}
			byID[strings.TrimSpace(item.Plugin.ID)] = item.Module
		}
		ordered, err := ValidatePlugins(metadata)
		if err != nil {
			return err
		}
		for _, item := range ordered {
			builder.includes = append(builder.includes, byID[item.ID])
		}
		return nil
	})
}

// ValidatePlugins validates identity uniqueness and returns a deterministic
// execution order: lower priority first, then stable ID.
func ValidatePlugins(plugins []Plugin) ([]Plugin, error) {
	result := append([]Plugin(nil), plugins...)
	seen := make(map[string]struct{}, len(result))
	for index := range result {
		result[index].ID = strings.TrimSpace(result[index].ID)
		if result[index].ID == "" {
			return nil, fmt.Errorf("%w: plugin %d has empty ID", ErrInvalidModule, index)
		}
		if _, duplicate := seen[result[index].ID]; duplicate {
			return nil, fmt.Errorf("%w: plugin %q", ErrDuplicateProvider, result[index].ID)
		}
		seen[result[index].ID] = struct{}{}
		result[index].Capabilities = append([]string(nil), result[index].Capabilities...)
		capabilities := make(map[string]struct{}, len(result[index].Capabilities))
		for capabilityIndex := range result[index].Capabilities {
			capability := strings.TrimSpace(result[index].Capabilities[capabilityIndex])
			if capability == "" || capability != result[index].Capabilities[capabilityIndex] {
				return nil, fmt.Errorf("%w: plugin %q has invalid capability", ErrInvalidModule, result[index].ID)
			}
			if _, duplicate := capabilities[capability]; duplicate {
				return nil, fmt.Errorf("%w: plugin %q capability %q", ErrDuplicateProvider, result[index].ID, capability)
			}
			capabilities[capability] = struct{}{}
		}
		sort.Strings(result[index].Capabilities)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Priority != result[j].Priority {
			return result[i].Priority < result[j].Priority
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}
