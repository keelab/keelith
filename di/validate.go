package di

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Validate checks that root T is reachable without constructing providers.
func Validate[T any](modules ...Module) (Description, error) {
	root := reflect.TypeOf((*T)(nil)).Elem()
	builder, err := newBuilder(context.Background(), modules)
	if err != nil {
		return Description{}, err
	}
	description := Description{Root: root.String()}
	visited := make(map[scopedKey]bool)
	visiting := make([]scopedKey, 0)
	var visit func(scopedKey, Scope) error
	visit = func(required scopedKey, consumerScope Scope) error {
		binding := builder.bindings[required]
		if binding == nil {
			return fmt.Errorf("%w: %s/%s", ErrMissingProvider, required.module, required.key)
		}
		if consumerScope == ApplicationScope && binding.provider.scope == TransientScope {
			return fmt.Errorf(
				"%w: application provider captures transient %s/%s",
				ErrScopeViolation,
				required.module,
				required.key,
			)
		}
		for _, active := range visiting {
			if active == required {
				cycle := append(append([]scopedKey(nil), visiting...), required)
				parts := make([]string, len(cycle))
				for index := range cycle {
					parts[index] = cycle[index].module + "/" + cycle[index].key.String()
				}
				return fmt.Errorf("%w: %s", ErrDependencyCycle, strings.Join(parts, " -> "))
			}
		}
		if visited[required] {
			return nil
		}
		visiting = append(visiting, required)
		item := binding.provider
		providerDescription := staticProviderDescription(item)
		for inputIndex := 0; item.function != nil && inputIndex < item.function.NumIn(); inputIndex++ {
			parameter := item.function.In(inputIndex)
			if parameter == contextType {
				continue
			}
			if parameter.Kind() == reflect.Struct && embedsMarker(parameter, inType) {
				for fieldIndex := 0; fieldIndex < parameter.NumField(); fieldIndex++ {
					field := parameter.Field(fieldIndex)
					if field.Anonymous && field.Type == inType {
						continue
					}
					name, group, optional, parseErr := parseTag(field.Tag.Get("di"))
					if parseErr != nil {
						return parseErr
					}
					if group != "" {
						if field.Type.Kind() != reflect.Slice {
							return fmt.Errorf("%w: group field %s must be a slice", ErrInvalidProvider, field.Name)
						}
						groupKey := scopedKey{
							module: item.module,
							key:    key{typeOf: field.Type.Elem(), group: group},
						}
						for _, groupBinding := range builder.groups[groupKey] {
							required := scopedKey{
								module: groupBinding.provider.module,
								key:    groupBinding.provider.outputs[groupBinding.output],
							}
							if visitErr := visit(required, item.scope); visitErr != nil {
								return visitErr
							}
						}
						continue
					}
					dependency := key{typeOf: field.Type, name: name}
					resolved, exists := builder.lookup(item.module, dependency)
					if optional && !exists {
						continue
					}
					if !exists {
						return fmt.Errorf("%w: module %s requires %s", ErrMissingProvider, item.module, dependency)
					}
					if visitErr := visit(resolved, item.scope); visitErr != nil {
						return visitErr
					}
				}
				continue
			}
			if parameter.Implements(reflect.TypeOf((*lazyDescriptor)(nil)).Elem()) {
				descriptor := reflect.New(parameter).Elem().Interface().(lazyDescriptor)
				resolved, exists := builder.lookup(item.module, key{typeOf: descriptor.dependencyType()})
				if !exists {
					return fmt.Errorf("%w: lazy dependency", ErrMissingProvider)
				}
				if visitErr := visit(resolved, item.scope); visitErr != nil {
					return visitErr
				}
				continue
			}
			resolved, exists := builder.lookup(item.module, key{typeOf: parameter})
			if !exists {
				return fmt.Errorf("%w: module %s requires %s", ErrMissingProvider, item.module, parameter)
			}
			if visitErr := visit(resolved, item.scope); visitErr != nil {
				return visitErr
			}
		}
		for _, decorator := range binding.decorators {
			for inputIndex := 0; inputIndex < decorator.function.NumIn(); inputIndex++ {
				parameter := decorator.function.In(inputIndex)
				if parameter == contextType || parameter == required.key.typeOf {
					continue
				}
				resolved, exists := builder.lookup(decorator.module, key{typeOf: parameter})
				if !exists {
					return fmt.Errorf("%w: decorator dependency", ErrMissingProvider)
				}
				if visitErr := visit(resolved, decorator.scope); visitErr != nil {
					return fmt.Errorf("decorator %s parameter %d: %w", decorator.displayName, inputIndex, visitErr)
				}
			}
			description.Providers = append(description.Providers, staticProviderDescription(decorator))
		}
		visiting = visiting[:len(visiting)-1]
		visited[required] = true
		description.Providers = append(description.Providers, providerDescription)
		return nil
	}
	rootKey := key{typeOf: root}
	var resolvedRoot scopedKey
	for _, module := range builder.rootModules {
		if candidate, exists := builder.lookup(module, rootKey); exists {
			resolvedRoot = candidate
			break
		}
	}
	if resolvedRoot.module == "" {
		return Description{}, fmt.Errorf("%w: root %s", ErrMissingProvider, root)
	}
	if err := visit(resolvedRoot, ApplicationScope); err != nil {
		return Description{}, err
	}
	sort.Slice(description.Providers, func(i, j int) bool {
		return description.Providers[i].ID < description.Providers[j].ID
	})
	return describeEdges(description), nil
}

func staticProviderDescription(item *provider) ProviderDescription {
	scope := "application"
	if item.scope == TransientScope {
		scope = "transient"
	}
	description := ProviderDescription{
		ID:        item.module + ":" + item.displayName,
		Module:    item.module,
		Type:      item.outputs[0].typeOf.String(),
		Name:      item.outputs[0].name,
		Group:     item.outputs[0].group,
		Scope:     scope,
		State:     "registered",
		Decorator: item.decorator,
		Override:  item.override,
	}
	if item.function != nil {
		for index := 0; index < item.function.NumIn(); index++ {
			if item.function.In(index) != contextType {
				description.Dependencies = append(description.Dependencies, item.function.In(index).String())
			}
		}
	}
	return description
}
