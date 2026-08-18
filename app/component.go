package app

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ErrInvalidComponentGraph reports duplicate, missing, or cyclic component
// dependencies.
var ErrInvalidComponentGraph = errors.New("app: invalid component graph")

// Component is an instance-scoped dependency initialized before servers.
//
// Components start after all of their declared dependencies and stop in the
// exact reverse order after servers have drained.
type Component interface {
	// Name returns the component's stable graph identifier.
	Name() string
	// Start initializes the component.
	Start(context.Context) error
	// Stop shuts down the component.
	Stop(context.Context) error
}

// DependencyProvider declares component names required before Start.
type DependencyProvider interface {
	// Dependencies returns the component's dependency list.
	Dependencies() []string
}

// ComponentFunc adapts functions to Component and DependencyProvider.
type ComponentFunc struct {
	ComponentName string                      // the component's stable graph identifier
	DependsOn     []string                    // the component's dependency list
	StartFunc     func(context.Context) error // the component's start function
	StopFunc      func(context.Context) error // the component's stop function
}

// Name returns the component's stable graph identifier.
func (c ComponentFunc) Name() string { return c.ComponentName }

// Dependencies returns an independent dependency list.
func (c ComponentFunc) Dependencies() []string {
	return append([]string(nil), c.DependsOn...)
}

// Start initializes the component.
func (c ComponentFunc) Start(ctx context.Context) error {
	if c.StartFunc == nil {
		return nil
	}
	return c.StartFunc(ctx)
}

// Stop shuts down the component.
func (c ComponentFunc) Stop(ctx context.Context) error {
	if c.StopFunc == nil {
		return nil
	}
	return c.StopFunc(ctx)
}

// sortComponents sorts the components in topological order.
func sortComponents(components []Component) ([]Component, error) {
	byName := make(map[string]int, len(components))
	names := make([]string, len(components))
	dependencies := make([][]string, len(components))
	for index, component := range components {
		if isNilComponent(component) {
			return nil, fmt.Errorf("%w: component %d is nil", ErrInvalidComponentGraph, index)
		}
		name := strings.TrimSpace(component.Name())
		if name == "" || name != component.Name() {
			return nil, fmt.Errorf("%w: component %d has an invalid name %q", ErrInvalidComponentGraph, index, component.Name())
		}
		if _, duplicate := byName[name]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate component %q",
				ErrInvalidComponentGraph,
				name,
			)
		}
		byName[name] = index
		names[index] = name
		if provider, ok := component.(DependencyProvider); ok {
			dependencies[index] = append([]string(nil), provider.Dependencies()...)
		}
	}

	for index, required := range dependencies {
		seen := make(map[string]struct{}, len(required))
		for dependencyIndex, dependency := range required {
			name := strings.TrimSpace(dependency)
			if name == "" || name != dependency {
				return nil, fmt.Errorf("%w: component %q dependency %d is invalid", ErrInvalidComponentGraph, names[index], dependencyIndex)
			}
			if name == names[index] {
				return nil, fmt.Errorf("%w: component %q depends on itself", ErrInvalidComponentGraph, name)
			}
			if _, exists := byName[name]; !exists {
				return nil, fmt.Errorf("%w: component %q requires unknown component %q", ErrInvalidComponentGraph, names[index], name)
			}
			if _, duplicate := seen[name]; duplicate {
				return nil, fmt.Errorf("%w: component %q repeats dependency %q", ErrInvalidComponentGraph, names[index], name)
			}
			seen[name] = struct{}{}
		}
	}

	const (
		unvisited uint8 = iota
		visiting
		visited
	)

	states := make([]uint8, len(components))
	stack := make([]string, 0, len(components))
	order := make([]Component, 0, len(components))

	var visit func(int) error
	visit = func(index int) error {
		switch states[index] {
		case visited:
			return nil
		case visiting:
			cycle := append(append([]string(nil), stack...), names[index])
			return fmt.Errorf("%w: dependency cycle %s", ErrInvalidComponentGraph, strings.Join(cycle, " -> "))
		}
		states[index] = visiting
		stack = append(stack, names[index])
		for _, dependency := range dependencies[index] {
			if err := visit(byName[dependency]); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		states[index] = visited
		order = append(order, components[index])
		return nil
	}

	for index := range components {
		if err := visit(index); err != nil {
			return nil, err
		}
	}

	return order, nil
}

func isNilComponent(component Component) bool {
	if component == nil {
		return true
	}
	value := reflect.ValueOf(component)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
