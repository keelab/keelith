package app

import (
	"context"
	"errors"
	"fmt"
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
	Name() string
	Start(context.Context) error
	Stop(context.Context) error
}

// DependencyProvider declares component names required before Start.
type DependencyProvider interface {
	Dependencies() []string
}

// ComponentFunc adapts functions to Component and DependencyProvider.
type ComponentFunc struct {
	ComponentName string
	DependsOn     []string
	StartFunc     func(context.Context) error
	StopFunc      func(context.Context) error
}

// Name returns the component's graph identifier.
func (c ComponentFunc) Name() string { return c.ComponentName }

// Dependencies returns an independent dependency list.
func (c ComponentFunc) Dependencies() []string {
	return append([]string(nil), c.DependsOn...)
}

// Start calls StartFunc when it is set.
func (c ComponentFunc) Start(ctx context.Context) error {
	if c.StartFunc == nil {
		return nil
	}
	return c.StartFunc(ctx)
}

// Stop calls StopFunc when it is set.
func (c ComponentFunc) Stop(ctx context.Context) error {
	if c.StopFunc == nil {
		return nil
	}
	return c.StopFunc(ctx)
}

func sortComponents(components []Component) ([]Component, error) {
	byName := make(map[string]int, len(components))
	names := make([]string, len(components))
	dependencies := make([][]string, len(components))

	for index, component := range components {
		if isNilInterface(component) {
			return nil, fmt.Errorf("%w: component %d is nil", ErrInvalidComponentGraph, index)
		}
		name := strings.TrimSpace(component.Name())
		if name == "" || name != component.Name() {
			return nil, fmt.Errorf("%w: component %d has an invalid name %q", ErrInvalidComponentGraph, index, component.Name())
		}
		if _, duplicate := byName[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate component %q", ErrInvalidComponentGraph, name)
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
