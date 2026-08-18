package app

import (
	"context"
	"errors"
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
