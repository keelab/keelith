package client

import (
	"fmt"

	"github.com/keelab/keelith/governance/dependency"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/selector"
)

// OutboundConfig defines one instance-scoped outbound invocation runtime.
//
// Dependency supplies the coherent policy, timeout, bulkhead, breaker,
// retry/hedging, and instance-outlier layers. Middleware and StreamMiddleware
// add application-owned stages such as telemetry after the governance chain.
type OutboundConfig struct {
	Dependency       dependency.Config
	Middleware       *middleware.Bundle
	StreamMiddleware *middleware.StreamBundle
}

// OutboundDescription is an immutable diagnostic view of an Outbound.
type OutboundDescription struct {
	Middleware       []middleware.Description
	StreamMiddleware []middleware.StreamDescription
	Dependency       dependency.Description
}

// Outbound is the standard composition point for Keelith client transports.
//
// It owns no goroutines or network resources. Routers and transport
// connections remain explicit App components, while every HTTP/gRPC client
// created from this object shares one coherent governance state.
type Outbound struct {
	dependency       *dependency.Runtime
	middleware       *middleware.Bundle
	streamMiddleware *middleware.StreamBundle
}

// NewOutbound assembles the standard outbound governance and extension chain.
func NewOutbound(config OutboundConfig) (*Outbound, error) {
	governance, err := dependency.New(config.Dependency)
	if err != nil {
		return nil, fmt.Errorf("%w: dependency runtime: %w", ErrInvalidOption, err)
	}
	unary, err := middleware.CombineBundles(
		governance.Middleware(),
		config.Middleware,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: combine middleware: %w", ErrInvalidOption, err)
	}
	stream, err := middleware.CombineStreamBundles(
		config.StreamMiddleware,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: combine stream middleware: %w", ErrInvalidOption, err)
	}
	return &Outbound{
		dependency:       governance,
		middleware:       unary,
		streamMiddleware: stream,
	}, nil
}

// Middleware returns the complete unary/client-creation middleware chain.
func (o *Outbound) Middleware() *middleware.Bundle {
	if o == nil {
		return nil
	}
	return o.middleware
}

// StreamMiddleware returns the per-stream lifecycle middleware chain.
func (o *Outbound) StreamMiddleware() *middleware.StreamBundle {
	if o == nil {
		return nil
	}
	return o.streamMiddleware
}

// InstanceObserver returns the passive instance-health observer shared by
// selectors created for this Outbound.
func (o *Outbound) InstanceObserver() selector.Observer {
	if o == nil || o.dependency == nil {
		return nil
	}
	return o.dependency.InstanceObserver()
}

// SelectorOptions returns the selector options required for per-attempt
// instance health feedback.
func (o *Outbound) SelectorOptions() []selector.Option {
	if o == nil || o.dependency == nil {
		return nil
	}
	return o.dependency.SelectorOptions()
}

// Describe returns an immutable snapshot of middleware and governance state.
func (o *Outbound) Describe() OutboundDescription {
	if o == nil {
		return OutboundDescription{}
	}
	return OutboundDescription{
		Middleware:       o.middleware.Describe(),
		StreamMiddleware: o.streamMiddleware.Describe(),
		Dependency:       o.dependency.Describe(),
	}
}
