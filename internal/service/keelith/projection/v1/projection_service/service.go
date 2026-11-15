// Package serviceimpl contains the application implementation for keelith.projection.v1.ProjectionService.
package serviceimpl

import (
	projectionv1 "github.com/keelab/keelith/api/projection/v1"
	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/feature"
	appcomponent "github.com/keelab/keelith/internal/component"
	appdependency "github.com/keelab/keelith/internal/dependency"
)

// Service is the application-owned implementation root.
//
// Add methods to this type to override the generated unimplemented defaults.
type Service struct {
	projectionv1.UnimplementedProjectionServiceKeelithServer
	outbound     *kclient.Outbound
	features     *feature.Store
	components   *appcomponent.Resources
	dependencies *appdependency.Clients
}

// New creates the service implementation.
func New() *Service {
	return &Service{}
}

// BindOutbound injects the shared client runtime before servers start.
func (service *Service) BindOutbound(outbound *kclient.Outbound) {
	service.outbound = outbound
}

// Outbound returns the shared client runtime for typed dependency clients.
func (service *Service) Outbound() *kclient.Outbound {
	return service.outbound
}

// BindFeatures injects the shared dynamic feature evaluator before servers start.
func (service *Service) BindFeatures(features *feature.Store) {
	service.features = features
}

// Features returns the shared dynamic feature evaluator.
func (service *Service) Features() *feature.Store {
	return service.features
}

// BindComponents injects generated application resources before servers start.
func (service *Service) BindComponents(components *appcomponent.Resources) {
	service.components = components
}

// Components returns generated application resources.
func (service *Service) Components() *appcomponent.Resources {
	return service.components
}

// BindDependencies injects generated typed clients before servers start.
func (service *Service) BindDependencies(dependencies *appdependency.Clients) {
	service.dependencies = dependencies
}

// Dependencies returns generated typed clients declared by contract methods.
func (service *Service) Dependencies() *appdependency.Clients {
	return service.dependencies
}
