package service

import "github.com/keelab/keelith/middleware"

// Description is an immutable, bounded deployment-topology snapshot.
type Description struct {
	Name     string               `json:"name"`
	Groups   []GroupDescription   `json:"groups"`
	Services []ServiceDescription `json:"services"`
}

// GroupDescription describes one declared policy group before per-service
// scoping is applied.
type GroupDescription struct {
	Name             string                   `json:"name"`
	Services         []string                 `json:"services"`
	HTTPMiddleware   []middleware.Description `json:"http_middleware,omitempty"`
	GRPCMiddleware   []middleware.Description `json:"grpc_middleware,omitempty"`
	HTTPCapabilities []Capability             `json:"http_capabilities,omitempty"`
	GRPCCapabilities []Capability             `json:"grpc_capabilities,omitempty"`
	RequiredHTTP     []Capability             `json:"required_http_capabilities,omitempty"`
	RequiredGRPC     []Capability             `json:"required_grpc_capabilities,omitempty"`
}

// ServiceDescription describes one generated service and its final transport
// middleware chains.
//
//nolint:revive // The exported name distinguishes the nested service projection from Profile Description.
type ServiceDescription struct {
	Name           string                   `json:"name"`
	Group          string                   `json:"group,omitempty"`
	HTTP           bool                     `json:"http"`
	GRPC           bool                     `json:"grpc"`
	HTTPMiddleware []middleware.Description `json:"http_middleware,omitempty"`
	GRPCMiddleware []middleware.Description `json:"grpc_middleware,omitempty"`
}

func groupDescription(group Group) GroupDescription {
	services := make([]string, len(group.bindings))
	for index, binding := range group.bindings {
		services[index] = binding.name
	}
	httpBundle, _ := middleware.CombineBundles(group.httpBundles...)
	grpcBundle, _ := middleware.CombineBundles(group.grpcBundles...)
	return GroupDescription{
		Name:             group.name,
		Services:         services,
		HTTPMiddleware:   describeBundle(httpBundle),
		GRPCMiddleware:   describeBundle(grpcBundle),
		HTTPCapabilities: append([]Capability(nil), group.httpCapabilities...),
		GRPCCapabilities: append([]Capability(nil), group.grpcCapabilities...),
		RequiredHTTP:     append([]Capability(nil), group.requiredHTTP...),
		RequiredGRPC:     append([]Capability(nil), group.requiredGRPC...),
	}
}

func describeBundle(bundle *middleware.Bundle) []middleware.Description {
	if bundle == nil {
		return nil
	}
	return bundle.Describe()
}

func cloneDescription(description Description) Description {
	result := Description{Name: description.Name}
	result.Groups = make([]GroupDescription, len(description.Groups))
	for index, group := range description.Groups {
		result.Groups[index] = group
		result.Groups[index].Services = append([]string(nil), group.Services...)
		result.Groups[index].HTTPMiddleware = append([]middleware.Description(nil), group.HTTPMiddleware...)
		result.Groups[index].GRPCMiddleware = append([]middleware.Description(nil), group.GRPCMiddleware...)
		result.Groups[index].HTTPCapabilities = append([]Capability(nil), group.HTTPCapabilities...)
		result.Groups[index].GRPCCapabilities = append([]Capability(nil), group.GRPCCapabilities...)
		result.Groups[index].RequiredHTTP = append([]Capability(nil), group.RequiredHTTP...)
		result.Groups[index].RequiredGRPC = append([]Capability(nil), group.RequiredGRPC...)
	}
	result.Services = make([]ServiceDescription, len(description.Services))
	for index, service := range description.Services {
		result.Services[index] = service
		result.Services[index].HTTPMiddleware = append([]middleware.Description(nil), service.HTTPMiddleware...)
		result.Services[index].GRPCMiddleware = append([]middleware.Description(nil), service.GRPCMiddleware...)
	}
	return result
}
