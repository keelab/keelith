// Package wiringcompiler builds and validates Keelith's static wiring graph.
package wiringcompiler

import (
	"fmt"
	"sort"
)

// Graph is the compiler's single, serializable description of application
// wiring. It contains only source-level facts; generated expressions and
// runtime values do not belong here.
type Graph struct {
	Module    string
	Root      string
	Providers []Provider
	Edges     []Edge
	Services  []Service
	Roots     []Root
}

// Service is the compiler-level projection of a generated service manifest.
type Service struct {
	Name       string
	Operations []string
	Transports []string
	HTTPRoutes int
}

// Root identifies an application entrypoint and its optional constructor.
type Root struct {
	Name        string
	Kind        string
	Provider    string
	ProviderIDs []string
}

// Edge connects a provider output to one constructor input.
type Edge struct {
	From string
	To   string
	Type string
}

// BuildGraph validates and returns a deterministic wiring IR.
func BuildGraph(module, root string, providers []Provider) (Graph, error) {
	return BuildGraphWithServices(module, root, providers, nil)
}

// BuildGraphWithServices builds the wiring graph and carries generated service
// contracts alongside providers without exposing the full contract schema.
func BuildGraphWithServices(module, root string, providers []Provider, services []Service) (Graph, error) {
	return BuildGraphWithServicesAndRoots(module, root, providers, services, nil)
}

// BuildGraphWithServicesAndRoots carries all static project entrypoints in
// the same IR used for provider and service validation.
func BuildGraphWithServicesAndRoots(module, root string, providers []Provider, services []Service, roots []Root) (Graph, error) {
	if module == "" || root == "" {
		return Graph{}, fmt.Errorf("wiring compiler: module and root are required")
	}
	ordered, err := orderProviders(append([]Provider(nil), providers...))
	if err != nil {
		return Graph{}, fmt.Errorf("build wiring graph: %w", err)
	}
	ids := make(map[string]string, len(ordered))
	for _, provider := range ordered {
		id := module + ":provider/" + provider.Spec.Constructor
		if len(provider.Outputs) == 0 {
			ids[provider.OutputKey] = id
			continue
		}
		for _, output := range provider.Outputs {
			ids[output.Key] = id + "/" + output.Field
		}
	}
	edges := make([]Edge, 0)
	for _, provider := range ordered {
		id := module + ":provider/" + provider.Spec.Constructor
		fromIDs := []string{id}
		if len(provider.Outputs) > 0 {
			fromIDs = fromIDs[:0]
			for _, output := range provider.Outputs {
				fromIDs = append(fromIDs, id+"/"+output.Field)
			}
		}
		for _, input := range provider.InputTypes {
			to := ids[input]
			if to == "" {
				continue
			}
			for _, from := range fromIDs {
				edges = append(edges, Edge{From: from, To: to, Type: input})
			}
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From != edges[j].From {
			return edges[i].From < edges[j].From
		}
		if edges[i].To != edges[j].To {
			return edges[i].To < edges[j].To
		}
		return edges[i].Type < edges[j].Type
	})
	resolvedRoots := append([]Root(nil), roots...)
	byConstructor := make(map[string]Provider, len(ordered))
	for _, provider := range ordered {
		byConstructor[provider.Spec.Constructor] = provider
	}
	for index := range resolvedRoots {
		if resolvedRoots[index].Provider == "" {
			continue
		}
		seen := make(map[string]struct{})
		var visit func(string)
		visit = func(constructor string) {
			if _, exists := seen[constructor]; exists {
				return
			}
			provider, exists := byConstructor[constructor]
			if !exists {
				return
			}
			seen[constructor] = struct{}{}
			seenID := module + ":provider/" + constructor
			resolvedRoots[index].ProviderIDs = append(resolvedRoots[index].ProviderIDs, seenID)
			for _, input := range provider.InputTypes {
				if dependency, ok := providerByInput(ordered, input); ok {
					visit(dependency.Spec.Constructor)
				}
			}
		}
		visit(resolvedRoots[index].Provider)
		sort.Strings(resolvedRoots[index].ProviderIDs)
	}
	return Graph{Module: module, Root: root, Providers: ordered, Edges: edges, Services: append([]Service(nil), services...), Roots: resolvedRoots}, nil
}

func providerByInput(providers []Provider, input string) (Provider, bool) {
	for _, provider := range providers {
		if provider.OutputKey == input || (provider.OutputKey == "" && provider.OutputType == input) {
			return provider, true
		}
		for _, output := range provider.Outputs {
			if output.Key == input {
				return provider, true
			}
		}
	}
	return Provider{}, false
}
