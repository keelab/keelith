package generator

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/keelab/keelith/contract"
	"google.golang.org/protobuf/compiler/protogen"
)

type manifestDependencyTarget struct {
	service      string
	descriptor   *protogen.Service
	goImportPath string
	goPackage    string
	goName       string
}

type manifestDependencyKey struct {
	kind      string
	transport string
	service   string
	binding   string
	reason    string
}

func generateManifest(
	plugin *protogen.Plugin,
	file *protogen.File,
) error {
	targets, err := manifestDependencyTargets(plugin)
	if err != nil {
		return err
	}
	projectionRules, err := fileProjectionRules(file)
	if err != nil {
		return fmt.Errorf(
			"generator: manifest projections in %s: %w",
			file.Desc.Path(),
			err,
		)
	}
	sort.Slice(projectionRules, func(first, second int) bool {
		return projectionRules[first].ID < projectionRules[second].ID
	})
	document := contract.Manifest{
		SchemaVersion:     contract.ManifestSchemaVersion,
		GeneratorProtocol: ProtocolVersion,
		Source:            file.Desc.Path(),
		Package:           string(file.Desc.Package()),
		GoImportPath:      string(file.GoImportPath),
		GoPackage:         string(file.GoPackageName),
	}
	for _, rule := range projectionRules {
		document.Projections = append(document.Projections, contract.Projection{
			ID:          rule.ID,
			Message:     rule.Message,
			KeyFields:   append([]string(nil), rule.KeyFields...),
			SchemaMajor: rule.SchemaMajor,
		})
	}
	dependencies := make(map[manifestDependencyKey]*contract.Dependency)
	for _, service := range file.Services {
		serviceName := string(service.Desc.FullName())
		currentTarget := targets[serviceName]
		manifestService := contract.Service{
			Name:   serviceName,
			GoName: service.GoName,
		}
		grpcListener := contract.Listener{
			Transport: "grpc",
			Service:   serviceName,
		}
		httpListener := contract.Listener{
			Transport: "http",
			Service:   serviceName,
		}
		gatewayDependency := contract.Dependency{
			Kind:         contract.DependencyGeneratedAdapter,
			Transport:    "grpc",
			Service:      serviceName,
			GoImportPath: currentTarget.goImportPath,
			GoPackage:    currentTarget.goPackage,
			GoName:       currentTarget.goName,
			Reason:       "generated-http-gateway",
		}
		for _, method := range service.Methods {
			operationName := "/" + serviceName + "/" + string(method.Desc.Name())
			entry := contract.Method{
				Name:      string(method.Desc.Name()),
				Operation: operationName,
				Kind:      manifestMethodKind(method),
				Input:     string(method.Input.Desc.FullName()),
				Output:    string(method.Output.Desc.FullName()),
			}
			continuation, ok, err := methodContinuationRule(method)
			if err != nil {
				return fmt.Errorf(
					"generator: manifest method %s: %w",
					method.Desc.FullName(),
					err,
				)
			}
			if ok {
				entry.Continuation = &contract.Continuation{
					MachineVersion:     continuation.MachineVersion,
					InlineBudgetMillis: continuation.InlineBudgetMillis,
					RetentionSeconds:   continuation.RetentionSeconds,
					MaxPayloadBytes:    continuation.MaxPayloadBytes,
				}
			}
			rules, ok, err := methodHTTPRules(method)
			if err != nil {
				return fmt.Errorf(
					"generator: manifest method %s: %w",
					method.Desc.FullName(),
					err,
				)
			}
			if ok {
				entry.HTTP = &contract.HTTPBinding{
					Method:       rules[0].Method,
					Path:         rules[0].Path,
					Body:         rules[0].Body,
					ResponseBody: rules[0].ResponseBody,
				}
				additional := make(
					contract.HTTPBindings,
					0,
					len(rules)-1,
				)
				for index, rule := range rules {
					if index > 0 {
						additional = append(
							additional,
							contract.HTTPBinding{
								Method:       rule.Method,
								Path:         rule.Path,
								Body:         rule.Body,
								ResponseBody: rule.ResponseBody,
							},
						)
					}
					httpListener.Routes = append(
						httpListener.Routes,
						contract.Route{
							Method:    rule.Method,
							Path:      rule.Path,
							Operation: operationName,
						},
					)
				}
				if len(additional) > 0 {
					entry.HTTP.Additional = &additional
				}
				gatewayDependency.Operations = append(
					gatewayDependency.Operations,
					operationName,
				)
			}
			declared, err := methodServiceDependencies(method)
			if err != nil {
				return fmt.Errorf(
					"generator: manifest method %s dependencies: %w",
					method.Desc.FullName(),
					err,
				)
			}
			for _, declaration := range declared {
				if declaration.Service == serviceName {
					return fmt.Errorf(
						"generator: method %s declares a self dependency",
						method.Desc.FullName(),
					)
				}
				target, exists := targets[declaration.Service]
				if !exists {
					return fmt.Errorf(
						"generator: method %s dependency service %q is not in the descriptor set",
						method.Desc.FullName(),
						declaration.Service,
					)
				}
				if declaration.Transport == "http" {
					mappings, mappingErr := serviceHTTPMappings(
						target.descriptor,
					)
					if mappingErr != nil {
						return fmt.Errorf(
							"generator: method %s HTTP dependency service %q: %w",
							method.Desc.FullName(),
							declaration.Service,
							mappingErr,
						)
					}
					if len(mappings) == 0 {
						return fmt.Errorf(
							"generator: method %s HTTP dependency service %q has no HTTP mapping",
							method.Desc.FullName(),
							declaration.Service,
						)
					}
				}
				mergeManifestDependency(
					dependencies,
					contract.Dependency{
						Kind:         contract.DependencyDeclared,
						Transport:    declaration.Transport,
						Service:      declaration.Service,
						GoImportPath: target.goImportPath,
						GoPackage:    target.goPackage,
						GoName:       target.goName,
						Binding:      declaration.Binding,
						Reason:       declaration.Reason,
						Operations:   []string{operationName},
					},
				)
			}
			manifestService.Methods = append(manifestService.Methods, entry)
		}
		document.Services = append(document.Services, manifestService)
		document.Listeners = append(document.Listeners, grpcListener)
		if len(httpListener.Routes) > 0 {
			document.Listeners = append(document.Listeners, httpListener)
			mergeManifestDependency(
				dependencies,
				gatewayDependency,
			)
		}
	}
	document.Dependencies = sortedManifestDependencies(dependencies)
	if err := contract.Validate(document); err != nil {
		return fmt.Errorf("generator: validate contract manifest: %w", err)
	}
	payload, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("generator: encode contract manifest: %w", err)
	}
	payload = append(payload, '\n')
	output := plugin.NewGeneratedFile(
		file.GeneratedFilenamePrefix+".keelith.manifest.json",
		"",
	)
	output.P(string(payload))
	return nil
}

func manifestDependencyTargets(
	plugin *protogen.Plugin,
) (map[string]manifestDependencyTarget, error) {
	result := make(map[string]manifestDependencyTarget)
	for _, file := range plugin.Files {
		for _, service := range file.Services {
			name := string(service.Desc.FullName())
			if _, duplicate := result[name]; duplicate {
				return nil, fmt.Errorf(
					"generator: dependency service %q is duplicated",
					name,
				)
			}
			result[name] = manifestDependencyTarget{
				service:      name,
				descriptor:   service,
				goImportPath: string(file.GoImportPath),
				goPackage:    string(file.GoPackageName),
				goName:       service.GoName,
			}
		}
	}
	return result, nil
}

func mergeManifestDependency(
	dependencies map[manifestDependencyKey]*contract.Dependency,
	candidate contract.Dependency,
) {
	key := manifestDependencyKey{
		kind:      candidate.Kind,
		transport: candidate.Transport,
		service:   candidate.Service,
		binding:   candidate.Binding,
		reason:    candidate.Reason,
	}
	existing := dependencies[key]
	if existing == nil {
		cloned := candidate
		cloned.Operations = append([]string(nil), candidate.Operations...)
		dependencies[key] = &cloned
		return
	}
	existing.Operations = append(existing.Operations, candidate.Operations...)
}

func sortedManifestDependencies(
	dependencies map[manifestDependencyKey]*contract.Dependency,
) []contract.Dependency {
	result := make([]contract.Dependency, 0, len(dependencies))
	for _, dependency := range dependencies {
		cloned := *dependency
		sort.Strings(cloned.Operations)
		result = append(result, cloned)
	}
	sort.Slice(result, func(first, second int) bool {
		left := result[first]
		right := result[second]
		switch {
		case left.Kind != right.Kind:
			return left.Kind < right.Kind
		case left.Service != right.Service:
			return left.Service < right.Service
		case left.Transport != right.Transport:
			return left.Transport < right.Transport
		case left.Binding != right.Binding:
			return left.Binding < right.Binding
		default:
			return left.Reason < right.Reason
		}
	})
	return result
}

func manifestMethodKind(method *protogen.Method) string {
	switch {
	case method.Desc.IsStreamingClient() && method.Desc.IsStreamingServer():
		return "bidi-stream"
	case method.Desc.IsStreamingClient():
		return "client-stream"
	case method.Desc.IsStreamingServer():
		return "server-stream"
	default:
		return "unary"
	}
}
