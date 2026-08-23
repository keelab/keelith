package generator

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/keelab/keelith/internal/protowkt"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type openAPIDocument struct {
	OpenAPI    string                 `json:"openapi"`
	Info       openAPIInfo            `json:"info"`
	Paths      map[string]openAPIPath `json:"paths"`
	Components openAPIComponents      `json:"components"`
	Tags       []map[string]string    `json:"tags,omitempty"`
}

type openAPIInfo struct {
	Title   string `json:"title"`
	Version string `json:"version"`
}

type openAPIPath map[string]openAPIOperation

type openAPIOperation struct {
	OperationID string                     `json:"operationId"`
	Summary     string                     `json:"summary"`
	Tags        []string                   `json:"tags,omitempty"`
	Parameters  []openAPIParameter         `json:"parameters,omitempty"`
	RequestBody *openAPIRequestBody        `json:"requestBody,omitempty"`
	Responses   map[string]openAPIResponse `json:"responses"`
}

type openAPIParameter struct {
	Name          string         `json:"name"`
	In            string         `json:"in"`
	Required      bool           `json:"required,omitempty"`
	AllowReserved bool           `json:"allowReserved,omitempty"`
	Schema        map[string]any `json:"schema"`
}

type openAPIRequestBody struct {
	Required bool                    `json:"required"`
	Content  map[string]openAPIMedia `json:"content"`
}

type openAPIResponse struct {
	Description string                  `json:"description"`
	Content     map[string]openAPIMedia `json:"content,omitempty"`
}

type openAPIMedia struct {
	Schema map[string]any `json:"schema"`
}

type openAPIComponents struct {
	Schemas map[string]map[string]any `json:"schemas"`
}

func generateOpenAPI(
	plugin *protogen.Plugin,
	file *protogen.File,
) error {
	document := openAPIDocument{
		OpenAPI: "3.1.0",
		Info: openAPIInfo{
			Title:   string(file.Desc.Package()),
			Version: "generated",
		},
		Paths: make(map[string]openAPIPath),
		Components: openAPIComponents{
			Schemas: map[string]map[string]any{
				"KeelithError": {
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"code": map[string]any{
							"type":   "integer",
							"format": "int32",
						},
						"reason":  map[string]any{"type": "string"},
						"message": map[string]any{"type": "string"},
						"metadata": map[string]any{
							"type": "object",
							"additionalProperties": map[string]any{
								"type": "string",
							},
						},
					},
					"required": []string{"code", "reason", "message"},
				},
			},
		},
	}
	tagSet := make(map[string]struct{})
	for _, service := range file.Services {
		mappings, err := serviceHTTPMappings(service)
		if err != nil {
			return fmt.Errorf("OpenAPI: %w", err)
		}
		for _, mapping := range mappings {
			for bindingIndex, rule := range mapping.rules {
				httpMethod := strings.ToLower(rule.Method)
				pathBindings, err := httpPathBindings(
					mapping.method.Input,
					rule.Path,
				)
				if err != nil {
					return fmt.Errorf("OpenAPI: %w", err)
				}
				openAPIPathValue := openAPIHTTPRoutePath(rule.Path, pathBindings)
				path := document.Paths[openAPIPathValue]
				if path == nil {
					path = make(openAPIPath)
					document.Paths[openAPIPathValue] = path
				}
				if _, duplicate := path[httpMethod]; duplicate {
					return fmt.Errorf(
						"duplicate OpenAPI route %s %s",
						rule.Method,
						openAPIPathValue,
					)
				}
				tag := string(service.Desc.FullName())
				tagSet[tag] = struct{}{}
				operation, err := openAPIMethod(
					service,
					mapping.method,
					rule,
					bindingIndex,
				)
				if err != nil {
					return err
				}
				path[httpMethod] = operation
			}
			collectMessageSchemas(document.Components.Schemas, mapping.method.Input)
			collectMessageSchemas(document.Components.Schemas, mapping.method.Output)
		}
	}
	if len(document.Paths) == 0 {
		return nil
	}
	tags := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		document.Tags = append(document.Tags, map[string]string{"name": tag})
	}
	payload, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("generator: encode OpenAPI: %w", err)
	}
	payload = append(payload, '\n')
	output := plugin.NewGeneratedFile(
		file.GeneratedFilenamePrefix+".openapi.json",
		"",
	)
	output.P(string(payload))
	return nil
}

func openAPIMethod(
	service *protogen.Service,
	method *protogen.Method,
	rule httpRule,
	bindingIndex int,
) (openAPIOperation, error) {
	pathBindings, err := httpPathBindings(method.Input, rule.Path)
	if err != nil {
		return openAPIOperation{}, err
	}
	responseSchema := schemaReference(method.Output)
	responseMediaType := "application/json"
	responseDescription := "protojson " +
		string(method.Output.Desc.FullName()) + " messages"
	if httpResponseUsesHTTPBody(method.Output, rule.ResponseBody) {
		responseSchema = rawHTTPBodySchema()
		responseMediaType = "application/octet-stream"
		responseDescription = "raw google.api.HttpBody payload"
	} else if rule.ResponseBody != "" {
		field, fieldErr := httpResponseBodyField(
			method.Output,
			rule.ResponseBody,
		)
		if fieldErr != nil {
			return openAPIOperation{}, fieldErr
		}
		responseSchema = fieldSchema(field)
		responseDescription = "protojson response_body field " +
			rule.ResponseBody + " from " +
			string(method.Output.Desc.FullName())
	}
	operation := openAPIOperation{
		OperationID: string(service.Desc.FullName()) + "." +
			string(method.Desc.Name()),
		Summary: method.GoName,
		Tags:    []string{string(service.Desc.FullName())},
		Responses: map[string]openAPIResponse{
			"200": {
				Description: "Successful response",
				Content: map[string]openAPIMedia{
					responseMediaType: {
						Schema: responseSchema,
					},
				},
			},
			"default": {
				Description: "Keelith error response",
				Content: map[string]openAPIMedia{
					"application/json": {
						Schema: map[string]any{
							"$ref": "#/components/schemas/KeelithError",
						},
					},
				},
			},
		},
	}
	if rule.Method == "HEAD" {
		success := operation.Responses["200"]
		success.Content = nil
		operation.Responses["200"] = success
	}
	if bindingIndex > 0 {
		operation.OperationID += fmt.Sprintf(".additional%d", bindingIndex)
	}
	if method.Desc.IsStreamingServer() {
		operation.Responses["200"] = openAPIResponse{
			Description: "Server-sent event stream",
			Content: map[string]openAPIMedia{
				"text/event-stream": {
					Schema: map[string]any{
						"type": "string",
						"description": "SSE data fields contain " +
							responseDescription,
					},
				},
			},
		}
	}
	excluded := make(
		[][]protoreflect.FieldDescriptor,
		0,
		len(pathBindings)+1,
	)
	for _, binding := range pathBindings {
		field := binding.fields[len(binding.fields)-1]
		excluded = append(excluded, binding.fields)
		if !binding.explicit {
			operation.Parameters = append(operation.Parameters, openAPIParameter{
				Name:     binding.fieldPath,
				In:       "path",
				Required: true,
				Schema:   fieldSchema(field),
			})
			continue
		}
		for _, capture := range binding.captures {
			schema := map[string]any{"type": "string"}
			if !binding.requiresString && len(binding.captures) == 1 {
				schema = fieldSchema(field)
			}
			operation.Parameters = append(operation.Parameters, openAPIParameter{
				Name:          capture.openAPIName,
				In:            "path",
				Required:      true,
				AllowReserved: capture.multi,
				Schema:        schema,
			})
		}
	}
	if rule.Body == "*" {
		mediaType := "application/json"
		schema := messageProjectionSchema(method.Input, excluded)
		if httpRequestUsesHTTPBody(method.Input, rule.Body) {
			mediaType = "application/octet-stream"
			schema = rawHTTPBodySchema()
		}
		operation.RequestBody = &openAPIRequestBody{
			Required: true,
			Content: map[string]openAPIMedia{
				mediaType: {
					Schema: schema,
				},
			},
		}
	} else {
		if rule.Body != "" {
			bodyPath, err := httpBodyFieldPath(method.Input, rule.Body)
			if err != nil {
				return openAPIOperation{}, err
			}
			excluded = append(excluded, bodyPath)
			mediaType := "application/json"
			schema := fieldSchema(bodyPath[len(bodyPath)-1])
			if httpRequestUsesHTTPBody(method.Input, rule.Body) {
				mediaType = "application/octet-stream"
				schema = rawHTTPBodySchema()
			}
			operation.RequestBody = &openAPIRequestBody{
				Required: true,
				Content: map[string]openAPIMedia{
					mediaType: {
						Schema: schema,
					},
				},
			}
		}
		queryFields, err := httpQueryFields(method.Input, excluded)
		if err != nil {
			return openAPIOperation{}, err
		}
		for _, queryField := range queryFields {
			operation.Parameters = append(
				operation.Parameters,
				openAPIParameter{
					Name:     queryField.name,
					In:       "query",
					Required: queryField.required,
					Schema:   fieldSchema(queryField.field.Desc),
				},
			)
		}
	}
	return operation, nil
}

func rawHTTPBodySchema() map[string]any {
	return map[string]any{"type": "string", "format": "binary"}
}

func collectMessageSchemas(
	schemas map[string]map[string]any,
	message *protogen.Message,
) {
	if message == nil || message.Desc.IsMapEntry() {
		return
	}
	name := string(message.Desc.FullName())
	if _, exists := schemas[name]; exists {
		return
	}
	// Reserve before recursion to handle recursive messages.
	schemas[name] = map[string]any{}
	schemas[name] = messageSchema(message, nil)
	for _, field := range message.Fields {
		if field.Message != nil && !field.Desc.IsMap() {
			collectMessageSchemas(schemas, field.Message)
		}
		if field.Desc.IsMap() && field.Message != nil &&
			len(field.Message.Fields) == 2 {
			collectMessageSchemas(schemas, field.Message.Fields[1].Message)
		}
	}
	for _, nested := range message.Messages {
		collectMessageSchemas(schemas, nested)
	}
}

func messageSchema(
	message *protogen.Message,
	excluded map[protoreflect.Name]struct{},
) map[string]any {
	properties := make(map[string]any)
	required := make([]string, 0)
	for index, field := range message.Fields {
		if _, skip := excluded[field.Desc.Name()]; skip {
			continue
		}
		properties[field.Desc.JSONName()] = fieldSchema(field.Desc)
		if fieldRequired(message.Fields[index]) {
			required = append(required, field.Desc.JSONName())
		}
	}
	sort.Strings(required)
	result := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           properties,
	}
	if len(required) > 0 {
		result["required"] = required
	}
	return result
}

func messageProjectionSchema(
	message *protogen.Message,
	excluded [][]protoreflect.FieldDescriptor,
) map[string]any {
	properties := make(map[string]any)
	required := make([]string, 0)
	for _, field := range message.Fields {
		descendants := make([][]protoreflect.FieldDescriptor, 0)
		skip := false
		for _, path := range excluded {
			if len(path) == 0 || path[0].FullName() != field.Desc.FullName() {
				continue
			}
			if len(path) == 1 {
				skip = true
				break
			}
			descendants = append(descendants, path[1:])
		}
		if skip {
			continue
		}
		schema := fieldSchema(field.Desc)
		if len(descendants) > 0 && field.Message != nil {
			schema = messageProjectionSchema(field.Message, descendants)
		}
		properties[field.Desc.JSONName()] = schema
		if fieldRequired(field) {
			required = append(required, field.Desc.JSONName())
		}
	}
	sort.Strings(required)
	result := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           properties,
	}
	if len(required) > 0 {
		result["required"] = required
	}
	return result
}

func fieldSchema(field protoreflect.FieldDescriptor) map[string]any {
	if field.IsMap() {
		value := field.MapValue()
		return map[string]any{
			"type":                 "object",
			"additionalProperties": scalarSchema(value),
		}
	}
	schema := scalarSchema(field)
	if field.IsList() {
		return map[string]any{
			"type":  "array",
			"items": schema,
		}
	}
	return schema
}

func scalarSchema(field protoreflect.FieldDescriptor) map[string]any {
	if field.Message() != nil {
		fullName := string(field.Message().FullName())
		switch fullName {
		case "google.protobuf.Timestamp":
			return map[string]any{"type": "string", "format": "date-time"}
		case "google.protobuf.Duration":
			return map[string]any{"type": "string", "format": "duration"}
		case "google.protobuf.FieldMask":
			return map[string]any{"type": "string"}
		default:
			switch protowkt.QueryKindFor(fullName) {
			case protowkt.QueryString:
				if fullName == "google.protobuf.BytesValue" {
					return map[string]any{"type": "string", "format": "byte"}
				}
				return map[string]any{"type": "string"}
			case protowkt.QueryBool:
				return map[string]any{"type": "boolean"}
			case protowkt.QueryNumber:
				switch fullName {
				case "google.protobuf.Int32Value":
					return map[string]any{"type": "integer", "format": "int32"}
				case "google.protobuf.UInt32Value":
					return map[string]any{
						"type": "integer", "format": "int32", "minimum": 0,
					}
				case "google.protobuf.FloatValue":
					return map[string]any{"type": "number", "format": "float"}
				default:
					return map[string]any{"type": "number", "format": "double"}
				}
			case protowkt.QueryIntegerString:
				format := "int64"
				pattern := "^-?[0-9]+$"
				if fullName == "google.protobuf.UInt64Value" {
					format = "uint64"
					pattern = "^[0-9]+$"
				}
				return map[string]any{
					"type": "string", "format": format, "pattern": pattern,
				}
			}
			return map[string]any{
				"$ref": "#/components/schemas/" + fullName,
			}
		}
	}
	if field.Enum() != nil {
		values := field.Enum().Values()
		names := make([]string, 0, values.Len())
		for index := range values.Len() {
			names = append(names, string(values.Get(index).Name()))
		}
		return map[string]any{"type": "string", "enum": names}
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		return map[string]any{"type": "boolean"}
	case protoreflect.StringKind:
		return map[string]any{"type": "string"}
	case protoreflect.BytesKind:
		return map[string]any{"type": "string", "format": "byte"}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind,
		protoreflect.Sfixed32Kind:
		return map[string]any{"type": "integer", "format": "int32"}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return map[string]any{
			"type":    "integer",
			"format":  "int32",
			"minimum": 0,
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed64Kind:
		return map[string]any{
			"type":    "string",
			"format":  "int64",
			"pattern": "^-?[0-9]+$",
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return map[string]any{
			"type":    "string",
			"format":  "uint64",
			"pattern": "^[0-9]+$",
		}
	case protoreflect.FloatKind:
		return map[string]any{"type": "number", "format": "float"}
	case protoreflect.DoubleKind:
		return map[string]any{"type": "number", "format": "double"}
	default:
		return map[string]any{}
	}
}

func schemaReference(message *protogen.Message) map[string]any {
	return map[string]any{
		"$ref": "#/components/schemas/" + string(message.Desc.FullName()),
	}
}
