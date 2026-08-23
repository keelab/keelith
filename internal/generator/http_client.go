package generator

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
)

func generateHTTPClient(
	output *protogen.GeneratedFile,
	service *protogen.Service,
) error {
	mappings, err := serviceHTTPMappings(service)
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		return nil
	}
	interfaceName := service.GoName + "KeelithHTTPClient"
	implementation := unexported(service.GoName) + "HTTPClient"
	output.P("// ", interfaceName, " is the annotated typed HTTP client contract.")
	output.P("type ", interfaceName, " interface {")
	for _, mapping := range mappings {
		if mapping.method.Desc.IsStreamingServer() {
			output.P(
				mapping.method.GoName,
				"(ctx ",
				output.QualifiedGoIdent(contextPackage.Ident("Context")),
				", input *",
				mapping.method.Input.GoIdent,
				", options ...",
				output.QualifiedGoIdent(
					httpPackage.Ident("SSEClientOption"),
				),
				") (*",
				output.QualifiedGoIdent(
					httpPackage.Ident("SSEClientStream"),
				),
				"[*",
				mapping.method.Output.GoIdent,
				"], error)",
			)
		} else {
			output.P(
				mapping.method.GoName,
				"(ctx ",
				output.QualifiedGoIdent(contextPackage.Ident("Context")),
				", input *",
				mapping.method.Input.GoIdent,
				") (*",
				mapping.method.Output.GoIdent,
				", error)",
			)
		}
	}
	output.P("}")
	output.P()
	output.P("type ", implementation, " struct {")
	output.P("client *", output.QualifiedGoIdent(httpPackage.Ident("Client")))
	output.P("baseURL string")
	for _, mapping := range mappings {
		output.P(
			unexported(mapping.method.GoName),
			"Operation ",
			output.QualifiedGoIdent(operationPackage.Ident("Operation")),
		)
	}
	output.P("}")
	output.P()
	output.P(
		"func New",
		service.GoName,
		"HTTPClient(client *",
		output.QualifiedGoIdent(httpPackage.Ident("Client")),
		", baseURL string) (",
		interfaceName,
		", error) {",
	)
	output.P("if client == nil {")
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(",
		fmt.Sprintf("%q", "keelith generated HTTP client: client is nil"),
		")",
	)
	output.P("}")
	output.P(
		"normalizedBaseURL, err := ",
		output.QualifiedGoIdent(httpPackage.Ident("NormalizeClientBaseURL")),
		"(baseURL)",
	)
	output.P("if err != nil { return nil, err }")
	for _, mapping := range mappings {
		method := mapping.method
		variable := unexported(method.GoName) + "Operation"
		output.P(
			variable,
			", err := ",
			output.QualifiedGoIdent(operationPackage.Ident("New")),
			"(",
			fmt.Sprintf("%q", "http"),
			", ",
			fmt.Sprintf("%q", string(service.Desc.FullName())),
			", ",
			fmt.Sprintf("%q", string(method.Desc.Name())),
			", ",
			output.QualifiedGoIdent(
				operationPackage.Ident(httpOperationKind(method)),
			),
			")",
		)
		output.P("if err != nil { return nil, err }")
	}
	output.P("return &", implementation, "{")
	output.P("client: client,")
	output.P("baseURL: normalizedBaseURL,")
	for _, mapping := range mappings {
		field := unexported(mapping.method.GoName) + "Operation"
		output.P(field, ": ", field, ",")
	}
	output.P("}, nil")
	output.P("}")
	output.P()
	output.P(
		"// New",
		service.GoName,
		"ManagedHTTPClient binds the generated typed client to a lifecycle-managed HTTP dependency.",
	)
	output.P(
		"func New",
		service.GoName,
		"ManagedHTTPClient(dependency *",
		output.QualifiedGoIdent(httpPackage.Ident("ManagedDependency")),
		") (",
		interfaceName,
		", error) {",
	)
	output.P("if dependency == nil {")
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(",
		fmt.Sprintf("%q", "keelith generated HTTP client: managed dependency is nil"),
		")",
	)
	output.P("}")
	output.P(
		"if dependency.Service() != ",
		fmt.Sprintf("%q", string(service.Desc.FullName())),
		" {",
	)
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(",
		fmt.Sprintf("%q", "keelith generated HTTP client: dependency service %q does not match %q"),
		", dependency.Service(), ",
		fmt.Sprintf("%q", string(service.Desc.FullName())),
		")",
	)
	output.P("}")
	output.P(
		"return New",
		service.GoName,
		"HTTPClient(dependency.Client(), dependency.BaseURL())",
	)
	output.P("}")
	output.P()

	for _, mapping := range mappings {
		generateHTTPClientMethod(output, mapping, implementation)
	}
	return nil
}

func generateHTTPClientMethod(
	output *protogen.GeneratedFile,
	mapping mappedHTTPMethod,
	implementation string,
) {
	method := mapping.method
	rule := mapping.rule
	if method.Desc.IsStreamingServer() {
		output.P(
			"func (client *",
			implementation,
			") ",
			method.GoName,
			"(ctx ",
			output.QualifiedGoIdent(contextPackage.Ident("Context")),
			", input *",
			method.Input.GoIdent,
			", options ...",
			output.QualifiedGoIdent(httpPackage.Ident("SSEClientOption")),
			") (*",
			output.QualifiedGoIdent(httpPackage.Ident("SSEClientStream")),
			"[*",
			method.Output.GoIdent,
			"], error) {",
		)
		output.P(
			"request, err := ",
			output.QualifiedGoIdent(httpPackage.Ident("NewProtoRequest")),
			"(ctx, client.baseURL, ",
			fmt.Sprintf("%q", rule.Method),
			", ",
			fmt.Sprintf("%q", rule.Path),
			", input, ",
			fmt.Sprintf("%q", rule.Body),
			")",
		)
		output.P("if err != nil { return nil, err }")
		openFunction := "OpenProtoSSE"
		responseArgument := ""
		if rule.ResponseBody != "" {
			openFunction = "OpenProtoSSEResponseBody"
			responseArgument = ", " + fmt.Sprintf("%q", rule.ResponseBody)
		}
		output.P(
			"return ",
			output.QualifiedGoIdent(httpPackage.Ident(openFunction)),
			"(ctx, client.client, client.",
			unexported(method.GoName),
			"Operation, request, func() *",
			method.Output.GoIdent,
			" { return new(",
			method.Output.GoIdent,
			") }",
			responseArgument,
			", options...)",
		)
		output.P("}")
		output.P()
		return
	}
	output.P(
		"func (client *",
		implementation,
		") ",
		method.GoName,
		"(ctx ",
		output.QualifiedGoIdent(contextPackage.Ident("Context")),
		", input *",
		method.Input.GoIdent,
		") (*",
		method.Output.GoIdent,
		", error) {",
	)
	output.P(
		"request, err := ",
		output.QualifiedGoIdent(httpPackage.Ident("NewProtoRequest")),
		"(ctx, client.baseURL, ",
		fmt.Sprintf("%q", rule.Method),
		", ",
		fmt.Sprintf("%q", rule.Path),
		", input, ",
		fmt.Sprintf("%q", rule.Body),
		")",
	)
	output.P("if err != nil { return nil, err }")
	invokeFunction := "InvokeProto"
	responseArgument := ""
	if httpResponseUsesHTTPBody(method.Output, rule.ResponseBody) {
		invokeFunction = "InvokeProtoHTTPBody"
		responseArgument = ", " + fmt.Sprintf("%q", rule.ResponseBody)
	} else if rule.ResponseBody != "" {
		invokeFunction = "InvokeProtoResponseBody"
		responseArgument = ", " + fmt.Sprintf("%q", rule.ResponseBody)
	}
	output.P(
		"return ",
		output.QualifiedGoIdent(httpPackage.Ident(invokeFunction)),
		"(ctx, client.client, client.",
		unexported(method.GoName),
		"Operation, request, func() *",
		method.Output.GoIdent,
		" { return new(",
		method.Output.GoIdent,
		") }",
		responseArgument,
		")",
	)
	output.P("}")
	output.P()
}

func generateHTTPGateway(
	output *protogen.GeneratedFile,
	service *protogen.Service,
	clientName string,
) error {
	mappings, err := serviceHTTPMappings(service)
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		return nil
	}
	output.P(
		"// Register",
		service.GoName,
		"Gateway binds annotated HTTP routes to the typed gRPC client.",
	)
	output.P(
		"func Register",
		service.GoName,
		"Gateway(router *",
		output.QualifiedGoIdent(httpPackage.Ident("Router")),
		", client ",
		clientName,
		") error {",
	)
	output.P("if router == nil || client == nil {")
	output.P(
		"return ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(",
		fmt.Sprintf("%q", "keelith generated gateway: router or client is nil"),
		")",
	)
	output.P("}")
	if hasServerStreamingHTTPMapping(mappings) {
		output.P(
			"sseEncoder, err := ",
			output.QualifiedGoIdent(httpPackage.Ident("NewSSEEncoder")),
			"(",
			output.QualifiedGoIdent(httpPackage.Ident("SSEConfig")),
			"{})",
		)
		output.P("if err != nil { return err }")
	}
	for _, mapping := range mappings {
		method := mapping.method
		operationVariable := unexported(
			service.GoName + method.GoName + "GatewayOperation",
		)
		output.P(
			operationVariable,
			", err := ",
			output.QualifiedGoIdent(operationPackage.Ident("New")),
			"(",
			fmt.Sprintf("%q", "http"),
			", ",
			fmt.Sprintf("%q", string(service.Desc.FullName())),
			", ",
			fmt.Sprintf("%q", string(method.Desc.Name())),
			", ",
			output.QualifiedGoIdent(
				operationPackage.Ident(httpOperationKind(method)),
			),
			")",
		)
		output.P("if err != nil { return err }")
		for _, rule := range mapping.rules {
			decoder, pathBindings, err := protoDecoderExpression(
				output,
				httpPackage,
				method.Input,
				rule,
			)
			if err != nil {
				return err
			}
			registration, route := standardHTTPRegistration(
				rule.Path,
				pathBindings,
			)
			if method.Desc.IsStreamingServer() {
				output.P(
					"if err := router.", registration, "(",
					fmt.Sprintf("%q", rule.Method),
					", ",
					fmt.Sprintf("%q", route),
					", ",
					operationVariable,
					", ",
					decoder,
					", ",
					unexported(service.GoName+method.GoName+"GatewayHandler"),
					"(client, ",
					fmt.Sprintf("%q", rule.ResponseBody),
					"), sseEncoder, ",
					output.QualifiedGoIdent(httpPackage.Ident("WithStreaming")),
					"()); err != nil { return err }",
				)
			} else {
				output.P(
					"if err := router.", registration, "(",
					fmt.Sprintf("%q", rule.Method),
					", ",
					fmt.Sprintf("%q", route),
					", ",
					operationVariable,
					", ",
					decoder,
					", ",
					unexported(service.GoName+method.GoName+"GatewayHandler"),
					"(client), ",
					protoResponseEncoderExpression(
						output,
						httpPackage,
						method.Output,
						rule.ResponseBody,
					),
					"); err != nil { return err }",
				)
			}
		}
	}
	output.P("return nil")
	output.P("}")
	output.P()
	for _, mapping := range mappings {
		method := mapping.method
		handlerName := unexported(service.GoName + method.GoName + "GatewayHandler")
		if method.Desc.IsStreamingServer() {
			output.P(
				"func ",
				handlerName,
				"(client ",
				clientName,
				", responseBody string) ",
				output.QualifiedGoIdent(middlewarePackage.Ident("Handler")),
				" {",
			)
		} else {
			output.P(
				"func ",
				handlerName,
				"(client ",
				clientName,
				") ",
				output.QualifiedGoIdent(middlewarePackage.Ident("Handler")),
				" {",
			)
		}
		output.P(
			"return func(ctx ",
			output.QualifiedGoIdent(contextPackage.Ident("Context")),
			", request any) (any, error) {",
		)
		output.P("input, ok := request.(*", method.Input.GoIdent, ")")
		output.P("if !ok {")
		output.P(
			"return nil, ",
			output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
			"(",
			fmt.Sprintf("%q", "keelith generated gateway: unexpected request type %T"),
			", request)",
		)
		output.P("}")
		if method.Desc.IsStreamingServer() {
			output.P(
				"stream, err := client.",
				method.GoName,
				"(ctx, input)",
			)
			output.P("if err != nil { return nil, err }")
			output.P(
				"events := make(chan ",
				output.QualifiedGoIdent(httpPackage.Ident("SSEEvent")),
				")",
			)
			output.P("failures := make(chan error)")
			output.P("go func() {")
			output.P("for {")
			output.P("message, err := stream.Recv()")
			output.P("if err != nil {")
			output.P(
				"if err == ",
				output.QualifiedGoIdent(ioPackage.Ident("EOF")),
				" { close(events); close(failures); return }",
			)
			output.P(
				"select { case failures <- err: case <-ctx.Done(): }",
			)
			output.P("close(failures)")
			output.P("close(events)")
			output.P("return")
			output.P("}")
			output.P(
				"event, err := ",
				output.QualifiedGoIdent(httpPackage.Ident("NewSSEProtoResponseBodyEvent")),
				"(",
				fmt.Sprintf("%q", ""),
				", ",
				fmt.Sprintf(
					"%q",
					string(method.Output.Desc.FullName()),
				),
				", message, responseBody, 0)",
			)
			output.P("if err != nil {")
			output.P(
				"select { case failures <- err: case <-ctx.Done(): }",
			)
			output.P("close(failures)")
			output.P("close(events)")
			output.P("return")
			output.P("}")
			output.P(
				"select { case events <- event: case <-ctx.Done(): close(events); close(failures); return }",
			)
			output.P("}")
			output.P("}()")
			output.P(
				"return ",
				output.QualifiedGoIdent(
					httpPackage.Ident("NewServerSentEvents"),
				),
				"(events, failures)",
			)
		} else {
			output.P("return client.", method.GoName, "(ctx, input)")
		}
		output.P("}")
		output.P("}")
		output.P()
	}
	return nil
}
