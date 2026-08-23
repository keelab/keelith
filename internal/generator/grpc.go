package generator

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
)

func generateGRPCRegistration(
	output *protogen.GeneratedFile,
	file *protogen.File,
	service *protogen.Service,
	serverName string,
) {
	for _, method := range service.Methods {
		if method.Desc.IsStreamingClient() || method.Desc.IsStreamingServer() {
			generateClientStream(output, service, method)
		}
	}
	output.P(
		"// Register",
		service.GoName,
		"GRPC registers the generated adapter with grpc-go.",
	)
	output.P(
		"func Register",
		service.GoName,
		"GRPC(registrar ",
		output.QualifiedGoIdent(grpcPackage.Ident("ServiceRegistrar")),
		", service ",
		serverName,
		") {",
	)
	output.P(
		"registrar.RegisterService(&",
		service.GoName,
		"KeelithGRPCServiceDesc, service)",
	)
	output.P("}")
	output.P()

	for _, method := range service.Methods {
		if method.Desc.IsStreamingClient() || method.Desc.IsStreamingServer() {
			generateStreamHandler(output, service, method, serverName)
		} else {
			generateUnaryHandler(output, service, method, serverName)
		}
	}
	output.P(
		"var ",
		service.GoName,
		"KeelithGRPCServiceDesc = ",
		output.QualifiedGoIdent(grpcPackage.Ident("ServiceDesc")),
		"{",
	)
	output.P("ServiceName: ", fmt.Sprintf("%q", string(service.Desc.FullName())), ",")
	output.P("HandlerType: (*", serverName, ")(nil),")
	output.P("Methods: []", output.QualifiedGoIdent(grpcPackage.Ident("MethodDesc")), "{")
	for _, method := range service.Methods {
		if method.Desc.IsStreamingClient() || method.Desc.IsStreamingServer() {
			continue
		}
		output.P("{")
		output.P("MethodName: ", fmt.Sprintf("%q", string(method.Desc.Name())), ",")
		output.P(
			"Handler: ",
			unexported(service.GoName+method.GoName+"GRPCHandler"),
			",",
		)
		output.P("},")
	}
	output.P("},")
	output.P("Streams: []", output.QualifiedGoIdent(grpcPackage.Ident("StreamDesc")), "{")
	for _, method := range service.Methods {
		if !method.Desc.IsStreamingClient() && !method.Desc.IsStreamingServer() {
			continue
		}
		output.P("{")
		output.P("StreamName: ", fmt.Sprintf("%q", string(method.Desc.Name())), ",")
		output.P(
			"Handler: ",
			unexported(service.GoName+method.GoName+"GRPCStreamHandler"),
			",",
		)
		output.P("ServerStreams: ", method.Desc.IsStreamingServer(), ",")
		output.P("ClientStreams: ", method.Desc.IsStreamingClient(), ",")
		output.P("},")
	}
	output.P("},")
	output.P("Metadata: ", fmt.Sprintf("%q", file.Desc.Path()), ",")
	output.P("}")
	output.P()
}

func generateUnaryHandler(
	output *protogen.GeneratedFile,
	service *protogen.Service,
	method *protogen.Method,
	serverName string,
) {
	name := unexported(service.GoName + method.GoName + "GRPCHandler")
	fullMethod := "/" + string(service.Desc.FullName()) + "/" +
		string(method.Desc.Name())
	output.P(
		"func ",
		name,
		"(service any, ctx ",
		output.QualifiedGoIdent(contextPackage.Ident("Context")),
		", decode func(any) error, interceptor ",
		output.QualifiedGoIdent(grpcPackage.Ident("UnaryServerInterceptor")),
		") (any, error) {",
	)
	output.P("input := new(", method.Input.GoIdent, ")")
	output.P("if err := decode(input); err != nil { return nil, err }")
	output.P(
		"if interceptor == nil { return service.(",
		serverName,
		").",
		method.GoName,
		"(ctx, input) }",
	)
	output.P(
		"info := &",
		output.QualifiedGoIdent(grpcPackage.Ident("UnaryServerInfo")),
		"{Server: service, FullMethod: ",
		fmt.Sprintf("%q", fullMethod),
		"}",
	)
	output.P(
		"handler := func(ctx ",
		output.QualifiedGoIdent(contextPackage.Ident("Context")),
		", request any) (any, error) {",
	)
	output.P(
		"return service.(",
		serverName,
		").",
		method.GoName,
		"(ctx, request.(*",
		method.Input.GoIdent,
		"))",
	)
	output.P("}")
	output.P("return interceptor(ctx, input, info, handler)")
	output.P("}")
	output.P()
}

func generateStreamHandler(
	output *protogen.GeneratedFile,
	service *protogen.Service,
	method *protogen.Method,
	serverName string,
) {
	name := unexported(service.GoName + method.GoName + "GRPCStreamHandler")
	wrapper := unexported(service.GoName + method.GoName + "Server")
	output.P(
		"func ",
		name,
		"(service any, stream ",
		output.QualifiedGoIdent(grpcPackage.Ident("ServerStream")),
		") error {",
	)
	if !method.Desc.IsStreamingClient() {
		output.P("input := new(", method.Input.GoIdent, ")")
		output.P("if err := stream.RecvMsg(input); err != nil { return err }")
		output.P(
			"return service.(",
			serverName,
			").",
			method.GoName,
			"(input, &",
			wrapper,
			"{ServerStream: stream})",
		)
	} else {
		output.P(
			"return service.(",
			serverName,
			").",
			method.GoName,
			"(&",
			wrapper,
			"{ServerStream: stream})",
		)
	}
	output.P("}")
	output.P()
}

func generateServerStream(
	output *protogen.GeneratedFile,
	service *protogen.Service,
	method *protogen.Method,
) {
	name := serverStreamName(service, method)
	wrapper := unexported(service.GoName + method.GoName + "Server")
	output.P("type ", name, " interface {")
	output.P(
		"Context() ",
		output.QualifiedGoIdent(contextPackage.Ident("Context")),
	)
	if method.Desc.IsStreamingServer() {
		output.P("Send(*", method.Output.GoIdent, ") error")
	}
	if method.Desc.IsStreamingClient() {
		output.P("Recv() (*", method.Input.GoIdent, ", error)")
		if !method.Desc.IsStreamingServer() {
			output.P("SendAndClose(*", method.Output.GoIdent, ") error")
		}
	}
	output.P("}")
	output.P("type ", wrapper, " struct {")
	output.P(output.QualifiedGoIdent(grpcPackage.Ident("ServerStream")))
	output.P("}")
	if method.Desc.IsStreamingServer() {
		output.P(
			"func (stream *",
			wrapper,
			") Send(message *",
			method.Output.GoIdent,
			") error { return stream.SendMsg(message) }",
		)
	}
	if method.Desc.IsStreamingClient() {
		output.P(
			"func (stream *",
			wrapper,
			") Recv() (*",
			method.Input.GoIdent,
			", error) {",
		)
		output.P("message := new(", method.Input.GoIdent, ")")
		output.P("if err := stream.RecvMsg(message); err != nil { return nil, err }")
		output.P("return message, nil")
		output.P("}")
		if !method.Desc.IsStreamingServer() {
			output.P(
				"func (stream *",
				wrapper,
				") SendAndClose(message *",
				method.Output.GoIdent,
				") error { return stream.SendMsg(message) }",
			)
		}
	}
	output.P()
}

func generateClientStream(
	output *protogen.GeneratedFile,
	service *protogen.Service,
	method *protogen.Method,
) {
	name := clientStreamName(service, method)
	wrapper := unexported(service.GoName + method.GoName + "Client")
	output.P("type ", name, " interface {")
	output.P(output.QualifiedGoIdent(grpcPackage.Ident("ClientStream")))
	if method.Desc.IsStreamingClient() {
		output.P("Send(*", method.Input.GoIdent, ") error")
	}
	if method.Desc.IsStreamingServer() {
		output.P("Recv() (*", method.Output.GoIdent, ", error)")
	} else {
		output.P("CloseAndRecv() (*", method.Output.GoIdent, ", error)")
	}
	output.P("}")
	output.P("type ", wrapper, " struct {")
	output.P(output.QualifiedGoIdent(grpcPackage.Ident("ClientStream")))
	output.P("}")
	if method.Desc.IsStreamingClient() {
		output.P(
			"func (stream *",
			wrapper,
			") Send(message *",
			method.Input.GoIdent,
			") error { return stream.SendMsg(message) }",
		)
	}
	if method.Desc.IsStreamingServer() {
		output.P(
			"func (stream *",
			wrapper,
			") Recv() (*",
			method.Output.GoIdent,
			", error) {",
		)
		output.P("message := new(", method.Output.GoIdent, ")")
		output.P("if err := stream.RecvMsg(message); err != nil { return nil, err }")
		output.P("return message, nil")
		output.P("}")
	} else {
		output.P(
			"func (stream *",
			wrapper,
			") CloseAndRecv() (*",
			method.Output.GoIdent,
			", error) {",
		)
		output.P("if err := stream.CloseSend(); err != nil { return nil, err }")
		output.P("message := new(", method.Output.GoIdent, ")")
		output.P("if err := stream.RecvMsg(message); err != nil { return nil, err }")
		output.P("return message, nil")
		output.P("}")
	}
	output.P()
}
