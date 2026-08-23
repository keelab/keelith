package hertz

import (
	"bytes"
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/keelab/keelith/operation"
	khttp "github.com/keelab/keelith/transport/http"
	"google.golang.org/protobuf/proto"
)

// ProtoDecodeOption configures the shared Protobuf HTTP binding contract.
type ProtoDecodeOption = khttp.ProtoDecodeOption

// WithProtoBody enables strict protojson body decoding before path and query
// values are overlaid.
func WithProtoBody() ProtoDecodeOption {
	return khttp.WithProtoBody()
}

// WithProtoBodyField decodes the JSON body into one Protobuf field path.
func WithProtoBodyField(fieldPath string) ProtoDecodeOption {
	return khttp.WithProtoBodyField(fieldPath)
}

// WithUnknownProtoQuery allows query keys absent from the input message.
func WithUnknownProtoQuery() ProtoDecodeOption {
	return khttp.WithUnknownProtoQuery()
}

// WithProtoQueryDisabled rejects query parameters for body:"*" mappings.
func WithProtoQueryDisabled() ProtoDecodeOption {
	return khttp.WithProtoQueryDisabled()
}

// WithProtoPathField binds one Protobuf scalar field path from a generated
// Hertz router parameter.
func WithProtoPathField(fieldPath string, valueName string) ProtoDecodeOption {
	return khttp.WithProtoPathField(fieldPath, valueName)
}

// WithProtoPathTemplate binds assigned google.api.http resource-name paths
// using Keelith's shared standard transcoding contract.
func WithProtoPathTemplate(pathTemplate string) ProtoDecodeOption {
	return khttp.WithProtoPathTemplate(pathTemplate)
}

// DecodeProto adapts a Hertz request to Keelith's standard Protobuf HTTP
// binding. The standard and Hertz profiles therefore share the same body,
// path, query, scalar, and unknown-field behavior.
func DecodeProto[T proto.Message](
	factory func() T,
	optionList ...ProtoDecodeOption,
) Decoder {
	standard := khttp.DecodeProto(factory, optionList...)
	return func(request *app.RequestContext) (any, error) {
		if request == nil || factory == nil {
			return nil, fmt.Errorf("hertz profile: proto request or factory is nil")
		}
		descriptor := factory()
		if isNilProtoMessage(descriptor) {
			return nil, fmt.Errorf("hertz profile: proto factory returned nil")
		}
		uri := request.Request.URI()
		bridge := &nethttp.Request{
			Method: string(request.Request.Header.Method()),
			Header: make(nethttp.Header),
			URL: &url.URL{
				Path:     string(uri.Path()),
				RawQuery: string(uri.QueryString()),
			},
			Body: io.NopCloser(bytes.NewReader(request.Request.Body())),
		}
		if original := uri.PathOriginal(); len(original) > 0 {
			bridge.URL.RawPath = string(original)
		}
		request.Request.Header.VisitAll(func(key, value []byte) {
			bridge.Header.Add(string(key), string(value))
		})
		for _, parameter := range request.Params {
			bridge.SetPathValue(parameter.Key, parameter.Value)
		}
		return standard(bridge)
	}
}

// EncodeProto serializes a Protobuf response with canonical protojson names.
func EncodeProto(
	_ context.Context,
	request *app.RequestContext,
	response any,
) error {
	return encodeProtoResponseBody(request, response, "")
}

// EncodeProtoResponseBody returns a Hertz Encoder that projects one
// google.api.HttpRule.response_body field path.
func EncodeProtoResponseBody(responseBody string) Encoder {
	return func(
		_ context.Context,
		request *app.RequestContext,
		response any,
	) error {
		return encodeProtoResponseBody(request, response, responseBody)
	}
}

// EncodeProtoHTTPBody returns a Hertz Encoder that writes the selected
// google.api.HttpBody as a raw HTTP entity.
func EncodeProtoHTTPBody(responseBody string) Encoder {
	return func(
		_ context.Context,
		request *app.RequestContext,
		response any,
	) error {
		if request == nil {
			return fmt.Errorf("hertz profile: response context is nil")
		}
		contentType, payload, err := khttp.MarshalProtoHTTPBody(
			response,
			responseBody,
		)
		if err != nil {
			return err
		}
		request.Data(nethttp.StatusOK, contentType, payload)
		return nil
	}
}

func encodeProtoResponseBody(
	request *app.RequestContext,
	response any,
	responseBody string,
) error {
	if request == nil {
		return fmt.Errorf("hertz profile: response context is nil")
	}
	payload, err := khttp.MarshalProtoResponseBody(response, responseBody)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	request.Data(
		nethttp.StatusOK,
		"application/json; charset=utf-8",
		payload,
	)
	return nil
}

// NormalizeClientBaseURL validates a generated Hertz client's base URL using
// the same contract as the standard HTTP profile.
func NormalizeClientBaseURL(raw string) (string, error) {
	return khttp.NormalizeClientBaseURL(raw)
}

// NewProtoRequest creates a native Hertz request from an annotated Protobuf
// mapping while preserving Keelith's standard path/query/body semantics.
func NewProtoRequest(
	ctx context.Context,
	baseURL string,
	method string,
	pathTemplate string,
	input proto.Message,
	body string,
) (*protocol.Request, error) {
	standard, err := khttp.NewProtoRequest(
		ctx,
		baseURL,
		method,
		pathTemplate,
		input,
		body,
	)
	if err != nil {
		return nil, err
	}
	defer standard.Body.Close()
	payload, err := io.ReadAll(standard.Body)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: read proto request body: %v",
			ErrInvalidCall,
			err,
		)
	}
	request := new(protocol.Request)
	request.Header.SetMethod(standard.Method)
	request.SetRequestURI(standard.URL.String())
	request.Header.SetHost(standard.URL.Host)
	for key, values := range standard.Header {
		for index, value := range values {
			if index == 0 {
				request.Header.Set(key, value)
				continue
			}
			request.Header.Add(key, value)
		}
	}
	if len(payload) > 0 {
		request.SetBody(payload)
	}
	return request, nil
}

// InvokeProto invokes a generated Hertz request and strictly decodes
// protojson.
func InvokeProto[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
	factory func() T,
) (T, error) {
	return invokeProto(ctx, client, target, request, factory, "", false)
}

// InvokeProtoResponseBody invokes a generated Hertz request and reconstructs
// one google.api.HttpRule.response_body field path.
func InvokeProtoResponseBody[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
	factory func() T,
	responseBody string,
) (T, error) {
	return invokeProto(
		ctx,
		client,
		target,
		request,
		factory,
		responseBody,
		false,
	)
}

// InvokeProtoHTTPBody invokes a generated Hertz request and reconstructs one
// raw google.api.HttpBody response or selected nested field.
func InvokeProtoHTTPBody[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
	factory func() T,
	responseBody string,
) (T, error) {
	if request != nil {
		request.Header.Set("Accept", "*/*")
	}
	return invokeProto(
		ctx,
		client,
		target,
		request,
		factory,
		responseBody,
		true,
	)
}

func invokeProto[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
	factory func() T,
	responseBody string,
	rawHTTPBody bool,
) (T, error) {
	var zero T
	if client == nil || request == nil || factory == nil {
		return zero, fmt.Errorf(
			"%w: proto client, request, or factory is nil",
			ErrInvalidCall,
		)
	}
	response, err := client.Invoke(ctx, target, ClientCall{
		Request: request,
		Decode: func(
			_ context.Context,
			response *protocol.Response,
		) (any, error) {
			message := factory()
			if isNilProtoMessage(message) {
				return nil, fmt.Errorf(
					"%w: proto response factory returned nil",
					ErrInvalidCall,
				)
			}
			payload := response.Body()
			var decodeErr error
			if rawHTTPBody {
				decodeErr = khttp.UnmarshalProtoHTTPBody(
					payload,
					message,
					responseBody,
					string(response.Header.ContentType()),
				)
			} else {
				decodeErr = khttp.UnmarshalProtoResponseBody(
					bytes.TrimSpace(payload),
					message,
					responseBody,
				)
			}
			if decodeErr != nil {
				return nil, decodeErr
			}
			return message, nil
		},
	})
	if err != nil {
		return zero, err
	}
	typed, ok := response.(T)
	if !ok || isNilProtoMessage(typed) {
		return zero, fmt.Errorf(
			"%w: proto response type %T",
			ErrInvalidCall,
			response,
		)
	}
	return typed, nil
}

func isNilProtoMessage(message proto.Message) bool {
	if message == nil {
		return true
	}
	reflection := message.ProtoReflect()
	return !reflection.IsValid()
}
