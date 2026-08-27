package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/google/jsonschema-go/jsonschema"
	keelitherrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	httpTransport  = "mcp+http"
	stdioTransport = "mcp+stdio"
)

// ToolHints are advisory MCP client hints. They never replace authorization.
type ToolHints struct {
	ReadOnly    bool
	Destructive *bool
	Idempotent  bool
	OpenWorld   *bool
}

// Tool describes one immutable typed capability.
type Tool struct {
	Name        string
	Title       string
	Description string
	Hints       ToolHints
}

// Call is the transport-neutral request passed through Keelith middleware.
type Call[In any] struct {
	Name      string
	Arguments In
}

// ToolHandler executes one typed tool invocation.
type ToolHandler[In, Out any] func(context.Context, Call[In]) (Out, error)

// Server owns one immutable MCP tool registry.
type Server struct {
	implementation  Implementation
	sdk             *sdk.Server
	chain           middleware.Middleware
	maxMessageBytes int64
	maxResultBytes  int64

	mu           sync.Mutex
	frozen       bool
	stdioRunning bool
	toolNames    map[string]struct{}
}

// New validates an MCP implementation and constructs an empty server.
func New(implementation Implementation, options Options) (*Server, error) {
	normalized, err := normalizeOptions(implementation, options)
	if err != nil {
		return nil, err
	}
	chain := middleware.Chain()
	if normalized.middleware != nil {
		chain = normalized.middleware.Chain()
	}
	protocolServer := sdk.NewServer(
		&sdk.Implementation{
			Name:    normalized.implementation.Name,
			Title:   normalized.implementation.Title,
			Version: normalized.implementation.Version,
		},
		&sdk.ServerOptions{
			Instructions: normalized.instructions,
			PageSize:     normalized.pageSize,
			Capabilities: &sdk.ServerCapabilities{
				Tools: &sdk.ToolCapabilities{},
			},
		},
	)
	return &Server{
		implementation:  normalized.implementation,
		sdk:             protocolServer,
		chain:           chain,
		maxMessageBytes: normalized.maxMessageBytes,
		maxResultBytes:  normalized.maxResultBytes,
		toolNames:       make(map[string]struct{}),
	}, nil
}

// AddTool registers one typed tool before the server registry is frozen.
func AddTool[In, Out any](
	server *Server,
	tool Tool,
	handler ToolHandler[In, Out],
) (err error) {
	if server == nil || server.sdk == nil {
		return fmt.Errorf("%w: server is nil", ErrInvalidTool)
	}
	if handler == nil {
		return fmt.Errorf("%w: handler is nil", ErrInvalidTool)
	}
	if err := validateTool(tool); err != nil {
		return err
	}
	inputSchema, inputResolved, err := schemaFor[In]()
	if err != nil {
		return fmt.Errorf("%w: input schema is invalid", ErrInvalidTool)
	}
	outputSchema, outputResolved, err := schemaFor[Out]()
	if err != nil {
		return fmt.Errorf("%w: output schema is invalid", ErrInvalidTool)
	}
	httpOperation, err := operation.New(
		httpTransport,
		server.implementation.Name,
		tool.Name,
		operation.KindUnary,
	)
	if err != nil {
		return fmt.Errorf("%w: http operation is invalid", ErrInvalidTool)
	}
	stdioOperation, err := operation.New(
		stdioTransport,
		server.implementation.Name,
		tool.Name,
		operation.KindUnary,
	)
	if err != nil {
		return fmt.Errorf("%w: stdio operation is invalid", ErrInvalidTool)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.frozen {
		return ErrRegistryFrozen
	}
	if _, duplicate := server.toolNames[tool.Name]; duplicate {
		return fmt.Errorf("%w: %q", ErrDuplicateTool, tool.Name)
	}
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: SDK registration failed", ErrInvalidTool)
		}
	}()

	wrapped := buildToolHandler(
		server,
		tool,
		handler,
		inputResolved,
		outputResolved,
		httpOperation,
		stdioOperation,
	)
	server.sdk.AddTool(&sdk.Tool{
		Name:         tool.Name,
		Title:        tool.Title,
		Description:  tool.Description,
		InputSchema:  inputSchema,
		OutputSchema: outputSchema,
		Annotations: &sdk.ToolAnnotations{
			ReadOnlyHint:    tool.Hints.ReadOnly,
			DestructiveHint: cloneBool(tool.Hints.Destructive),
			IdempotentHint:  tool.Hints.Idempotent,
			OpenWorldHint:   cloneBool(tool.Hints.OpenWorld),
		},
	}, wrapped)
	server.toolNames[tool.Name] = struct{}{}
	return nil
}

func buildToolHandler[In, Out any](
	server *Server,
	tool Tool,
	typedHandler ToolHandler[In, Out],
	inputSchema *jsonschema.Resolved,
	outputSchema *jsonschema.Resolved,
	httpOperation operation.Operation,
	stdioOperation operation.Operation,
) sdk.ToolHandler {
	final := middleware.Handler(func(ctx context.Context, request any) (any, error) {
		call, ok := request.(Call[In])
		if !ok {
			return nil, keelitherrors.New(
				500,
				"INTERNAL",
				"internal server error",
			)
		}
		return typedHandler(ctx, call)
	})
	chain := server.chain(final)
	return func(
		ctx context.Context,
		request *sdk.CallToolRequest,
	) (result *sdk.CallToolResult, err error) {
		defer func() {
			if recover() != nil {
				result = failureResult(nil)
				err = nil
			}
		}()
		facts, ok := callFactsFromContext(ctx)
		if !ok {
			return failureResult(nil), nil
		}
		target := httpOperation
		if facts.transport == stdioTransport {
			target = stdioOperation
		} else if facts.transport != httpTransport {
			return failureResult(nil), nil
		}
		requestInfoOptions := make([]operation.RequestInfoOption, 0, 1)
		if facts.hasPeer {
			requestInfoOptions = append(
				requestInfoOptions,
				operation.WithPeer(facts.peer),
			)
		}
		info, infoErr := operation.NewRequestInfo(target, requestInfoOptions...)
		if infoErr != nil {
			return failureResult(nil), nil
		}
		ctx = operation.WithRequestInfo(ctx, info)

		var raw json.RawMessage
		if request != nil && request.Params != nil {
			raw = request.Params.Arguments
		}
		input, inputErr := decodeInput[In](
			raw,
			inputSchema,
			server.maxMessageBytes,
		)
		if inputErr != nil {
			return invalidArgumentsResult(), nil
		}
		response, invokeErr := chain(ctx, Call[In]{
			Name:      tool.Name,
			Arguments: input,
		})
		if invokeErr != nil {
			return failureResult(invokeErr), nil
		}
		output, ok := response.(Out)
		if !ok {
			return failureResult(nil), nil
		}
		return successResult(output, outputSchema, server.maxResultBytes), nil
	}
}

func schemaFor[T any]() (*jsonschema.Schema, *jsonschema.Resolved, error) {
	schema, err := jsonschema.For[T](nil)
	if err != nil || schema == nil || schema.Type != "object" {
		return nil, nil, ErrInvalidTool
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{
		ValidateDefaults: true,
	})
	if err != nil {
		return nil, nil, err
	}
	return schema, resolved, nil
}

func decodeInput[In any](
	raw json.RawMessage,
	schema *jsonschema.Resolved,
	maxBytes int64,
) (In, error) {
	var zero In
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	if int64(len(raw)) > maxBytes {
		return zero, ErrMessageTooLarge
	}
	document, err := decodejson(raw)
	if err != nil || schema.Validate(document) != nil {
		return zero, ErrInvalidTool
	}
	if err := json.Unmarshal(raw, &zero); err != nil {
		return zero, ErrInvalidTool
	}
	return zero, nil
}

func successResult[Out any](
	output Out,
	schema *jsonschema.Resolved,
	maxBytes int64,
) *sdk.CallToolResult {
	encoded, err := json.Marshal(output)
	if err != nil || int64(len(encoded)) > maxBytes {
		return failureResult(nil)
	}
	document, err := decodejson(encoded)
	if err != nil || schema.Validate(document) != nil {
		return failureResult(nil)
	}
	snapshot := append(json.RawMessage(nil), encoded...)
	return &sdk.CallToolResult{
		Content: []sdk.Content{
			&sdk.TextContent{Text: string(snapshot)},
		},
		StructuredContent: snapshot,
	}
}

func decodejson(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errorsForTrailingjson(err)
	}
	return document, nil
}

func errorsForTrailingjson(err error) error {
	if err == nil {
		return ErrInvalidTool
	}
	return err
}

type errorEnvelope struct {
	Code    int32  `json:"code"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

func invalidArgumentsResult() *sdk.CallToolResult {
	return envelopeResult(errorEnvelope{
		Code:    400,
		Reason:  "INVALid_ARGUMENT",
		Message: "tool arguments are invalid",
	})
}

func failureResult(err error) *sdk.CallToolResult {
	envelope := errorEnvelope{
		Code:    500,
		Reason:  "INTERNAL",
		Message: "internal server error",
	}
	var frameworkError *keelitherrors.Error
	if stderrors.As(err, &frameworkError) && safeFrameworkError(frameworkError) {
		envelope = errorEnvelope{
			Code:    frameworkError.Code(),
			Reason:  frameworkError.Reason(),
			Message: frameworkError.Message(),
		}
	}
	return envelopeResult(envelope)
}

func safeFrameworkError(err *keelitherrors.Error) bool {
	return err != nil &&
		err.Code() >= 400 && err.Code() <= 599 &&
		validReason(err.Reason()) &&
		validText(err.Message(), maxDescriptionBytes, true)
}

func validReason(reason string) bool {
	if reason == "" || len(reason) > 128 {
		return false
	}
	for _, character := range reason {
		if character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' {
			continue
		}
		return false
	}
	return true
}

func envelopeResult(envelope errorEnvelope) *sdk.CallToolResult {
	encoded, err := json.Marshal(envelope)
	if err != nil {
		encoded = []byte(`{"code":500,"reason":"INTERNAL","message":"internal server error"}`)
	}
	return &sdk.CallToolResult{
		IsError: true,
		Content: []sdk.Content{
			&sdk.TextContent{Text: string(encoded)},
		},
	}
}

func validateTool(tool Tool) error {
	if !validIdentifier(tool.Name) || len(tool.Name) > 128 {
		return fmt.Errorf("%w: name is empty or malformed", ErrInvalidTool)
	}
	if tool.Title != "" && !validText(tool.Title, maxTitleBytes, false) {
		return fmt.Errorf("%w: title is malformed", ErrInvalidTool)
	}
	if tool.Description == "" ||
		!validText(tool.Description, maxDescriptionBytes, true) {
		return fmt.Errorf("%w: description is empty or malformed", ErrInvalidTool)
	}
	if tool.Hints.ReadOnly &&
		tool.Hints.Destructive != nil && *tool.Hints.Destructive {
		return fmt.Errorf(
			"%w: a read-only tool cannot be destructive",
			ErrInvalidTool,
		)
	}
	return nil
}

func cloneBool(source *bool) *bool {
	if source == nil {
		return nil
	}
	clone := *source
	return &clone
}

func isNilStream(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
