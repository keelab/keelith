package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"
	"reflect"
	"time"

	kclient "github.com/keelab/keelith/client"
	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/selector"
	"go.opentelemetry.io/otel/propagation"
)

const defaultClientMaxHeaderBytes = 1 * 1024 * 1024

// ClientCall contains a prepared HTTP request and typed-friendly decoder.
type ClientCall struct {
	Request   *nethttp.Request
	Decode    func(context.Context, *nethttp.Response) (any, error)
	Streaming bool
}

// ClientOption configures a Client.
type ClientOption interface {
	applyClient(*clientOptions) error
}

type clientOptionFunc func(*clientOptions) error

func (f clientOptionFunc) applyClient(options *clientOptions) error {
	return f(options)
}

type clientOptions struct {
	bundle           *middleware.Bundle
	metadataPolicy   metadata.Policy
	maxResponseBytes int64
	maxHeaderBytes   int
	propagator       propagation.TextMapPropagator
	tlsConfig        *tls.Config
	picker           kclient.Picker
}

// WithClientMiddleware configures the immutable outbound middleware Bundle.
func WithClientMiddleware(bundle *middleware.Bundle) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if bundle == nil {
			return fmt.Errorf("middleware bundle is nil")
		}
		options.bundle = bundle
		return nil
	})
}

// WithClientMetadataPolicy configures HTTP header propagation.
func WithClientMetadataPolicy(policy metadata.Policy) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		options.metadataPolicy = policy
		return nil
	})
}

// WithClientPropagator configures distributed context injection.
func WithClientPropagator(
	propagator propagation.TextMapPropagator,
) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if propagator == nil {
			return fmt.Errorf("propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithClientMaxResponseBytes sets the decoded response body budget.
func WithClientMaxResponseBytes(maxBytes int64) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max response bytes must be positive")
		}
		options.maxResponseBytes = maxBytes
		return nil
	})
}

// WithClientMaxHeaderBytes sets the response header budget.
func WithClientMaxHeaderBytes(maxBytes int) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max header bytes must be positive")
		}
		options.maxHeaderBytes = maxBytes
		return nil
	})
}

// WithClientTLS installs a cloned TLS/mTLS profile on a standard Transport.
func WithClientTLS(config *tls.Config) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if config == nil {
			return fmt.Errorf("TLS config is nil")
		}
		if config.MinVersion < tls.VersionTLS12 {
			return fmt.Errorf("TLS minimum version must be 1.2 or newer")
		}
		options.tlsConfig = config.Clone()
		return nil
	})
}

// WithClientPicker enables per-attempt service discovery and node feedback.
func WithClientPicker(picker kclient.Picker) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if isNilPicker(picker) {
			return fmt.Errorf("picker is nil")
		}
		options.picker = picker
		return nil
	})
}

// Client executes HTTP calls through Keelith's outbound invocation model.
type Client struct {
	client           *nethttp.Client
	bundle           *middleware.Bundle
	metadataPolicy   metadata.Policy
	maxResponseBytes int64
	maxHeaderBytes   int
	propagator       propagation.TextMapPropagator
	picker           kclient.Picker
}

// NewClient constructs a Client around an explicit standard HTTP client.
func NewClient(client *nethttp.Client, optionList ...ClientOption) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: HTTP client is nil", ErrInvalidOption)
	}
	options := clientOptions{
		maxResponseBytes: defaultMaxResponseBytes,
		maxHeaderBytes:   defaultClientMaxHeaderBytes,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: client option %d is nil", ErrInvalidOption, index)
		}
		if err := option.applyClient(&options); err != nil {
			return nil, fmt.Errorf("%w: client option %d: %w", ErrInvalidOption, index, err)
		}
	}
	if options.tlsConfig != nil {
		cloned := *client
		switch transport := client.Transport.(type) {
		case nil:
			defaultTransport, ok := nethttp.DefaultTransport.(*nethttp.Transport)
			if !ok {
				return nil, fmt.Errorf(
					"%w: default HTTP transport is not standard",
					ErrInvalidOption,
				)
			}
			transportClone := defaultTransport.Clone()
			transportClone.TLSClientConfig = options.tlsConfig
			cloned.Transport = transportClone
		case *nethttp.Transport:
			if transport == nil {
				return nil, fmt.Errorf(
					"%w: HTTP transport is nil",
					ErrInvalidOption,
				)
			}
			transportClone := transport.Clone()
			transportClone.TLSClientConfig = options.tlsConfig
			cloned.Transport = transportClone
		default:
			return nil, fmt.Errorf(
				"%w: client TLS requires *http.Transport, got %T",
				ErrInvalidOption,
				client.Transport,
			)
		}
		client = &cloned
	}
	return &Client{
		client:           client,
		bundle:           options.bundle,
		metadataPolicy:   options.metadataPolicy,
		maxResponseBytes: options.maxResponseBytes,
		maxHeaderBytes:   options.maxHeaderBytes,
		propagator:       options.propagator,
		picker:           options.picker,
	}, nil
}

// Invoke executes call through outbound middleware.
func (client *Client) Invoke(
	ctx context.Context,
	target operation.Operation,
	call ClientCall,
) (any, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if call.Request == nil || call.Request.URL == nil || call.Decode == nil {
		return nil, ErrInvalidCall
	}
	if target.Transport() != "http" {
		return nil, fmt.Errorf(
			"%w: operation transport %q is not http",
			ErrInvalidCall,
			target.Transport(),
		)
	}
	network := call.Request.URL.Scheme
	address := call.Request.URL.Host
	if client.picker != nil {
		network = ""
		address = ""
	}
	requestInfo, err := newRequestInfo(target, network, address)
	if err != nil {
		return nil, fmt.Errorf("%w: request info: %w", ErrInvalidCall, err)
	}
	ctx = operation.WithRequestInfo(ctx, requestInfo)
	handler := middleware.Handler(client.invoke)
	if client.bundle != nil {
		handler = client.bundle.Chain()(handler)
	}
	return handler(ctx, call)
}

func (client *Client) invoke(
	ctx context.Context,
	request any,
) (responseValue any, resultErr error) {
	call, ok := request.(ClientCall)
	if !ok {
		return nil, fmt.Errorf("%w: request type %T", ErrInvalidCall, request)
	}
	outboundRequest := call.Request.Clone(ctx)
	if client.picker != nil {
		target, exists := operation.FromContext(ctx)
		if !exists {
			return nil, fmt.Errorf("%w: operation is missing", ErrInvalidCall)
		}
		node, done, err := client.picker.Pick(ctx, target)
		if err != nil {
			return nil, dependencyFailure(err)
		}
		started := time.Now()
		defer func() {
			recovered := recover()
			feedbackErr := resultErr
			if recovered != nil {
				feedbackErr = errors.New("http transport: invocation panic")
			}
			done(selector.Result{
				Latency:  time.Since(started),
				Error:    feedbackErr,
				Canceled: errors.Is(feedbackErr, context.Canceled),
				Retried:  operation.AttemptFromContext(ctx) > 1,
			})
			if recovered != nil {
				panic(recovered)
			}
		}()

		endpoint, err := selectedEndpoint(node)
		if err != nil {
			return nil, failure.MarkTransport(fmt.Errorf(
				"%w: selected endpoint: %w",
				ErrInvalidCall,
				err,
			))
		}
		requestInfo, err := newRequestInfo(
			target,
			endpoint.Scheme,
			endpoint.Host,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: selected request info: %w", ErrInvalidCall, err)
		}
		ctx = operation.WithRequestInfo(ctx, requestInfo)
		outboundRequest = call.Request.Clone(ctx)
		outboundRequest.URL.Scheme = endpoint.Scheme
		outboundRequest.URL.Host = endpoint.Host
	}
	if client.propagator != nil {
		client.propagator.Inject(
			ctx,
			propagation.HeaderCarrier(outboundRequest.Header),
		)
	}
	if outbound, ok := metadata.Outbound(ctx); ok {
		if err := client.metadataPolicy.Inject(
			outbound,
			httpHeaderCarrier(outboundRequest.Header),
		); err != nil {
			return nil, err
		}
	}
	response, err := client.client.Do(outboundRequest)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = response.Body.Close()
	}()

	if headerSize(response.Header) > int64(client.maxHeaderBytes) {
		return nil, ErrHeaderTooLarge
	}
	if !call.Streaming {
		if response.ContentLength > client.maxResponseBytes {
			return nil, ErrResponseTooLarge
		}
		response.Body = &limitedReadCloser{
			reader:    response.Body,
			remaining: client.maxResponseBytes,
		}
	}
	inbound, err := client.metadataPolicy.Extract(httpHeaderCarrier(response.Header))
	if err != nil {
		return nil, err
	}
	decodeContext := metadata.WithInbound(ctx, inbound)
	if response.StatusCode >= nethttp.StatusBadRequest {
		return nil, decodeHTTPError(response)
	}
	return call.Decode(decodeContext, response)
}

func dependencyFailure(err error) error {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return failure.MarkTransport(err)
}

func selectedEndpoint(node selector.Node) (*url.URL, error) {
	endpoint, err := url.Parse(node.Endpoint())
	if err != nil ||
		endpoint.Scheme != "http" && endpoint.Scheme != "https" ||
		endpoint.Host == "" ||
		endpoint.User != nil ||
		endpoint.Opaque != "" ||
		endpoint.RawQuery != "" ||
		endpoint.Fragment != "" ||
		endpoint.RawPath != "" ||
		endpoint.Path != "" && endpoint.Path != "/" {
		return nil, fmt.Errorf("unsafe HTTP endpoint %q", node.Endpoint())
	}
	return endpoint, nil
}

func isNilPicker(picker kclient.Picker) bool {
	if picker == nil {
		return true
	}
	value := reflect.ValueOf(picker)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// InvokeJSON decodes a successful JSON response as T.
func InvokeJSON[T any](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *nethttp.Request,
) (T, error) {
	var zero T
	if client == nil {
		return zero, fmt.Errorf("%w: client is nil", ErrInvalidCall)
	}
	response, err := client.Invoke(ctx, target, ClientCall{
		Request: request,
		Decode: func(_ context.Context, response *nethttp.Response) (any, error) {
			var value T
			err := json.NewDecoder(response.Body).Decode(&value)
			return value, err
		},
	})
	if err != nil {
		return zero, err
	}
	typed, ok := response.(T)
	if !ok {
		return zero, fmt.Errorf("%w: response type %T", ErrInvalidCall, response)
	}
	return typed, nil
}

func decodeHTTPError(response *nethttp.Response) error {
	var envelope ErrorEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return kerrors.New(
			int32(response.StatusCode),
			"HTTP_"+nethttp.StatusText(response.StatusCode),
			nethttp.StatusText(response.StatusCode),
		)
	}
	if envelope.Code == 0 {
		envelope.Code = int32(response.StatusCode)
	}
	if envelope.Reason == "" {
		envelope.Reason = "HTTP_" + nethttp.StatusText(response.StatusCode)
	}
	if envelope.Message == "" {
		envelope.Message = nethttp.StatusText(response.StatusCode)
	}
	return kerrors.New(
		envelope.Code,
		envelope.Reason,
		envelope.Message,
		kerrors.WithMetadata(envelope.Metadata),
	)
}

type limitedReadCloser struct {
	reader    io.ReadCloser
	remaining int64
	exceeded  bool
}

func (reader *limitedReadCloser) Read(destination []byte) (int, error) {
	if reader.exceeded {
		return 0, ErrResponseTooLarge
	}
	if reader.remaining > 0 {
		if int64(len(destination)) > reader.remaining {
			destination = destination[:reader.remaining]
		}
		count, err := reader.reader.Read(destination)
		reader.remaining -= int64(count)
		return count, err
	}
	var probe [1]byte
	count, err := reader.reader.Read(probe[:])
	if count > 0 {
		reader.exceeded = true
		return 0, ErrResponseTooLarge
	}
	return 0, err
}

func (reader *limitedReadCloser) Close() error {
	return reader.reader.Close()
}
