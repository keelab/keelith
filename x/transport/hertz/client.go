package hertz

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	herrors "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/protocol"
	kclient "github.com/keelab/keelith/client"
	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/selector"
	"go.opentelemetry.io/otel/propagation"
)

const defaultClientName = "keelith.experimental.hertz.client"

var (
	// ErrInvalidCall reports an incomplete or unsafe unary client call.
	ErrInvalidCall = errors.New("hertz profile: invalid client call")
	// ErrClientClosed reports an invocation after Shutdown.
	ErrClientClosed = errors.New("hertz profile: client is closed")
)

// ClientCall contains an immutable request template and response decoder.
//
// Invoke copies Request for every middleware attempt. Streaming permits a
// server-stream Operation and keeps Decode, Middleware, Picker feedback, and
// the native response body alive until the stream reaches a terminal state.
type ClientCall struct {
	Request   *protocol.Request
	Decode    func(context.Context, *protocol.Response) (any, error)
	Streaming bool
}

// ClientOption configures a Hertz unary Client.
type ClientOption interface {
	applyClient(*clientOptions) error
}

type clientOptionFunc func(*clientOptions) error

func (function clientOptionFunc) applyClient(options *clientOptions) error {
	return function(options)
}

type clientOptions struct {
	name             string
	bundle           *middleware.Bundle
	metadataPolicy   metadata.Policy
	propagator       propagation.TextMapPropagator
	picker           kclient.Picker
	maxHeaderBytes   int
	maxRequestBytes  int
	maxResponseBytes int
	nativeOptions    []hconfig.ClientOption
}

// WithClientName sets Hertz's bounded User-Agent identity.
func WithClientName(name string) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		normalized := strings.TrimSpace(name)
		if normalized == "" || normalized != name || len(normalized) > 128 {
			return fmt.Errorf("client name is malformed")
		}
		options.name = normalized
		return nil
	})
}

// WithClientMiddleware configures the shared outbound Middleware Bundle.
func WithClientMiddleware(bundle *middleware.Bundle) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if bundle == nil {
			return fmt.Errorf("client middleware bundle is nil")
		}
		options.bundle = bundle
		return nil
	})
}

// WithClientMetadataPolicy configures default-deny request/response headers.
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
			return fmt.Errorf("client propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithClientPicker enables Keelith per-attempt discovery and feedback.
func WithClientPicker(picker kclient.Picker) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if isNilClientPicker(picker) {
			return fmt.Errorf("client picker is nil")
		}
		options.picker = picker
		return nil
	})
}

// WithClientMaxHeaderBytes limits both injected request and parsed response
// header blocks.
func WithClientMaxHeaderBytes(maxBytes int) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("client max header bytes must be positive")
		}
		options.maxHeaderBytes = maxBytes
		return nil
	})
}

// WithClientMaxRequestBytes limits copied unary request bodies.
func WithClientMaxRequestBytes(maxBytes int) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("client max request bytes must be positive")
		}
		options.maxRequestBytes = maxBytes
		return nil
	})
}

// WithClientMaxResponseBytes limits the Hertz parser and response decoder.
func WithClientMaxResponseBytes(maxBytes int) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maxBytes < 256 {
			return fmt.Errorf(
				"client max response bytes must be at least 256",
			)
		}
		options.maxResponseBytes = maxBytes
		return nil
	})
}

// WithClientTLS installs a cloned TLS 1.2+ profile on Hertz HostClients.
func WithClientTLS(config *tls.Config) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if config == nil || config.MinVersion < tls.VersionTLS12 {
			return fmt.Errorf(
				"client TLS minimum version must be 1.2 or newer",
			)
		}
		options.nativeOptions = append(
			options.nativeOptions,
			hclient.WithTLSConfig(config.Clone()),
		)
		return nil
	})
}

// WithClientDialTimeout sets connection establishment timeout.
func WithClientDialTimeout(timeout time.Duration) ClientOption {
	return clientDurationOption(
		"dial timeout",
		timeout,
		hclient.WithDialTimeout,
	)
}

// WithClientReadTimeout bounds full response reads.
func WithClientReadTimeout(timeout time.Duration) ClientOption {
	return clientDurationOption(
		"read timeout",
		timeout,
		hclient.WithClientReadTimeout,
	)
}

// WithClientWriteTimeout bounds full request writes.
func WithClientWriteTimeout(timeout time.Duration) ClientOption {
	return clientDurationOption(
		"write timeout",
		timeout,
		hclient.WithWriteTimeout,
	)
}

// WithClientMaxIdleConnDuration bounds idle keep-alive lifetime.
func WithClientMaxIdleConnDuration(timeout time.Duration) ClientOption {
	return clientDurationOption(
		"max idle connection duration",
		timeout,
		hclient.WithMaxIdleConnDuration,
	)
}

// WithClientMaxConnWaitTimeout bounds waiting for a free pooled connection.
func WithClientMaxConnWaitTimeout(timeout time.Duration) ClientOption {
	return clientDurationOption(
		"max connection wait timeout",
		timeout,
		hclient.WithMaxConnWaitTimeout,
	)
}

// WithClientMaxConnsPerHost bounds concurrent connections per authority.
func WithClientMaxConnsPerHost(maxConnections int) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if maxConnections <= 0 || maxConnections > 1_000_000 {
			return fmt.Errorf(
				"client max connections per host is invalid",
			)
		}
		options.nativeOptions = append(
			options.nativeOptions,
			hclient.WithMaxConnsPerHost(maxConnections),
		)
		return nil
	})
}

func clientDurationOption(
	name string,
	timeout time.Duration,
	option func(time.Duration) hconfig.ClientOption,
) ClientOption {
	return clientOptionFunc(func(options *clientOptions) error {
		if timeout <= 0 || timeout > 24*time.Hour {
			return fmt.Errorf("client %s is invalid", name)
		}
		options.nativeOptions = append(
			options.nativeOptions,
			option(timeout),
		)
		return nil
	})
}

// Client owns one native Hertz client and applies Keelith unary semantics.
type Client struct {
	name             string
	native           *hclient.Client
	bundle           *middleware.Bundle
	metadataPolicy   metadata.Policy
	propagator       propagation.TextMapPropagator
	picker           kclient.Picker
	maxHeaderBytes   int
	maxRequestBytes  int
	maxResponseBytes int
	closed           atomic.Bool
}

// NewClient constructs an owned Hertz unary client.
//
// Retry remains exclusively controlled by Keelith Method Policy; the native
// Hertz retry layer is deliberately not exposed by this profile.
func NewClient(optionList ...ClientOption) (*Client, error) {
	settings := clientOptions{
		name:             defaultClientName,
		maxHeaderBytes:   defaultMaxHeaderBytes,
		maxRequestBytes:  defaultMaxRequestBytes,
		maxResponseBytes: defaultMaxResponseBytes,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: client option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.applyClient(&settings); err != nil {
			return nil, fmt.Errorf(
				"%w: client option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	nativeOptions := append(
		[]hconfig.ClientOption(nil),
		settings.nativeOptions...,
	)
	nativeOptions = append(
		nativeOptions,
		hclient.WithName(settings.name),
		hconfig.ClientOption{F: func(options *hconfig.ClientOptions) {
			options.MaxResponseBodySize = settings.maxResponseBytes
		}},
	)
	native, err := hclient.NewClient(nativeOptions...)
	if err != nil {
		return nil, fmt.Errorf("%w: native client: %w", ErrInvalidOption, err)
	}
	return &Client{
		name:             settings.name,
		native:           native,
		bundle:           settings.bundle,
		metadataPolicy:   settings.metadataPolicy,
		propagator:       settings.propagator,
		picker:           settings.picker,
		maxHeaderBytes:   settings.maxHeaderBytes,
		maxRequestBytes:  settings.maxRequestBytes,
		maxResponseBytes: settings.maxResponseBytes,
	}, nil
}

// Name returns the stable client diagnostic identity.
func (client *Client) Name() string {
	if client == nil {
		return ""
	}
	return client.name
}

// Invoke executes one unary call through Keelith outbound middleware.
func (client *Client) Invoke(
	ctx context.Context,
	target operation.Operation,
	call ClientCall,
) (any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidCall)
	}
	if client == nil || client.native == nil || client.closed.Load() {
		return nil, ErrClientClosed
	}
	endpoint, err := requestEndpoint(call.Request)
	validKind := !call.Streaming && target.Kind() == operation.KindUnary ||
		call.Streaming && target.Kind() == operation.KindServerStream
	if err != nil || call.Decode == nil || !validKind ||
		call.Request.IsBodyStream() ||
		target.Transport() != "http" {
		return nil, fmt.Errorf("%w: unary call is incomplete", ErrInvalidCall)
	}
	network := endpoint.Scheme
	address := endpoint.Host
	if client.picker != nil {
		network = ""
		address = ""
	}
	info, err := clientRequestInfo(target, network, address)
	if err != nil {
		return nil, fmt.Errorf("%w: request info: %w", ErrInvalidCall, err)
	}
	ctx = operation.WithRequestInfo(ctx, info)
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
		return nil, fmt.Errorf(
			"%w: request type %T",
			ErrInvalidCall,
			request,
		)
	}
	if len(call.Request.Body()) > client.maxRequestBytes {
		return nil, ErrRequestTooLarge
	}
	outbound := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(outbound)
	call.Request.CopyTo(outbound)

	if client.picker != nil {
		target, exists := operation.FromContext(ctx)
		if !exists {
			return nil, fmt.Errorf(
				"%w: operation is missing",
				ErrInvalidCall,
			)
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
				feedbackErr = errors.New(
					"hertz profile: client invocation panic",
				)
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

		endpoint, err := selectedClientEndpoint(node)
		if err != nil {
			return nil, failure.MarkTransport(fmt.Errorf(
				"%w: selected endpoint: %w",
				ErrInvalidCall,
				err,
			))
		}
		info, err := clientRequestInfo(
			target,
			endpoint.Scheme,
			endpoint.Host,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: selected request info: %w",
				ErrInvalidCall,
				err,
			)
		}
		ctx = operation.WithRequestInfo(ctx, info)
		outbound.URI().SetScheme(endpoint.Scheme)
		outbound.URI().SetHost(endpoint.Host)
		outbound.Header.SetHost(endpoint.Host)
	}

	if client.propagator != nil {
		client.propagator.Inject(
			ctx,
			hertzPropagationCarrier{header: &outbound.Header},
		)
	}
	if outboundMetadata, ok := metadata.Outbound(ctx); ok {
		if err := client.metadataPolicy.Inject(
			outboundMetadata,
			hertzMetadataCarrier{header: &outbound.Header},
		); err != nil {
			return nil, err
		}
	}
	if requestHeaderBytes(&outbound.Header) >
		int64(client.maxHeaderBytes) {
		return nil, ErrHeaderTooLarge
	}

	response := protocol.AcquireResponse()
	defer protocol.ReleaseResponse(response)
	if err := client.native.Do(ctx, outbound, response); err != nil {
		if errors.Is(err, herrors.ErrBodyTooLarge) {
			return nil, ErrResponseTooLarge
		}
		return nil, err
	}
	if responseHeaderBytes(&response.Header) >
		int64(client.maxHeaderBytes) {
		return nil, ErrHeaderTooLarge
	}
	if !call.Streaming &&
		len(response.Body()) > client.maxResponseBytes {
		return nil, ErrResponseTooLarge
	}
	inbound, err := client.metadataPolicy.Extract(
		hertzResponseMetadataCarrier{header: &response.Header},
	)
	if err != nil {
		return nil, err
	}
	decodeContext := metadata.WithInbound(ctx, inbound)
	if response.StatusCode() >= 400 {
		return nil, decodeClientError(response)
	}
	return call.Decode(decodeContext, response)
}

// Shutdown rejects new calls and closes idle native connections.
//
// In-flight calls retain their contexts and complete or cancel normally.
func (client *Client) Shutdown(ctx context.Context) error {
	if client == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	client.closed.Store(true)
	client.native.CloseIdleConnections()
	return nil
}

// InvokeJSON decodes a successful JSON response as T.
func InvokeJSON[T any](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *protocol.Request,
) (T, error) {
	var zero T
	if client == nil {
		return zero, fmt.Errorf("%w: client is nil", ErrInvalidCall)
	}
	value, err := client.Invoke(ctx, target, ClientCall{
		Request: request,
		Decode: func(
			_ context.Context,
			response *protocol.Response,
		) (any, error) {
			var decoded T
			decoder := json.NewDecoder(bytes.NewReader(response.Body()))
			if err := decoder.Decode(&decoded); err != nil {
				return nil, err
			}
			var trailing any
			if err := decoder.Decode(&trailing); err != io.EOF {
				if err == nil {
					return nil, errors.New(
						"hertz profile: response contains multiple JSON values",
					)
				}
				return nil, err
			}
			return decoded, nil
		},
	})
	if err != nil {
		return zero, err
	}
	typed, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf(
			"%w: response type %T",
			ErrInvalidCall,
			value,
		)
	}
	return typed, nil
}

func requestEndpoint(request *protocol.Request) (*url.URL, error) {
	if request == nil {
		return nil, ErrInvalidCall
	}
	uri := request.URI()
	if len(uri.Username()) != 0 || len(uri.Password()) != 0 {
		return nil, ErrInvalidCall
	}
	return parseClientURL(uri.String(), true)
}

func selectedClientEndpoint(node selector.Node) (*url.URL, error) {
	endpoint, err := parseClientURL(node.Endpoint(), false)
	if err != nil {
		return nil, fmt.Errorf(
			"unsafe HTTP endpoint %q",
			node.Endpoint(),
		)
	}
	return endpoint, nil
}

func parseClientURL(raw string, allowPath bool) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil ||
		endpoint.Scheme != "http" && endpoint.Scheme != "https" ||
		endpoint.Host == "" ||
		endpoint.User != nil ||
		endpoint.Opaque != "" ||
		endpoint.Fragment != "" {
		return nil, ErrInvalidCall
	}
	if !allowPath &&
		(endpoint.RawQuery != "" ||
			endpoint.RawPath != "" ||
			endpoint.Path != "" && endpoint.Path != "/") {
		return nil, ErrInvalidCall
	}
	return endpoint, nil
}

func clientRequestInfo(
	target operation.Operation,
	network string,
	address string,
) (operation.RequestInfo, error) {
	if network == "" && address == "" {
		return operation.NewRequestInfo(target)
	}
	peer, err := operation.NewPeer(network, address)
	if err != nil {
		return operation.RequestInfo{}, err
	}
	return operation.NewRequestInfo(target, operation.WithPeer(peer))
}

func dependencyFailure(err error) error {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return failure.MarkTransport(err)
}

func decodeClientError(response *protocol.Response) error {
	status := response.StatusCode()
	var envelope errorEnvelope
	if err := json.Unmarshal(response.Body(), &envelope); err != nil {
		return kerrors.New(
			int32(status),
			"HTTP_"+httpStatusText(status),
			httpStatusText(status),
		)
	}
	if envelope.Code == 0 {
		envelope.Code = int32(status)
	}
	if envelope.Reason == "" {
		envelope.Reason = "HTTP_" + httpStatusText(status)
	}
	if envelope.Message == "" {
		envelope.Message = httpStatusText(status)
	}
	return kerrors.New(
		envelope.Code,
		envelope.Reason,
		envelope.Message,
		kerrors.WithMetadata(envelope.Metadata),
	)
}

func httpStatusText(status int) string {
	text := http.StatusText(status)
	if text == "" {
		return "Unknown Status"
	}
	return text
}

func requestHeaderBytes(header *protocol.RequestHeader) int64 {
	if header == nil {
		return 0
	}
	var size int64
	header.VisitAll(func(key, value []byte) {
		size += int64(len(key) + len(value) + 4)
	})
	return size
}

func responseHeaderBytes(header *protocol.ResponseHeader) int64 {
	if header == nil {
		return 0
	}
	var size int64
	header.VisitAll(func(key, value []byte) {
		size += int64(len(key) + len(value) + 4)
	})
	return size
}

func isNilClientPicker(picker kclient.Picker) bool {
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
