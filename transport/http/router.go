package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"net/url"
	"strings"
	"sync"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/internal/httptemplate"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"go.opentelemetry.io/otel/propagation"
)

const defaultMaxResponseBytes = int64(4 * 1024 * 1024)

// RouterOption configures a Router.
type RouterOption interface {
	applyRouter(*routerOptions) error
}

type routerOptionFunc func(*routerOptions) error

func (function routerOptionFunc) applyRouter(options *routerOptions) error {
	return function(options)
}

type routerOptions struct {
	bundle            *middleware.Bundle
	metadataPolicy    metadata.Policy
	maxResponseBytes  int64
	errorEncoder      ErrorEncoder
	errorMetadataKeys []string
	propagator        propagation.TextMapPropagator
}

// WithMiddleware configures the immutable inbound middleware Bundle.
func WithMiddleware(bundle *middleware.Bundle) RouterOption {
	return routerOptionFunc(func(options *routerOptions) error {
		if bundle == nil {
			return fmt.Errorf("middleware bundle is nil")
		}
		options.bundle = bundle
		return nil
	})
}

// WithMetadataPolicy configures inbound HTTP header propagation.
func WithMetadataPolicy(policy metadata.Policy) RouterOption {
	return routerOptionFunc(func(options *routerOptions) error {
		options.metadataPolicy = policy
		return nil
	})
}

// WithPropagator configures distributed context extraction.
func WithPropagator(propagator propagation.TextMapPropagator) RouterOption {
	return routerOptionFunc(func(options *routerOptions) error {
		if propagator == nil {
			return fmt.Errorf("propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithMaxResponseBytes sets the ordinary response body budget.
func WithMaxResponseBytes(maxBytes int64) RouterOption {
	return routerOptionFunc(func(options *routerOptions) error {
		if maxBytes <= 0 {
			return fmt.Errorf("max response bytes must be positive")
		}
		options.maxResponseBytes = maxBytes
		return nil
	})
}

// WithErrorMetadata allows selected framework Error metadata on the wire.
func WithErrorMetadata(keys ...string) RouterOption {
	snapshot := append([]string(nil), keys...)
	return routerOptionFunc(func(options *routerOptions) error {
		normalized, err := validateErrorMetadata(snapshot)
		if err != nil {
			return err
		}
		options.errorMetadataKeys = normalized
		return nil
	})
}

// WithErrorEncoder replaces the default JSON error encoder.
func WithErrorEncoder(encoder ErrorEncoder) RouterOption {
	return routerOptionFunc(func(options *routerOptions) error {
		if encoder == nil {
			return fmt.Errorf("error encoder is nil")
		}
		options.errorEncoder = encoder
		return nil
	})
}

// RouteOption configures one route.
type RouteOption interface {
	applyRoute(*routeOptions) error
}

type routeOptionFunc func(*routeOptions) error

func (function routeOptionFunc) applyRoute(options *routeOptions) error {
	return function(options)
}

type routeOptions struct {
	streaming bool
}

// WithStreaming disables ordinary response buffering for a route.
func WithStreaming() RouteOption {
	return routeOptionFunc(func(options *routeOptions) error {
		options.streaming = true
		return nil
	})
}

// Router maps standard HTTP routes to transport-neutral handlers.
type Router struct {
	mux              *nethttp.ServeMux
	bundle           *middleware.Bundle
	metadataPolicy   metadata.Policy
	maxResponseBytes int64
	errorEncoder     ErrorEncoder
	propagator       propagation.TextMapPropagator

	mu             sync.RWMutex
	routes         map[string]struct{}
	templateRoutes []templateRoute
}

type templateRoute struct {
	method      string
	template    *httptemplate.Template
	handler     nethttp.Handler
	specificity int
}

// NewRouter validates options and creates an isolated Router.
func NewRouter(optionList ...RouterOption) (*Router, error) {
	options := routerOptions{maxResponseBytes: defaultMaxResponseBytes}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: router option %d is nil", ErrInvalidOption, index)
		}
		if err := option.applyRouter(&options); err != nil {
			return nil, fmt.Errorf("%w: router option %d: %w", ErrInvalidOption, index, err)
		}
	}
	if options.errorEncoder == nil {
		options.errorEncoder = newErrorEncoder(options.errorMetadataKeys)
	}
	return &Router{
		mux:              nethttp.NewServeMux(),
		bundle:           options.bundle,
		metadataPolicy:   options.metadataPolicy,
		maxResponseBytes: options.maxResponseBytes,
		errorEncoder:     options.errorEncoder,
		propagator:       options.propagator,
		routes:           make(map[string]struct{}),
	}, nil
}

// HandleTemplate registers a google.api.http path whose variable is followed
// by a custom verb. Go ServeMux requires wildcards to occupy a full segment,
// so these routes use a small exact matcher while ordinary routes remain on
// ServeMux.
func (router *Router) HandleTemplate(
	method string,
	pathTemplate string,
	target operation.Operation,
	decoder Decoder,
	handler middleware.Handler,
	encoder Encoder,
	optionList ...RouteOption,
) error {
	if router == nil {
		return fmt.Errorf("%w: router is nil", ErrInvalidRoute)
	}
	normalizedMethod := strings.ToUpper(strings.TrimSpace(method))
	template, err := httptemplate.Parse(pathTemplate)
	if err != nil {
		return fmt.Errorf(
			"%w: custom-verb template %q: %w",
			ErrInvalidRoute,
			pathTemplate,
			err,
		)
	}
	if normalizedMethod == "" || template.Verb == "" ||
		!template.EndsWithVariable() {
		return fmt.Errorf(
			"%w: method %q or custom-verb template %q is invalid",
			ErrInvalidRoute,
			method,
			pathTemplate,
		)
	}
	if target.Transport() != "http" {
		return fmt.Errorf(
			"%w: operation transport %q is not http",
			ErrInvalidRoute,
			target.Transport(),
		)
	}
	if decoder == nil || handler == nil || encoder == nil {
		return fmt.Errorf("%w: route codec or handler is nil", ErrInvalidRoute)
	}
	settings := routeOptions{}
	for index, option := range optionList {
		if option == nil {
			return fmt.Errorf("%w: route option %d is nil", ErrInvalidRoute, index)
		}
		if err := option.applyRoute(&settings); err != nil {
			return fmt.Errorf(
				"%w: route option %d: %w",
				ErrInvalidRoute,
				index,
				err,
			)
		}
	}
	key, err := canonicalTemplateRouteKey(normalizedMethod, template)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRoute, err)
	}
	routeHandler := router.route(
		target,
		decoder,
		handler,
		encoder,
		settings.streaming,
	)
	router.mu.Lock()
	defer router.mu.Unlock()
	if _, duplicate := router.routes[key]; duplicate {
		return fmt.Errorf("%w: duplicate %s", ErrInvalidRoute, key)
	}
	specificity := templateRouteSpecificity(template)
	for _, existing := range router.templateRoutes {
		if templateMethodsOverlap(existing.method, normalizedMethod) &&
			existing.specificity == specificity &&
			templateRoutesOverlap(existing.template, template) {
			return fmt.Errorf(
				"%w: ambiguous custom-verb template %s %s",
				ErrInvalidRoute,
				normalizedMethod,
				pathTemplate,
			)
		}
	}
	router.routes[key] = struct{}{}
	router.templateRoutes = append(router.templateRoutes, templateRoute{
		method:      normalizedMethod,
		template:    template,
		handler:     routeHandler,
		specificity: specificity,
	})
	return nil
}

// Handle registers one route with its stable Operation and codecs.
func (router *Router) Handle(
	method string,
	pattern string,
	target operation.Operation,
	decoder Decoder,
	handler middleware.Handler,
	encoder Encoder,
	optionList ...RouteOption,
) error {
	if router == nil {
		return fmt.Errorf("%w: router is nil", ErrInvalidRoute)
	}
	normalizedMethod := strings.ToUpper(strings.TrimSpace(method))
	if normalizedMethod == "" || !strings.HasPrefix(pattern, "/") {
		return fmt.Errorf(
			"%w: method %q or pattern %q is invalid",
			ErrInvalidRoute,
			method,
			pattern,
		)
	}
	if target.Transport() != "http" {
		return fmt.Errorf(
			"%w: operation transport %q is not http",
			ErrInvalidRoute,
			target.Transport(),
		)
	}
	if decoder == nil || handler == nil || encoder == nil {
		return fmt.Errorf("%w: route codec or handler is nil", ErrInvalidRoute)
	}
	settings := routeOptions{}
	for index, option := range optionList {
		if option == nil {
			return fmt.Errorf("%w: route option %d is nil", ErrInvalidRoute, index)
		}
		if err := option.applyRoute(&settings); err != nil {
			return fmt.Errorf("%w: route option %d: %w", ErrInvalidRoute, index, err)
		}
	}

	key := normalizedMethod + " " + pattern
	router.mu.Lock()
	defer router.mu.Unlock()
	if _, duplicate := router.routes[key]; duplicate {
		return fmt.Errorf("%w: duplicate %s", ErrInvalidRoute, key)
	}
	routeHandler := router.route(target, decoder, handler, encoder, settings.streaming)
	if err := registerRoute(router.mux, key, routeHandler); err != nil {
		return err
	}
	router.routes[key] = struct{}{}
	return nil
}

// ServeHTTP dispatches a request to a registered route.
func (router *Router) ServeHTTP(
	writer nethttp.ResponseWriter,
	request *nethttp.Request,
) {
	if router == nil || request == nil || request.URL == nil {
		nethttp.Error(writer, "invalid request", nethttp.StatusBadRequest)
		return
	}
	if !unsafeTemplateDispatchPath(request.URL.EscapedPath()) {
		if candidate := router.matchTemplateRoute(request); candidate != nil {
			_, muxPattern := router.mux.Handler(request)
			if candidate.specificity > serveMuxPatternSpecificity(muxPattern) {
				candidate.handler.ServeHTTP(writer, request)
				return
			}
		}
	}
	router.mux.ServeHTTP(writer, request)
}

func (router *Router) matchTemplateRoute(
	request *nethttp.Request,
) *templateRoute {
	method := strings.ToUpper(request.Method)
	router.mu.RLock()
	routes := append([]templateRoute(nil), router.templateRoutes...)
	router.mu.RUnlock()
	var selected *templateRoute
	for index := range routes {
		route := &routes[index]
		if route.method != method && (route.method != "GET" || method != "HEAD") {
			continue
		}
		if _, err := route.template.Match(request.URL.EscapedPath()); err != nil {
			continue
		}
		exactMethod := route.method == method
		selectedExactMethod := selected != nil && selected.method == method
		if selected == nil ||
			exactMethod && !selectedExactMethod ||
			exactMethod == selectedExactMethod &&
				route.specificity > selected.specificity {
			selected = route
		}
	}
	return selected
}

func canonicalTemplateRouteKey(
	method string,
	template *httptemplate.Template,
) (string, error) {
	route, err := template.Render(func(_ int, _ int, multi bool) string {
		if multi {
			return "{...}"
		}
		return "{}"
	})
	if err != nil {
		return "", err
	}
	return method + " " + route, nil
}

func templateRouteSpecificity(template *httptemplate.Template) int {
	if template == nil {
		return -1
	}
	score := 0
	for _, segment := range template.Segments {
		if segment.Variable == nil {
			score += 1000 + len(decodedTemplateLiteral(segment.Literal))*10
			continue
		}
		for _, pattern := range segment.Variable.Pattern {
			switch pattern.Wildcard {
			case httptemplate.NoWildcard:
				score += 1000 + len(decodedTemplateLiteral(pattern.Literal))*10
			case httptemplate.SingleWildcard:
				score += 100
			case httptemplate.MultiWildcard:
				score += 10
			}
		}
	}
	score += 500 + len(template.Verb)*10
	return score
}

func templateMethodsOverlap(left string, right string) bool {
	return left == right
}

func serveMuxPatternSpecificity(pattern string) int {
	if pattern == "" {
		return -1
	}
	if space := strings.IndexAny(pattern, " \t"); space >= 0 {
		pattern = strings.TrimSpace(pattern[space:])
	}
	if pattern == "" || pattern[0] != '/' {
		return -1
	}
	score := 0
	for _, segment := range strings.Split(strings.TrimPrefix(pattern, "/"), "/") {
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			if strings.HasSuffix(segment, "...}") {
				score += 10
			} else {
				score += 100
			}
			continue
		}
		score += 1000 + len(segment)*10
	}
	return score
}

func unsafeTemplateDispatchPath(escapedPath string) bool {
	if escapedPath == "" || escapedPath[0] != '/' {
		return true
	}
	parts := strings.Split(strings.TrimPrefix(escapedPath, "/"), "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return true
		}
	}
	return false
}

type templateRouteToken struct {
	literal  string
	wildcard httptemplate.Wildcard
}

func templateRoutesOverlap(
	left *httptemplate.Template,
	right *httptemplate.Template,
) bool {
	if left == nil || right == nil || left.Verb != right.Verb {
		return false
	}
	leftTokens := templateRouteTokens(left)
	rightTokens := templateRouteTokens(right)
	leftMulti := len(leftTokens) > 0 &&
		leftTokens[len(leftTokens)-1].wildcard == httptemplate.MultiWildcard
	rightMulti := len(rightTokens) > 0 &&
		rightTokens[len(rightTokens)-1].wildcard == httptemplate.MultiWildcard
	leftPrefix := len(leftTokens)
	if leftMulti {
		leftPrefix--
	}
	rightPrefix := len(rightTokens)
	if rightMulti {
		rightPrefix--
	}
	if !leftMulti && !rightMulti && leftPrefix != rightPrefix {
		return false
	}
	if leftMulti && !rightMulti && rightPrefix < leftPrefix ||
		rightMulti && !leftMulti && leftPrefix < rightPrefix {
		return false
	}
	limit := leftPrefix
	if rightPrefix < limit {
		limit = rightPrefix
	}
	for index := 0; index < limit; index++ {
		leftToken := leftTokens[index]
		rightToken := rightTokens[index]
		if leftToken.wildcard == httptemplate.NoWildcard &&
			rightToken.wildcard == httptemplate.NoWildcard &&
			leftToken.literal != rightToken.literal {
			return false
		}
	}
	return true
}

func templateRouteTokens(
	template *httptemplate.Template,
) []templateRouteToken {
	result := make([]templateRouteToken, 0, len(template.Segments))
	for _, segment := range template.Segments {
		if segment.Variable == nil {
			result = append(result, templateRouteToken{
				literal: decodedTemplateLiteral(segment.Literal),
			})
			continue
		}
		for _, pattern := range segment.Variable.Pattern {
			result = append(result, templateRouteToken{
				literal:  decodedTemplateLiteral(pattern.Literal),
				wildcard: pattern.Wildcard,
			})
		}
	}
	return result
}

func decodedTemplateLiteral(value string) string {
	decoded, err := url.PathUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

func (router *Router) route(
	target operation.Operation,
	decoder Decoder,
	handler middleware.Handler,
	encoder Encoder,
	streaming bool,
) nethttp.Handler {
	var invoke middleware.Handler
	if !streaming {
		invoke = handler
		if router.bundle != nil {
			invoke = router.bundle.Chain()(handler)
		}
	}
	return nethttp.HandlerFunc(func(
		writer nethttp.ResponseWriter,
		request *nethttp.Request,
	) {
		ctx := request.Context()
		if router.propagator != nil {
			ctx = router.propagator.Extract(
				ctx,
				propagation.HeaderCarrier(request.Header),
			)
		}
		requestInfo, err := newRequestInfo(
			target,
			"tcp",
			request.RemoteAddr,
		)
		if err != nil {
			router.writeError(ctx, writer, err)
			return
		}
		ctx = operation.WithRequestInfo(ctx, requestInfo)
		inbound, err := router.metadataPolicy.Extract(httpHeaderCarrier(request.Header))
		if err != nil {
			router.writeError(
				ctx,
				writer,
				fmt.Errorf("%w: %w", ErrHeaderTooLarge, err),
			)
			return
		}
		ctx = metadata.WithInbound(ctx, inbound)
		request = request.WithContext(ctx)

		decoded, err := decoder(request)
		if err != nil {
			router.writeError(ctx, writer, requestDecodeError(err))
			return
		}
		if streaming {
			streamState := &streamingResponseWriter{
				ResponseWriter: writer,
			}
			var streamWriter nethttp.ResponseWriter = streamState
			if flusher, ok := writer.(nethttp.Flusher); ok {
				streamWriter = &flushingStreamingResponseWriter{
					streamingResponseWriter: streamState,
					flusher:                 flusher,
				}
			}
			streamHandler := middleware.Handler(func(
				ctx context.Context,
				request any,
			) (any, error) {
				response, err := handler(ctx, request)
				if err != nil {
					return nil, err
				}
				return nil, encoder(ctx, streamWriter, response)
			})
			if router.bundle != nil {
				streamHandler = router.bundle.Chain()(streamHandler)
			}
			if _, err := streamHandler(ctx, decoded); err != nil &&
				!streamState.committed {
				router.writeError(ctx, writer, err)
			}
			return
		}
		response, err := invoke(ctx, decoded)
		if err != nil {
			router.writeError(ctx, writer, err)
			return
		}

		buffer := newBufferedResponseWriter(router.maxResponseBytes)
		if err := encoder(ctx, buffer, response); err != nil {
			router.writeError(ctx, writer, err)
			return
		}
		buffer.commit(writer)
	})
}

// streamingResponseWriter records whether headers became irreversible while
// retaining flush and ResponseController unwrapping support.
type streamingResponseWriter struct {
	nethttp.ResponseWriter
	committed bool
}

func (writer *streamingResponseWriter) WriteHeader(status int) {
	if writer.committed {
		return
	}
	writer.committed = true
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *streamingResponseWriter) Write(body []byte) (int, error) {
	if !writer.committed {
		writer.WriteHeader(nethttp.StatusOK)
	}
	return writer.ResponseWriter.Write(body)
}

func (writer *streamingResponseWriter) Unwrap() nethttp.ResponseWriter {
	return writer.ResponseWriter
}

type flushingStreamingResponseWriter struct {
	*streamingResponseWriter
	flusher nethttp.Flusher
}

func (writer *flushingStreamingResponseWriter) Flush() {
	if !writer.committed {
		writer.WriteHeader(nethttp.StatusOK)
	}
	writer.flusher.Flush()
}

func requestDecodeError(err error) error {
	var maxBytesError *nethttp.MaxBytesError
	if errors.As(err, &maxBytesError) || errors.Is(err, ErrRequestTooLarge) {
		return err
	}
	return kerrors.Wrap(
		err,
		nethttp.StatusBadRequest,
		"INVALID_REQUEST",
		"request body is invalid",
	)
}

func (router *Router) writeError(
	ctx context.Context,
	writer nethttp.ResponseWriter,
	err error,
) {
	router.errorEncoder(ctx, writer, err)
}

func registerRoute(
	mux *nethttp.ServeMux,
	pattern string,
	handler nethttp.Handler,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"%w: pattern %q: %v",
				ErrInvalidRoute,
				pattern,
				recovered,
			)
		}
	}()
	mux.Handle(pattern, handler)
	return nil
}

type httpHeaderCarrier nethttp.Header

func (carrier httpHeaderCarrier) Values(key string) []string {
	return nethttp.Header(carrier).Values(key)
}

func (carrier httpHeaderCarrier) Set(key string, values []string) {
	header := nethttp.Header(carrier)
	header.Del(key)
	for _, value := range values {
		header.Add(key, value)
	}
}

type bufferedResponseWriter struct {
	header nethttp.Header
	status int
	body   bytes.Buffer
	limit  int64
}

func newBufferedResponseWriter(limit int64) *bufferedResponseWriter {
	return &bufferedResponseWriter{
		header: make(nethttp.Header),
		limit:  limit,
	}
}

func (writer *bufferedResponseWriter) Header() nethttp.Header {
	return writer.header
}

func (writer *bufferedResponseWriter) WriteHeader(status int) {
	if writer.status == 0 {
		writer.status = status
	}
}

func (writer *bufferedResponseWriter) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.status = nethttp.StatusOK
	}
	if int64(writer.body.Len())+int64(len(body)) > writer.limit {
		return 0, ErrResponseTooLarge
	}
	return writer.body.Write(body)
}

func (writer *bufferedResponseWriter) commit(target nethttp.ResponseWriter) {
	for key, values := range writer.header {
		target.Header()[key] = append([]string(nil), values...)
	}
	status := writer.status
	if status == 0 {
		status = nethttp.StatusOK
	}
	target.WriteHeader(status)
	_, _ = target.Write(writer.body.Bytes())
}
