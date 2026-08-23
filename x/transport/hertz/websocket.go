package hertz

import (
	"fmt"
	"net/http"

	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	transporthttp "github.com/keelab/keelith/transport/http"
	keelithws "github.com/keelab/keelith/transport/websocket"
)

// HandleWebSocket registers one lifecycle-owned WebSocket route through
// Hertz's official http.Handler adaptor.
//
// The route still uses Keelith's standard HTTP Operation, Middleware,
// Metadata, error, Hub, and StreamBundle semantics. The archived
// hertz-contrib/websocket package is intentionally not used.
func (server *Server) HandleWebSocket(
	path string,
	target operation.Operation,
	hub *keelithws.Hub,
	handler middleware.Handler,
) error {
	if server == nil ||
		hub == nil ||
		handler == nil ||
		target.Transport() != "http" ||
		target.Kind() != operation.KindBidiStream {
		return fmt.Errorf(
			"%w: websocket route is incomplete or not bidi-stream",
			ErrInvalidOption,
		)
	}
	routerOptions := []transporthttp.RouterOption{
		transporthttp.WithMetadataPolicy(server.semantics.metadata),
		transporthttp.WithMaxResponseBytes(
			int64(server.semantics.maxResponseBytes),
		),
	}
	if server.bundle != nil {
		routerOptions = append(
			routerOptions,
			transporthttp.WithMiddleware(server.bundle),
		)
	}
	if server.semantics.propagator != nil {
		routerOptions = append(
			routerOptions,
			transporthttp.WithPropagator(server.semantics.propagator),
		)
	}
	if len(server.semantics.errorMetadataKeys) > 0 {
		routerOptions = append(
			routerOptions,
			transporthttp.WithErrorMetadata(
				server.semantics.errorMetadataKeys...,
			),
		)
	}
	router, err := transporthttp.NewRouter(routerOptions...)
	if err != nil {
		return fmt.Errorf("%w: build websocket router: %w", ErrInvalidOption, err)
	}
	if err := router.Handle(
		http.MethodGet,
		path,
		target,
		keelithws.DecodeRequest,
		handler,
		hub.Encode,
		transporthttp.WithStreaming(),
	); err != nil {
		return fmt.Errorf(
			"%w: build websocket route: %w",
			ErrInvalidOption,
			err,
		)
	}
	return server.addRouteWith(
		http.MethodGet,
		path,
		adaptor.HertzHandler(router),
		func() {
			server.websocketHubs[hub] = struct{}{}
		},
	)
}
