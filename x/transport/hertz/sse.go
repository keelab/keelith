package hertz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/protocol/http1/resp"
	ksse "github.com/keelab/keelith/transport/sse"
)

// DecodeSSERequest reads and validates the standard Last-Event-ID header.
func DecodeSSERequest(request *app.RequestContext) (any, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: request is nil", ksse.ErrInvalid)
	}
	values := request.Request.Header.PeekAll("Last-Event-ID")
	if len(values) > 1 {
		return nil, fmt.Errorf(
			"%w: multiple Last-Event-ID values",
			ksse.ErrInvalid,
		)
	}
	value := ""
	if len(values) == 1 {
		value = string(values[0])
	}
	return ksse.NewRequest(value)
}

// NewSSEEncoder creates a bounded native Hertz SSE Encoder.
//
// It commits a chunked response before waiting for the first event and keeps
// the route's common Middleware lifecycle active until stream termination.
func NewSSEEncoder(config ksse.Config) (Encoder, error) {
	settings, err := ksse.Resolve(config)
	if err != nil {
		return nil, err
	}
	return func(
		ctx context.Context,
		request *app.RequestContext,
		response any,
	) (resultErr error) {
		if ctx == nil || request == nil {
			return ksse.ErrInvalid
		}
		stream, ok := response.(ksse.Stream)
		if !ok || stream.Events() == nil {
			return fmt.Errorf(
				"%w: response type %T",
				ksse.ErrInvalid,
				response,
			)
		}
		request.Response.Header.SetStatusCode(consts.StatusOK)
		request.Response.Header.SetContentType(ksse.ContentType)
		request.Response.Header.Set("Cache-Control", ksse.CacheControl)
		request.Response.Header.Set("X-Content-Type-Options", "nosniff")

		writer := request.Response.GetHijackWriter()
		if writer == nil {
			writer = resp.NewChunkedBodyWriter(
				&request.Response,
				request.GetWriter(),
			)
			request.Response.HijackWriter(writer)
		}
		defer func() {
			resultErr = errors.Join(resultErr, writer.Finalize())
		}()
		if _, err := writer.Write(nil); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}

		var ticker *time.Ticker
		var heartbeat <-chan time.Time
		if settings.HeartbeatEnabled() {
			ticker = time.NewTicker(settings.HeartbeatInterval())
			heartbeat = ticker.C
			defer ticker.Stop()
		}
		events := stream.Events()
		failures := stream.Failures()
		for events != nil {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case event, open := <-events:
				if !open {
					return nil
				}
				payload, err := ksse.Render(
					event,
					settings.MaximumEventBytes(),
				)
				if errors.Is(err, ksse.ErrEventTooLarge) {
					return ErrResponseTooLarge
				}
				if err != nil {
					return err
				}
				if _, err := writer.Write(payload); err != nil {
					return err
				}
				if err := writer.Flush(); err != nil {
					return err
				}
			case sourceErr, open := <-failures:
				if !open {
					failures = nil
					continue
				}
				if sourceErr == nil {
					return ksse.ErrSource
				}
				return fmt.Errorf("%w: %v", ksse.ErrSource, sourceErr)
			case <-heartbeat:
				if _, err := io.WriteString(
					writer,
					ksse.HeartbeatComment,
				); err != nil {
					return err
				}
				if err := writer.Flush(); err != nil {
					return err
				}
			}
		}
		return nil
	}, nil
}
