// Package websocket provides an explicit, lifecycle-owned RFC 6455 adapter
// for standard Keelith HTTP routes.
package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	coderws "github.com/coder/websocket"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

type hubState uint8

const (
	hubNew hubState = iota
	hubRunning
	hubClosing
	hubStopped
)

// Description is a low-cardinality, value-free Hub snapshot.
type Description struct {
	State             string
	Ready             bool
	Active            int
	Pending           int
	Accepted          uint64
	Finished          uint64
	Rejected          uint64
	Sent              uint64
	Received          uint64
	HeartbeatFailures uint64
	Closed            bool
}

// Hub owns upgraded connections that net/http no longer manages.
type Hub struct {
	options Options
	streams *middleware.StreamBundle

	mu          sync.Mutex
	state       hubState
	pending     int
	connections map[*Connection]struct{}
	drained     chan struct{}
	drainOnce   sync.Once

	accepted          atomic.Uint64
	finished          atomic.Uint64
	rejected          atomic.Uint64
	sent              atomic.Uint64
	received          atomic.Uint64
	heartbeatFailures atomic.Uint64
}

// NewHub creates an instance-scoped WebSocket lifecycle component.
func NewHub(
	options Options,
	streams *middleware.StreamBundle,
) (*Hub, error) {
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Hub{
		options:     normalized,
		streams:     streams,
		state:       hubNew,
		connections: make(map[*Connection]struct{}),
		drained:     make(chan struct{}),
	}, nil
}

// Name returns the App component name.
func (hub *Hub) Name() string {
	if hub == nil {
		return defaultName
	}
	return hub.options.Name
}

// Start opens the handshake gate before HTTP servers start.
func (hub *Hub) Start(ctx context.Context) error {
	if hub == nil || ctx == nil {
		return fmt.Errorf(
			"%w: hub or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.state != hubNew {
		return ErrNotRunning
	}
	hub.state = hubRunning
	return nil
}

// Stop rejects handshakes, cancels sessions, closes hijacked connections, and
// waits for handlers to leave or ctx to expire.
func (hub *Hub) Stop(ctx context.Context) error {
	if hub == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	hub.mu.Lock()
	connections := make([]*Connection, 0)
	switch hub.state {
	case hubNew:
		hub.state = hubStopped
		hub.maybeDrainLocked()
	case hubRunning:
		hub.state = hubClosing
		for connection := range hub.connections {
			connections = append(connections, connection)
		}
		hub.maybeDrainLocked()
	case hubClosing, hubStopped:
		hub.maybeDrainLocked()
	}
	drained := hub.drained
	hub.mu.Unlock()
	for _, connection := range connections {
		connection.cancel(ErrClosed)
		_ = connection.raw.CloseNow()
	}

	select {
	case <-drained:
		hub.mu.Lock()
		hub.state = hubStopped
		hub.mu.Unlock()
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Describe returns aggregate state without route, origin, peer, or payload.
func (hub *Hub) Describe() Description {
	if hub == nil {
		return Description{Closed: true}
	}
	hub.mu.Lock()
	active := len(hub.connections)
	pending := hub.pending
	state := hub.state.String()
	ready := hub.state == hubRunning
	closed := hub.state == hubClosing || hub.state == hubStopped
	hub.mu.Unlock()
	return Description{
		State:             state,
		Ready:             ready,
		Active:            active,
		Pending:           pending,
		Accepted:          hub.accepted.Load(),
		Finished:          hub.finished.Load(),
		Rejected:          hub.rejected.Load(),
		Sent:              hub.sent.Load(),
		Received:          hub.received.Load(),
		HeartbeatFailures: hub.heartbeatFailures.Load(),
		Closed:            closed,
	}
}

func (state hubState) String() string {
	switch state {
	case hubNew:
		return "new"
	case hubRunning:
		return "active"
	case hubClosing:
		return "draining"
	case hubStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Encode upgrades a Session returned by a Keelith streaming HTTP route.
func (hub *Hub) Encode(
	ctx context.Context,
	writer http.ResponseWriter,
	response any,
) error {
	if hub == nil || ctx == nil || writer == nil {
		return fmt.Errorf(
			"%w: hub, context, or writer is nil",
			ErrInvalidOption,
		)
	}
	target, ok := operation.FromContext(ctx)
	if !ok || target.Kind() != operation.KindBidiStream {
		return fmt.Errorf(
			"%w: websocket route must use bidi-stream operation",
			ErrInvalidOption,
		)
	}
	session, ok := response.(Session)
	if !ok || session.request.state == nil || session.handler == nil {
		return fmt.Errorf(
			"%w: response type %T",
			ErrInvalidOption,
			response,
		)
	}
	if !hub.reserve() {
		hub.rejected.Add(1)
		if hub.isRunning() {
			return ErrCapacity
		}
		return ErrNotRunning
	}
	reserved := true
	defer func() {
		if reserved {
			hub.releaseReservation()
		}
	}()

	request := session.request.consume()
	if request == nil {
		return fmt.Errorf(
			"%w: websocket request was already consumed",
			ErrInvalidOption,
		)
	}
	request = request.WithContext(ctx)
	headers := cloneHeader(writer.Header())
	handshake := &handshakeWriter{ResponseWriter: writer}
	raw, err := coderws.Accept(
		handshake,
		request,
		hub.options.acceptOptions(),
	)
	if err != nil {
		restoreHeader(writer.Header(), headers)
		hub.rejected.Add(1)
		return ErrHandshake
	}
	if hub.options.RequireSubprotocol && raw.Subprotocol() == "" {
		hub.rejected.Add(1)
		_ = raw.Close(
			coderws.StatusPolicyViolation,
			"subprotocol required",
		)
		return ErrHandshake
	}
	raw.SetReadLimit(hub.options.MaxReadBytes)
	connection, err := hub.activate(ctx, raw)
	reserved = false
	if err != nil {
		hub.rejected.Add(1)
		_ = raw.CloseNow()
		return err
	}
	defer hub.remove(connection)

	if err := connection.create(); err != nil {
		_ = raw.Close(
			coderws.StatusPolicyViolation,
			"stream rejected",
		)
		_ = connection.complete(err)
		return err
	}
	connection.startHeartbeat()
	sessionErr := session.handler(connection.context, connection)
	if errors.Is(sessionErr, io.EOF) {
		sessionErr = nil
	}
	finishErr := connection.complete(sessionErr)
	result := errors.Join(sessionErr, finishErr)
	switch {
	case result == nil:
		_ = raw.Close(coderws.StatusNormalClosure, "")
	case errors.Is(result, ErrClosed):
		_ = raw.CloseNow()
	default:
		_ = raw.Close(
			coderws.StatusInternalError,
			"session failed",
		)
	}
	return result
}

func (hub *Hub) reserve() bool {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.state != hubRunning ||
		hub.pending+len(hub.connections) >= hub.options.MaxConnections {
		return false
	}
	hub.pending++
	return true
}

func (hub *Hub) releaseReservation() {
	hub.mu.Lock()
	if hub.pending > 0 {
		hub.pending--
	}
	hub.maybeDrainLocked()
	hub.mu.Unlock()
}

func (hub *Hub) activate(
	ctx context.Context,
	raw *coderws.Conn,
) (*Connection, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.pending > 0 {
		hub.pending--
	}
	if hub.state != hubRunning {
		hub.maybeDrainLocked()
		return nil, ErrClosed
	}
	sessionContext, cancel := context.WithCancelCause(ctx)
	connection := newConnection(
		sessionContext,
		cancel,
		raw,
		hub,
	)
	hub.connections[connection] = struct{}{}
	hub.accepted.Add(1)
	return connection, nil
}

func (hub *Hub) remove(connection *Connection) {
	if connection == nil {
		return
	}
	connection.cancel(nil)
	hub.mu.Lock()
	if _, exists := hub.connections[connection]; exists {
		delete(hub.connections, connection)
		hub.finished.Add(1)
	}
	hub.maybeDrainLocked()
	hub.mu.Unlock()
}

func (hub *Hub) isRunning() bool {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return hub.state == hubRunning
}

func (hub *Hub) maybeDrainLocked() {
	if (hub.state == hubClosing || hub.state == hubStopped) &&
		hub.pending == 0 &&
		len(hub.connections) == 0 {
		hub.drainOnce.Do(func() { close(hub.drained) })
	}
}

type handshakeWriter struct {
	http.ResponseWriter
	upgraded bool
}

func (writer *handshakeWriter) WriteHeader(status int) {
	if writer.upgraded {
		return
	}
	if status == http.StatusSwitchingProtocols {
		writer.upgraded = true
		writer.ResponseWriter.WriteHeader(status)
	}
}

func (writer *handshakeWriter) Write(content []byte) (int, error) {
	if writer.upgraded {
		return writer.ResponseWriter.Write(content)
	}
	return len(content), nil
}

func (writer *handshakeWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

func cloneHeader(header http.Header) http.Header {
	result := make(http.Header, len(header))
	for key, values := range header {
		result[key] = append([]string(nil), values...)
	}
	return result
}

func restoreHeader(target, snapshot http.Header) {
	for key := range target {
		delete(target, key)
	}
	for key, values := range snapshot {
		target[key] = append([]string(nil), values...)
	}
}
