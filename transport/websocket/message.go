package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	coderws "github.com/coder/websocket"
	"github.com/keelab/keelith/middleware"
)

// MessageType identifies a complete text or binary WebSocket message.
type MessageType string

const (
	// MessageText contains valid UTF-8.
	MessageText MessageType = "text"
	// MessageBinary contains arbitrary bytes.
	MessageBinary MessageType = "binary"
)

// StatusCode is an RFC 6455 close status.
type StatusCode int

const (
	// StatusNormalClosure indicates successful session completion.
	StatusNormalClosure StatusCode = 1000
	// StatusGoingAway indicates process or endpoint shutdown.
	StatusGoingAway StatusCode = 1001
	// StatusPolicyViolation indicates an application policy rejection.
	StatusPolicyViolation StatusCode = 1008
	// StatusMessageTooBig indicates a message beyond the accepted budget.
	StatusMessageTooBig StatusCode = 1009
	// StatusInternalError indicates an unexpected session failure.
	StatusInternalError StatusCode = 1011
)

// Message is an immutable complete WebSocket message.
type Message struct {
	messageType MessageType
	content     []byte
}

// NewMessage validates and snapshots one complete message.
func NewMessage(messageType MessageType, content []byte) (Message, error) {
	if messageType != MessageText && messageType != MessageBinary {
		return Message{}, fmt.Errorf(
			"%w: message type %q",
			ErrInvalidOption,
			messageType,
		)
	}
	if messageType == MessageText && !utf8.Valid(content) {
		return Message{}, fmt.Errorf(
			"%w: text message is not UTF-8",
			ErrInvalidOption,
		)
	}
	return Message{
		messageType: messageType,
		content:     append([]byte(nil), content...),
	}, nil
}

// Type returns the message kind.
func (message Message) Type() MessageType { return message.messageType }

// Bytes returns an independent payload copy.
func (message Message) Bytes() []byte {
	return append([]byte(nil), message.content...)
}

// Connection exposes bounded complete-message operations.
//
// Read must not be called concurrently. Write and Ping may run concurrently
// with Read and with other writes.
type Connection struct {
	raw       *coderws.Conn
	hub       *Hub
	context   context.Context
	cancel    context.CancelCauseFunc
	events    middleware.StreamHandler
	send      atomic.Uint64
	receive   atomic.Uint64
	finish    sync.Once
	finishErr error
}

func newConnection(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	raw *coderws.Conn,
	hub *Hub,
) *Connection {
	connection := &Connection{
		raw:     raw,
		hub:     hub,
		context: ctx,
		cancel:  cancel,
	}
	terminal := middleware.StreamHandler(func(
		ctx context.Context,
		event middleware.StreamEvent,
	) error {
		switch event.Phase {
		case middleware.StreamPhaseCreate, middleware.StreamPhaseFinish:
			return nil
		case middleware.StreamPhaseSend:
			message, ok := event.Message.(Message)
			if !ok {
				return fmt.Errorf(
					"%w: send message type %T",
					ErrInvalidOption,
					event.Message,
				)
			}
			if int64(len(message.content)) > hub.options.MaxWriteBytes {
				return ErrMessageTooLarge
			}
			return raw.Write(
				ctx,
				coderMessageType(message.messageType),
				message.content,
			)
		case middleware.StreamPhaseReceive:
			target, ok := event.Message.(*Message)
			if !ok || target == nil {
				return fmt.Errorf(
					"%w: receive message target",
					ErrInvalidOption,
				)
			}
			messageType, content, err := raw.Read(ctx)
			if err != nil {
				return err
			}
			*target = Message{
				messageType: keelithMessageType(messageType),
				content:     append([]byte(nil), content...),
			}
			return nil
		default:
			return fmt.Errorf(
				"%w: stream phase %q",
				ErrInvalidOption,
				event.Phase,
			)
		}
	})
	if hub.streams != nil {
		terminal = hub.streams.Chain()(terminal)
	}
	connection.events = terminal
	return connection
}

// Read receives one complete message.
func (connection *Connection) Read(ctx context.Context) (Message, error) {
	if connection == nil || ctx == nil {
		return Message{}, fmt.Errorf(
			"%w: connection or context is nil",
			ErrInvalidOption,
		)
	}
	var message Message
	err := connection.events(ctx, middleware.StreamEvent{
		Side:     middleware.StreamSideServer,
		Phase:    middleware.StreamPhaseReceive,
		Sequence: connection.receive.Add(1),
		Message:  &message,
	})
	if err != nil {
		return Message{}, connection.mapError(ctx, err)
	}
	connection.hub.received.Add(1)
	return message, nil
}

// Write sends one immutable complete message.
func (connection *Connection) Write(
	ctx context.Context,
	message Message,
) error {
	if connection == nil || ctx == nil {
		return fmt.Errorf(
			"%w: connection or context is nil",
			ErrInvalidOption,
		)
	}
	if _, err := NewMessage(message.messageType, message.content); err != nil {
		return err
	}
	err := connection.events(ctx, middleware.StreamEvent{
		Side:     middleware.StreamSideServer,
		Phase:    middleware.StreamPhaseSend,
		Sequence: connection.send.Add(1),
		Message:  message,
	})
	if err != nil {
		return connection.mapError(ctx, err)
	}
	connection.hub.sent.Add(1)
	return nil
}

// Ping sends a control-frame ping and waits for the peer pong.
//
// A reader must be active, as required by the underlying RFC 6455 runtime.
func (connection *Connection) Ping(ctx context.Context) error {
	if connection == nil || ctx == nil {
		return fmt.Errorf(
			"%w: connection or context is nil",
			ErrInvalidOption,
		)
	}
	return connection.mapError(ctx, connection.raw.Ping(ctx))
}

// CloseRead starts a control-frame read loop for a write-only connection.
// Read must not be called after this method.
func (connection *Connection) CloseRead(ctx context.Context) context.Context {
	if connection == nil || ctx == nil {
		closed, cancel := context.WithCancel(context.Background())
		cancel()
		return closed
	}
	return connection.raw.CloseRead(ctx)
}

// Close performs the RFC 6455 close handshake with a stable reason.
func (connection *Connection) Close(code StatusCode, reason string) error {
	if connection == nil {
		return nil
	}
	if len(reason) > 123 || !utf8.ValidString(reason) {
		return fmt.Errorf("%w: close reason", ErrInvalidOption)
	}
	return connection.raw.Close(coderws.StatusCode(code), reason)
}

// Subprotocol returns the negotiated subprotocol, or empty when optional.
func (connection *Connection) Subprotocol() string {
	if connection == nil {
		return ""
	}
	return connection.raw.Subprotocol()
}

func (connection *Connection) create() error {
	return connection.events(connection.context, middleware.StreamEvent{
		Side:  middleware.StreamSideServer,
		Phase: middleware.StreamPhaseCreate,
	})
}

func (connection *Connection) complete(err error) error {
	connection.finish.Do(func() {
		connection.finishErr = connection.events(
			connection.context,
			middleware.StreamEvent{
				Side:  middleware.StreamSideServer,
				Phase: middleware.StreamPhaseFinish,
				Error: err,
			},
		)
	})
	return connection.finishErr
}

func (connection *Connection) startHeartbeat() {
	if connection.hub.options.PingInterval == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(connection.hub.options.PingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-connection.context.Done():
				return
			case <-ticker.C:
				pingContext, cancel := context.WithTimeout(
					connection.context,
					connection.hub.options.PingTimeout,
				)
				err := connection.raw.Ping(pingContext)
				cancel()
				if err != nil {
					connection.hub.heartbeatFailures.Add(1)
					connection.cancel(ErrHeartbeat)
					_ = connection.raw.CloseNow()
					return
				}
			}
		}
	}()
}

func (connection *Connection) mapError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if cause := context.Cause(connection.context); cause != nil {
		return cause
	}
	if errors.Is(err, coderws.ErrMessageTooBig) ||
		errors.Is(err, ErrMessageTooLarge) {
		return ErrMessageTooLarge
	}
	status := coderws.CloseStatus(err)
	switch status {
	case coderws.StatusNormalClosure, coderws.StatusGoingAway:
		return io.EOF
	case -1:
		if errors.Is(err, net.ErrClosed) {
			return ErrConnectionClosed
		}
	default:
		return fmt.Errorf(
			"%w: status %d",
			ErrConnectionClosed,
			status,
		)
	}
	return ErrConnectionClosed
}

func coderMessageType(messageType MessageType) coderws.MessageType {
	if messageType == MessageText {
		return coderws.MessageText
	}
	return coderws.MessageBinary
}

func keelithMessageType(messageType coderws.MessageType) MessageType {
	if messageType == coderws.MessageText {
		return MessageText
	}
	return MessageBinary
}
