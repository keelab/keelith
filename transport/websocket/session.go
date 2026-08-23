package websocket

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
)

// Request is the validated, request-scoped WebSocket upgrade input.
//
// It intentionally does not expose the underlying *http.Request. Authentication
// and Metadata should be read from context by ordinary Keelith middleware.
type Request struct {
	state *requestState
}

type requestState struct {
	raw      *http.Request
	consumed atomic.Bool
}

// Origin returns the browser Origin header for application-level auditing.
func (request Request) Origin() string {
	if request.state == nil || request.state.raw == nil {
		return ""
	}
	return request.state.raw.Header.Get("Origin")
}

// RequestedSubprotocols returns a bounded snapshot of client offers.
func (request Request) RequestedSubprotocols() []string {
	if request.state == nil || request.state.raw == nil {
		return nil
	}
	result := make([]string, 0)
	for _, header := range request.state.raw.Header.Values(
		"Sec-WebSocket-Protocol",
	) {
		for _, value := range strings.Split(header, ",") {
			value = strings.TrimSpace(value)
			if validSubprotocol(value) {
				result = append(result, value)
				if len(result) == 32 {
					return result
				}
			}
		}
	}
	return result
}

// DecodeRequest validates the request shape before ordinary middleware runs.
func DecodeRequest(request *http.Request) (any, error) {
	if request == nil ||
		request.Method != http.MethodGet ||
		!headerContainsToken(request.Header, "Connection", "upgrade") ||
		!headerContainsToken(request.Header, "Upgrade", "websocket") {
		return nil, ErrHandshake
	}
	return Request{state: &requestState{raw: request}}, nil
}

// SessionHandler owns one upgraded connection until it returns.
type SessionHandler func(context.Context, *Connection) error

// Session binds the decoded request to its application handler.
type Session struct {
	request Request
	handler SessionHandler
}

// NewSession constructs the response consumed by Hub.Encode.
func NewSession(request Request, handler SessionHandler) (Session, error) {
	if request.state == nil || request.state.raw == nil || handler == nil {
		return Session{}, fmt.Errorf(
			"%w: request or handler is nil",
			ErrInvalidOption,
		)
	}
	return Session{request: request, handler: handler}, nil
}

func (request Request) consume() *http.Request {
	if request.state == nil ||
		request.state.raw == nil ||
		!request.state.consumed.CompareAndSwap(false, true) {
		return nil
	}
	return request.state.raw
}

func headerContainsToken(
	header http.Header,
	name string,
	target string,
) bool {
	for _, value := range header.Values(name) {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), target) {
				return true
			}
		}
	}
	return false
}
