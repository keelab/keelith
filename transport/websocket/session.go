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
func (r Request) Origin() string {
	if r.state == nil || r.state.raw == nil {
		return ""
	}
	return r.state.raw.Header.Get("Origin")
}

// RequestedSubprotocols returns a bounded snapshot of client offers.
func (r Request) RequestedSubprotocols() []string {
	if r.state == nil || r.state.raw == nil {
		return nil
	}
	result := make([]string, 0)
	for _, header := range r.state.raw.Header.Values(
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
func DecodeRequest(r *http.Request) (any, error) {
	if r == nil ||
		r.Method != http.MethodGet ||
		!headerContainsToken(r.Header, "Connection", "upgrade") ||
		!headerContainsToken(r.Header, "Upgrade", "websocket") {
		return nil, ErrHandshake
	}
	return Request{state: &requestState{raw: r}}, nil
}

// SessionHandler owns one upgraded connection until it returns.
type SessionHandler func(context.Context, *Connection) error

// Session binds the decoded request to its application handler.
type Session struct {
	request Request
	handler SessionHandler
}

// NewSession constructs the response consumed by Hub.Encode.
func NewSession(r Request, handler SessionHandler) (Session, error) {
	if r.state == nil || r.state.raw == nil || handler == nil {
		return Session{}, fmt.Errorf(
			"%w: request or handler is nil",
			ErrInvalidOption,
		)
	}
	return Session{request: r, handler: handler}, nil
}

func (r Request) consume() *http.Request {
	if r.state == nil ||
		r.state.raw == nil ||
		!r.state.consumed.CompareAndSwap(false, true) {
		return nil
	}
	return r.state.raw
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
