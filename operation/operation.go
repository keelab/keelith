// Package operation defines stable identities for transport-neutral calls.
package operation

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrInvalid means an Operation component is missing or malformed.
	ErrInvalid = errors.New("operation: invalid operation")
)

// Kind identifies an invocation shape.
type Kind string

const (
	// KindUnary is a single request and single response.
	KindUnary Kind = "unary"
	// KindClientStream is a streamed request and single response.
	KindClientStream Kind = "client-stream"
	// KindServerStream is a single request and streamed response.
	KindServerStream Kind = "server-stream"
	// KindBidiStream is a bidirectional stream.
	KindBidiStream Kind = "bidi-stream"
	// KindConsumer is a message consumer invocation.
	KindConsumer Kind = "consumer"
	// KindJob is a scheduled or ad hoc job invocation.
	KindJob Kind = "job"
)

// Operation is an immutable, low-cardinality call identity.
type Operation struct {
	transport string
	service   string
	method    string
	kind      Kind
}

// New validates and constructs an Operation.
func New(transport, service, method string, kind Kind) (Operation, error) {
	normalizedTransport := strings.ToLower(transport)
	if !validTransport(normalizedTransport) {
		return Operation{}, fmt.Errorf("%w: transport %q", ErrInvalid, transport)
	}
	if !validComponent(service) {
		return Operation{}, fmt.Errorf("%w: service %q", ErrInvalid, service)
	}
	if !validComponent(method) {
		return Operation{}, fmt.Errorf("%w: method %q", ErrInvalid, method)
	}
	if !validKind(kind) {
		return Operation{}, fmt.Errorf("%w: kind %q", ErrInvalid, kind)
	}
	return Operation{
		transport: normalizedTransport,
		service:   service,
		method:    method,
		kind:      kind,
	}, nil
}

// Transport returns the normalized transport identifier.
func (o Operation) Transport() string {
	return o.transport
}

// Service returns the logical service identifier.
func (o Operation) Service() string {
	return o.service
}

// Method returns the logical method identifier.
func (o Operation) Method() string {
	return o.method
}

// Kind returns the invocation shape.
func (o Operation) Kind() Kind {
	return o.kind
}

// PolicyKey returns a stable, collision-free, human-readable policy key.
func (o Operation) PolicyKey() string {
	return url.PathEscape(o.transport) + "/" +
		url.PathEscape(o.service) + "/" +
		url.PathEscape(o.method) + "/" +
		url.PathEscape(string(o.kind))
}

// String returns the stable policy key.
func (o Operation) String() string {
	return o.PolicyKey()
}

// WithContext attaches an Operation through the canonical RequestInfo context
// value. Prefer WithRequestInfo when transport peer facts are available.
func WithContext(ctx context.Context, operation Operation) context.Context {
	return context.WithValue(ctx, requestInfoContextKey{}, RequestInfo{
		operation: operation,
		attempt:   1,
	})
}

// FromContext returns the Operation projected from the canonical RequestInfo.
func FromContext(ctx context.Context) (Operation, bool) {
	info, ok := RequestInfoFromContext(ctx)
	if !ok {
		return Operation{}, false
	}
	return info.Operation(), true
}

func validTransport(transport string) bool {
	if transport == "" || transport[0] < 'a' || transport[0] > 'z' {
		return false
	}
	for index := 1; index < len(transport); index++ {
		ch := transport[index]
		if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '+' || ch == '-' || ch == '.' {
			continue
		}
		return false
	}
	return true
}

func validComponent(component string) bool {
	if component == "" || !utf8.ValidString(component) {
		return false
	}
	if strings.TrimSpace(component) != component {
		return false
	}
	for _, r := range component {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validKind(kind Kind) bool {
	switch kind {
	case KindUnary, KindClientStream, KindServerStream, KindBidiStream, KindConsumer, KindJob:
		return true
	default:
		return false
	}
}
