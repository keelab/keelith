package operation

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const maxPeerAddressBytes = 2 * 1024

// Peer is an immutable transport peer identity. Address is intentionally not
// used as a metric label by the operation package because it is high
// cardinality and may contain tenant data.
type Peer struct {
	network string
	address string
}

// NewPeer validates and constructs a peer from a transport-provided address.
func NewPeer(network, address string) (Peer, error) {
	normalizedNetwork := strings.ToLower(strings.TrimSpace(network))
	if !validTransport(normalizedNetwork) {
		return Peer{}, fmt.Errorf("%w: peer network %q", ErrInvalid, network)
	}
	normalizedAddress := strings.TrimSpace(address)
	if normalizedAddress == "" ||
		len(normalizedAddress) > maxPeerAddressBytes ||
		!utf8.ValidString(normalizedAddress) {
		return Peer{}, fmt.Errorf("%w: peer address is empty, oversized, or malformed", ErrInvalid)
	}

	for _, character := range normalizedAddress {
		if unicode.IsControl(character) {
			return Peer{}, fmt.Errorf("%w: peer address contains a control character", ErrInvalid)
		}
	}
	return Peer{
		network: normalizedNetwork,
		address: normalizedAddress,
	}, nil
}

// Network returns the normalized network or application transport name.
func (peer Peer) Network() string {
	return peer.network
}

// Address returns the transport-provided peer address.
func (peer Peer) Address() string {
	return peer.address
}

// RequestInfoOption configures immutable request facts known before dispatch.
type RequestInfoOption interface {
	applyRequestInfo(*requestInfoOptions) error
}

type requestInfoOptionFunc func(*requestInfoOptions) error

func (f requestInfoOptionFunc) applyRequestInfo(
	options *requestInfoOptions,
) error {
	return f(options)
}

type requestInfoOptions struct {
	peer    Peer
	hasPeer bool
}

// WithPeer records a peer selected or accepted by a transport.
func WithPeer(peer Peer) RequestInfoOption {
	return requestInfoOptionFunc(func(options *requestInfoOptions) error {
		if peer.network == "" || peer.address == "" {
			return fmt.Errorf("%w: peer is invalid", ErrInvalid)
		}
		options.peer = peer
		options.hasPeer = true
		return nil
	})
}

// RequestInfo is an immutable snapshot of transport-neutral request facts.
// Deadline and attempt are refreshed from the current context when the value is
// retrieved so inner timeout/retry middleware remains visible.
type RequestInfo struct {
	operation   Operation
	peer        Peer
	hasPeer     bool
	attempt     int
	deadline    time.Time
	hasDeadline bool
}

// NewRequestInfo validates and constructs request facts for an Operation.
func NewRequestInfo(target Operation, optionList ...RequestInfoOption) (RequestInfo, error) {
	if target.transport == "" || target.service == "" || target.method == "" || !validKind(target.kind) {
		return RequestInfo{}, fmt.Errorf("%w: request operation is invalid", ErrInvalid)
	}
	options := requestInfoOptions{}

	for index, option := range optionList {
		if option == nil {
			return RequestInfo{}, fmt.Errorf("%w: request info option %d is nil", ErrInvalid, index)
		}
		if err := option.applyRequestInfo(&options); err != nil {
			return RequestInfo{}, fmt.Errorf("%w: request info option %d: %v", ErrInvalid, index, err)
		}
	}
	return RequestInfo{
		operation: target,
		peer:      options.peer,
		hasPeer:   options.hasPeer,
		attempt:   1,
	}, nil
}

// Operation returns the stable request operation.
func (i RequestInfo) Operation() Operation {
	return i.operation
}

// Peer returns the request peer when the transport knows it.
func (i RequestInfo) Peer() (Peer, bool) {
	return i.peer, i.hasPeer
}

// Attempt returns the one-based invocation attempt.
func (i RequestInfo) Attempt() int {
	if i.attempt < 1 {
		return 1
	}
	return i.attempt
}

// Deadline returns the effective deadline when one exists.
func (i RequestInfo) Deadline() (time.Time, bool) {
	return i.deadline, i.hasDeadline
}

type requestInfoContextKey struct{}
type attemptContextKey struct{}

// WithRequestInfo attaches the canonical request facts to ctx.
func WithRequestInfo(ctx context.Context, info RequestInfo) context.Context {
	return context.WithValue(ctx, requestInfoContextKey{}, info)
}

// RequestInfoFromContext returns a snapshot refreshed with the effective
// attempt and deadline at the current middleware depth.
func RequestInfoFromContext(ctx context.Context) (RequestInfo, bool) {
	if ctx == nil {
		return RequestInfo{}, false
	}
	info, ok := ctx.Value(requestInfoContextKey{}).(RequestInfo)
	if !ok || info.operation.transport == "" {
		return RequestInfo{}, false
	}
	info.attempt = AttemptFromContext(ctx)
	info.deadline, info.hasDeadline = ctx.Deadline()
	return info, true
}

// WithAttempt records a one-based invocation attempt.
func WithAttempt(ctx context.Context, number int) context.Context {
	if number < 1 {
		number = 1
	}
	return context.WithValue(ctx, attemptContextKey{}, number)
}

// AttemptFromContext returns a one-based attempt and defaults to one.
func AttemptFromContext(ctx context.Context) int {
	if ctx == nil {
		return 1
	}
	number, ok := ctx.Value(attemptContextKey{}).(int)
	if !ok || number < 1 {
		return 1
	}
	return number
}
