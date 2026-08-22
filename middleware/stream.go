package middleware

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrInvalidStreamEntry means a StreamBundle entry is incomplete.
	ErrInvalidStreamEntry = errors.New("middleware: invalid stream bundle entry")
)

// StreamSide identifies which side of a transport emits a lifecycle event.
type StreamSide string

const (
	// StreamSideClient identifies an outbound client stream.
	StreamSideClient StreamSide = "client"
	// StreamSideServer identifies an inbound server stream.
	StreamSideServer StreamSide = "server"
)

// StreamPhase identifies one transport-neutral stream lifecycle boundary.
type StreamPhase string

const (
	// StreamPhaseCreate occurs once before the stream becomes usable.
	StreamPhaseCreate StreamPhase = "create"
	// StreamPhaseSend surrounds one outgoing message attempt.
	StreamPhaseSend StreamPhase = "send"
	// StreamPhaseReceive surrounds one incoming message attempt.
	StreamPhaseReceive StreamPhase = "receive"
	// StreamPhaseFinish occurs once when the stream reaches a terminal state.
	StreamPhaseFinish StreamPhase = "finish"
)

// StreamEvent describes one lifecycle boundary without transport types.
//
// Sequence starts at one independently for Send and Receive; Create and
// Finish use zero. Message is the caller-owned value passed to the transport.
// Middleware must not retain or log it unless the application explicitly
// authorizes that behavior. Error is populated for Finish.
type StreamEvent struct {
	Side     StreamSide
	Phase    StreamPhase
	Sequence uint64
	Message  any
	Error    error
}

// StreamHandler executes one stream lifecycle boundary.
type StreamHandler func(context.Context, StreamEvent) error

// StreamMiddleware wraps lifecycle boundaries for one stream instance.
//
// A StreamBundle chain is instantiated separately for every stream, so the
// returned closure may safely hold state scoped to that stream.
type StreamMiddleware func(StreamHandler) StreamHandler

// StreamEntry names one stream middleware and its configuration source.
type StreamEntry struct {
	Name       string
	Source     string
	Middleware StreamMiddleware
}

// StreamDescription is an immutable StreamBundle diagnostic entry.
type StreamDescription struct {
	Position int
	Name     string
	Source   string
}

// StreamBundle is an immutable, inspectable stream middleware chain.
type StreamBundle struct {
	entries []StreamEntry
}

// NewStreamBundle validates and copies stream middleware entries.
func NewStreamBundle(entries ...StreamEntry) (*StreamBundle, error) {
	snapshot := make([]StreamEntry, 0, len(entries))
	names := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			return nil, fmt.Errorf(
				"%w: entry %d has an empty name",
				ErrInvalidStreamEntry,
				index,
			)
		}
		if entry.Middleware == nil {
			return nil, fmt.Errorf(
				"%w: entry %q has nil middleware",
				ErrInvalidStreamEntry,
				name,
			)
		}
		if _, duplicate := names[name]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate name %q",
				ErrInvalidStreamEntry,
				name,
			)
		}
		names[name] = struct{}{}
		source := strings.TrimSpace(entry.Source)
		if source == "" {
			source = explicitSource
		}
		snapshot = append(snapshot, StreamEntry{
			Name:       name,
			Source:     source,
			Middleware: entry.Middleware,
		})
	}
	return &StreamBundle{entries: snapshot}, nil
}

// CombineStreamBundles creates one auditable StreamBundle by concatenating
// entries in argument order. Nil bundles are ignored and duplicate names are
// rejected.
func CombineStreamBundles(
	bundles ...*StreamBundle,
) (*StreamBundle, error) {
	entries := make([]StreamEntry, 0)
	for _, bundle := range bundles {
		if bundle == nil {
			continue
		}
		entries = append(entries, bundle.entries...)
	}
	return NewStreamBundle(entries...)
}

// Chain composes stream middleware in declaration order.
//
// Call Chain once per stream to preserve stream-scoped middleware state.
func (bundle *StreamBundle) Chain() StreamMiddleware {
	if bundle == nil {
		return ChainStream()
	}
	middlewares := make([]StreamMiddleware, len(bundle.entries))
	for index, entry := range bundle.entries {
		middlewares[index] = entry.Middleware
	}
	return ChainStream(middlewares...)
}

// Describe returns entries in final stream execution order.
func (bundle *StreamBundle) Describe() []StreamDescription {
	if bundle == nil {
		return nil
	}
	result := make([]StreamDescription, len(bundle.entries))
	for index, entry := range bundle.entries {
		result[index] = StreamDescription{
			Position: index,
			Name:     entry.Name,
			Source:   entry.Source,
		}
	}
	return result
}

// ChainStream composes stream middleware in declaration order.
func ChainStream(middlewares ...StreamMiddleware) StreamMiddleware {
	snapshot := append([]StreamMiddleware(nil), middlewares...)
	return func(final StreamHandler) StreamHandler {
		handler := final
		for index := len(snapshot) - 1; index >= 0; index-- {
			handler = snapshot[index](handler)
		}
		return handler
	}
}
