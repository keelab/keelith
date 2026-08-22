// Package invalidation defines versioned cache invalidation events and a
// Worker-compatible processor.
package invalidation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// SchemaVersion is the current JSON invalidation event schema.
	SchemaVersion = 1

	maxNamespaceBytes = 256
	maxKeyBytes       = 1024
	maxEntries        = 128
	maxPayloadBytes   = 256 * 1024
)

var (
	// ErrInvalidEvent reports an unknown schema or malformed event.
	ErrInvalidEvent = errors.New("cache invalidation: invalid event")
	// ErrPayloadTooLarge reports an event beyond the decoder budget.
	ErrPayloadTooLarge = errors.New("cache invalidation: payload too large")
)

// Entry advances one cache key to a datastore-derived monotonic version.
type Entry struct {
	Key     string `json:"key"`
	Version uint64 `json:"version"`
}

// Event groups a bounded set of keys for one cache namespace.
type Event struct {
	Schema    int     `json:"schema"`
	Namespace string  `json:"namespace"`
	Entries   []Entry `json:"entries"`
}

// Validate rejects ambiguous namespaces, duplicate keys, and zero versions.
func (e Event) Validate() error {
	if e.Schema != SchemaVersion || !validIdentity(e.Namespace, maxNamespaceBytes) || len(e.Entries) == 0 || len(e.Entries) > maxEntries {
		return fmt.Errorf("%w: event envelope", ErrInvalidEvent)
	}
	seen := make(map[string]struct{}, len(e.Entries))
	for _, entry := range e.Entries {
		if !validIdentity(entry.Key, maxKeyBytes) || entry.Version == 0 {
			return fmt.Errorf("%w: entry", ErrInvalidEvent)
		}
		if _, exists := seen[entry.Key]; exists {
			return fmt.Errorf("%w: duplicate key", ErrInvalidEvent)
		}
		seen[entry.Key] = struct{}{}
	}
	return nil
}

// Encode validates and serializes one event.
func Encode(event Event) ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("cache invalidation: encode: %w", err)
	}
	if len(payload) > maxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}
	return payload, nil
}

// Decode strictly parses one bounded event and rejects unknown fields.
func Decode(payload []byte) (Event, error) {
	if len(payload) == 0 {
		return Event{}, fmt.Errorf("%w: empty payload", ErrInvalidEvent)
	}
	if len(payload) > maxPayloadBytes {
		return Event{}, ErrPayloadTooLarge
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var event Event
	if err := decoder.Decode(&event); err != nil {
		return Event{}, fmt.Errorf("cache invalidation: decode: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = ErrInvalidEvent
		}
		return Event{}, fmt.Errorf("cache invalidation: trailing data: %w", err)
	}
	if err := event.Validate(); err != nil {
		return Event{}, err
	}
	event.Entries = append([]Entry(nil), event.Entries...)
	return event, nil
}

func validIdentity(value string, maxBytes int) bool {
	if value == "" ||
		len(value) > maxBytes ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
