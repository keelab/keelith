// Package sse defines transport-neutral Server-Sent Events values and
// bounded wire rendering shared by HTTP transport profiles.
package sse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	defaultHeartbeat = 15 * time.Second
	maxIdentityBytes = 256
	minHeartbeat     = time.Second
	maxHeartbeat     = 5 * time.Minute

	// DefaultEventBytes is the default fully rendered event budget.
	DefaultEventBytes = 64 * 1024
	// MaximumEventBytes is the largest supported event budget.
	MaximumEventBytes = 1024 * 1024
	// MinimumRetry is the smallest accepted browser retry delay.
	MinimumRetry = 100 * time.Millisecond
	// MaximumRetry is the largest accepted browser retry delay.
	MaximumRetry = time.Hour

	// ContentType is the canonical SSE response media type.
	ContentType = "text/event-stream; charset=utf-8"
	// CacheControl prevents intermediaries from caching an event stream.
	CacheControl = "no-cache"
	// HeartbeatComment is the fixed value used for idle keepalives.
	HeartbeatComment = ": keelith-keepalive\n\n"
)

var (
	// ErrInvalid reports a malformed request, event, stream, or configuration.
	ErrInvalid = errors.New("sse: invalid server-sent events")
	// ErrSource reports a terminal producer error after response commitment.
	ErrSource = errors.New("sse: event source failed")
	// ErrEventTooLarge reports one rendered event beyond its configured budget.
	ErrEventTooLarge = errors.New("sse: event too large")
)

// EventSpec is the construction input for one immutable event.
type EventSpec struct {
	ID    string
	Name  string
	Data  string
	Retry time.Duration
}

// Event is one immutable Server-Sent Event.
type Event struct {
	id    string
	name  string
	data  string
	retry time.Duration
}

// Request is the immutable reconnection input for one subscription.
type Request struct {
	lastEventID string
}

// NewRequest validates a Last-Event-ID value.
func NewRequest(lastEventID string) (Request, error) {
	if !validID(lastEventID) {
		return Request{}, fmt.Errorf("%w: invalid last-event-id", ErrInvalid)
	}
	return Request{lastEventID: lastEventID}, nil
}

// LastEventID returns the optional browser reconnection cursor.
func (r Request) LastEventID() string { return r.lastEventID }

// NewEvent validates and snapshots one event.
func NewEvent(spec EventSpec) (Event, error) {
	if !validID(spec.ID) ||
		!validName(spec.Name) ||
		!utf8.ValidString(spec.Data) ||
		strings.ContainsRune(spec.Data, '\x00') {
		return Event{}, ErrInvalid
	}
	if spec.Retry != 0 &&
		(spec.Retry < MinimumRetry ||
			spec.Retry > MaximumRetry ||
			spec.Retry%time.Millisecond != 0) {
		return Event{}, fmt.Errorf(
			"%w: retry is outside supported bounds",
			ErrInvalid,
		)
	}
	return Event{
		id:    spec.ID,
		name:  spec.Name,
		data:  spec.Data,
		retry: spec.Retry,
	}, nil
}

// NewJSONEvent JSON-encodes value as one immutable event.
func NewJSONEvent(
	id string,
	name string,
	value any,
	retry time.Duration,
) (Event, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return Event{}, fmt.Errorf("%w: encode json data", ErrInvalid)
	}
	return NewEvent(EventSpec{
		ID: id, Name: name, Data: string(payload), Retry: retry,
	})
}

// NewProtoEvent protojson-encodes message as one immutable event.
func NewProtoEvent(
	id string,
	name string,
	message proto.Message,
	retry time.Duration,
) (Event, error) {
	if isNilProto(message) {
		return Event{}, fmt.Errorf("%w: Proto message is nil", ErrInvalid)
	}
	payload, err := (protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
	}).Marshal(message)
	if err != nil {
		return Event{}, fmt.Errorf("%w: encode Proto data", ErrInvalid)
	}
	return NewEvent(EventSpec{
		ID: id, Name: name, Data: string(payload), Retry: retry,
	})
}

// ID returns the reconnection cursor.
func (event Event) ID() string { return event.id }

// Name returns the optional event type.
func (event Event) Name() string { return event.name }

// Data returns immutable UTF-8 event data.
func (event Event) Data() string { return event.data }

// Retry returns the optional browser reconnection delay.
func (event Event) Retry() time.Duration { return event.retry }

// Stream is a response backed by events and optional terminal failures.
type Stream struct {
	events   <-chan Event
	failures <-chan error
}

// NewStream constructs a streaming response.
func NewStream(
	events <-chan Event,
	failures <-chan error,
) (Stream, error) {
	if events == nil {
		return Stream{}, fmt.Errorf("%w: event channel is nil", ErrInvalid)
	}
	return Stream{events: events, failures: failures}, nil
}

// Events returns the immutable event source.
func (stream Stream) Events() <-chan Event { return stream.events }

// Failures returns the optional terminal failure source.
func (stream Stream) Failures() <-chan error { return stream.failures }

// Config controls bounded SSE encoding.
type Config struct {
	HeartbeatInterval time.Duration
	DisableHeartbeat  bool
	MaxEventBytes     int
}

// Settings is one validated Config snapshot.
type Settings struct {
	heartbeat        time.Duration
	disableHeartbeat bool
	maxEventBytes    int
}

// Resolve validates configuration and applies bounded defaults.
func Resolve(config Config) (Settings, error) {
	heartbeat := config.HeartbeatInterval
	if heartbeat == 0 {
		heartbeat = defaultHeartbeat
	}
	if config.DisableHeartbeat && config.HeartbeatInterval != 0 {
		return Settings{}, fmt.Errorf(
			"%w: disabled heartbeat has an interval",
			ErrInvalid,
		)
	}
	if !config.DisableHeartbeat &&
		(heartbeat < minHeartbeat || heartbeat > maxHeartbeat) {
		return Settings{}, fmt.Errorf(
			"%w: heartbeat is outside supported bounds",
			ErrInvalid,
		)
	}
	maximum := config.MaxEventBytes
	if maximum == 0 {
		maximum = DefaultEventBytes
	}
	if maximum < 256 || maximum > MaximumEventBytes {
		return Settings{}, fmt.Errorf(
			"%w: event size is outside supported bounds",
			ErrInvalid,
		)
	}
	return Settings{
		heartbeat:        heartbeat,
		disableHeartbeat: config.DisableHeartbeat,
		maxEventBytes:    maximum,
	}, nil
}

// HeartbeatInterval returns the resolved keepalive interval.
func (settings Settings) HeartbeatInterval() time.Duration {
	return settings.heartbeat
}

// HeartbeatEnabled reports whether keepalives should be emitted.
func (settings Settings) HeartbeatEnabled() bool {
	return !settings.disableHeartbeat
}

// MaximumEventBytes returns the resolved per-event wire budget.
func (settings Settings) MaximumEventBytes() int {
	return settings.maxEventBytes
}

// Render validates and renders one bounded event.
func Render(event Event, maximum int) ([]byte, error) {
	if maximum < 1 ||
		!validID(event.id) ||
		!validName(event.name) ||
		!utf8.ValidString(event.data) ||
		strings.ContainsRune(event.data, '\x00') ||
		event.retry != 0 &&
			(event.retry < MinimumRetry ||
				event.retry > MaximumRetry ||
				event.retry%time.Millisecond != 0) {
		return nil, ErrInvalid
	}
	var payload bytes.Buffer
	if event.id != "" {
		payload.WriteString("id: ")
		payload.WriteString(event.id)
		payload.WriteByte('\n')
	}
	if event.name != "" {
		payload.WriteString("event: ")
		payload.WriteString(event.name)
		payload.WriteByte('\n')
	}
	if event.retry != 0 {
		payload.WriteString("retry: ")
		payload.WriteString(strconv.FormatInt(
			event.retry.Milliseconds(),
			10,
		))
		payload.WriteByte('\n')
	}
	data := strings.ReplaceAll(event.data, "\r\n", "\n")
	data = strings.ReplaceAll(data, "\r", "\n")
	for _, line := range strings.Split(data, "\n") {
		payload.WriteString("data: ")
		payload.WriteString(line)
		payload.WriteByte('\n')
		if payload.Len() > maximum {
			return nil, ErrEventTooLarge
		}
	}
	payload.WriteByte('\n')
	if payload.Len() > maximum {
		return nil, ErrEventTooLarge
	}
	return payload.Bytes(), nil
}

func validID(value string) bool {
	return len(value) <= maxIdentityBytes &&
		utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\r\n\x00")
}

// ValidID reports whether value is a safe SSE cursor.
func ValidID(value string) bool { return validID(value) }

func validName(value string) bool {
	if len(value) > maxIdentityBytes {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.',
			r == '_',
			r == '-':
		default:
			return false
		}
	}
	return true
}

// ValidName reports whether value is a bounded event type.
func ValidName(value string) bool { return validName(value) }

func isNilProto(message proto.Message) bool {
	if message == nil {
		return true
	}
	value := message.ProtoReflect()
	return !value.IsValid()
}
