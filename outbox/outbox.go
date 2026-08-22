// Package outbox provides a storage- and broker-neutral transactional outbox
// dispatcher.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxIDBytes          = 256
	maxDestinationBytes = 512
	maxKeyBytes         = 1024 * 1024
	maxHeaders          = 64
	maxHeaderBytes      = 8 * 1024
	maxPayloadBytes     = 16 * 1024 * 1024
	maxClaimLimit       = 10_000
)

var (
	// ErrInvalidOption reports invalid messages or dispatcher configuration.
	ErrInvalidOption = errors.New("outbox: invalid option")
	// ErrNotStarted reports Wait before Dispatcher.Start.
	ErrNotStarted = errors.New("outbox: dispatcher not started")
	// ErrReplayConflict reports that the exact terminal replay preconditions
	// no longer match durable state.
	ErrReplayConflict = errors.New("outbox: replay conflict")
)

// Message is one durable event claimed from an outbox Repository.
type Message struct {
	ID          string
	Destination string
	Key         []byte
	Payload     []byte
	Headers     map[string][]byte
	Attempts    int
}

// Validate enforces bounded broker-neutral message identity and payload.
func (message Message) Validate() error {
	if !validIdentity(message.ID, maxIDBytes) ||
		!validIdentity(message.Destination, maxDestinationBytes) ||
		message.Attempts < 0 ||
		len(message.Key) > maxKeyBytes ||
		len(message.Payload) > maxPayloadBytes ||
		len(message.Headers) > maxHeaders {
		return fmt.Errorf("%w: message fields are malformed", ErrInvalidOption)
	}
	headerBytes := 0
	for key, value := range message.Headers {
		if !validIdentity(key, maxIDBytes) {
			return fmt.Errorf("%w: header key is malformed", ErrInvalidOption)
		}
		headerBytes += len(key) + len(value)
		if headerBytes > maxHeaderBytes {
			return fmt.Errorf("%w: headers exceed byte budget", ErrInvalidOption)
		}
	}
	return nil
}

// Clone returns a deep independent message.
func (message Message) Clone() Message {
	headers := make(map[string][]byte, len(message.Headers))
	for key, value := range message.Headers {
		headers[key] = append([]byte(nil), value...)
	}
	return Message{
		ID:          message.ID,
		Destination: message.Destination,
		Key:         append([]byte(nil), message.Key...),
		Payload:     append([]byte(nil), message.Payload...),
		Headers:     headers,
		Attempts:    message.Attempts,
	}
}

// ClaimRequest atomically leases a bounded batch for one Dispatcher owner.
type ClaimRequest struct {
	Owner      string
	Limit      int
	LeaseUntil time.Time
}

// Validate rejects unbounded or already-expired claims.
func (request ClaimRequest) Validate(now time.Time) error {
	if now.IsZero() ||
		!validIdentity(request.Owner, maxIDBytes) ||
		request.Limit <= 0 ||
		request.Limit > maxClaimLimit ||
		!request.LeaseUntil.After(now) {
		return fmt.Errorf("%w: claim request is malformed", ErrInvalidOption)
	}
	return nil
}

// Enqueuer atomically appends an outbox message to a datastore transaction.
//
// Implementations define the transaction type. For example, the PostgreSQL
// adapter implements Enqueuer[*database/sql.Tx].
type Enqueuer[Transaction any] interface {
	Enqueue(context.Context, Transaction, Message, time.Time) error
}

// Repository persists and settles durable outbox rows.
//
// Claim must atomically exclude other owners until LeaseUntil, increment every
// returned Message.Attempts, and never return more than ClaimRequest.Limit.
// Complete and Reschedule must compare Owner so an expired worker cannot
// settle a newer claim.
type Repository interface {
	Claim(context.Context, ClaimRequest) ([]Message, error)
	Complete(context.Context, string, string) error
	Reschedule(
		context.Context,
		string,
		string,
		time.Time,
		bool,
		string,
	) error
}

// Publisher sends one claimed message to a broker or invalidation sink.
type Publisher interface {
	Publish(context.Context, Message) error
}

// FailureClassifier maps publisher errors to bounded low-cardinality reasons.
type FailureClassifier func(error) string

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
