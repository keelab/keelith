// Package versioned defines provider-neutral immutable configuration revision
// and activation contracts.
package versioned

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultHistoryLimit bounds history reads when a caller omits a limit.
	DefaultHistoryLimit = 50
	// MaxHistoryLimit is the largest activation history page.
	MaxHistoryLimit = 256
	maxActorBytes   = 128
	maxMessageBytes = 512
)

var (
	// ErrInvalidRequest reports malformed stage, activation, or history input.
	ErrInvalidRequest = errors.New("versioned config: invalid request")
	// ErrInvalidRevision reports corrupt or incomplete immutable metadata.
	ErrInvalidRevision = errors.New("versioned config: invalid revision")
	// ErrInvalidActivation reports corrupt or incomplete activation metadata.
	ErrInvalidActivation = errors.New("versioned config: invalid activation")
	// ErrNotFound reports an unknown immutable revision.
	ErrNotFound = errors.New("versioned config: revision not found")
	// ErrNoActive reports a store without an activated revision.
	ErrNoActive = errors.New("versioned config: no active revision")
	// ErrConflict reports a failed generation or backend compare-and-swap.
	ErrConflict = errors.New("versioned config: activation conflict")
	// ErrTampered reports a revision whose content no longer matches its ID.
	ErrTampered = errors.New("versioned config: revision integrity failure")
	// ErrClosed reports an operation after Store or Source shutdown.
	ErrClosed = errors.New("versioned config: closed")
)

// Format identifies a human-authored immutable configuration document.
type Format string

const (
	// FormatJSON identifies a JSON configuration document.
	FormatJSON Format = "json"
	// FormatYAML identifies a YAML configuration document.
	FormatYAML Format = "yaml"
)

// Revision is bounded metadata for one immutable configuration document.
// Content is intentionally returned separately and never belongs in history.
type Revision struct {
	ID        string    `json:"id"`
	Format    Format    `json:"format"`
	Size      int       `json:"size"`
	CreatedAt time.Time `json:"created_at"`
	Actor     string    `json:"actor"`
	Message   string    `json:"message,omitempty"`
}

// Validate verifies immutable revision metadata.
func (revision Revision) Validate() error {
	if !ValidRevisionID(revision.ID) || !revision.Format.Valid() ||
		revision.Size <= 0 || revision.CreatedAt.IsZero() ||
		!validText(revision.Actor, true, maxActorBytes) ||
		!validText(revision.Message, false, maxMessageBytes) {
		return ErrInvalidRevision
	}
	return nil
}

// Activation is one monotonic selection of an immutable Revision.
type Activation struct {
	Generation  uint64    `json:"generation"`
	Revision    string    `json:"revision"`
	Previous    string    `json:"previous,omitempty"`
	ActivatedAt time.Time `json:"activated_at"`
	Actor       string    `json:"actor"`
	Reason      string    `json:"reason"`
}

// Validate verifies an activation record without reading revision content.
func (activation Activation) Validate() error {
	if activation.Generation == 0 ||
		!ValidRevisionID(activation.Revision) ||
		(activation.Previous != "" && !ValidRevisionID(activation.Previous)) ||
		activation.ActivatedAt.IsZero() ||
		!validText(activation.Actor, true, maxActorBytes) ||
		!validText(activation.Reason, true, maxMessageBytes) {
		return ErrInvalidActivation
	}
	return nil
}

// StageRequest creates or reuses an immutable content-addressed Revision.
type StageRequest struct {
	Content []byte
	Format  Format
	Actor   string
	Message string
}

// Validate checks bounded provider-neutral Stage input.
func (r StageRequest) Validate() error {
	if len(r.Content) == 0 || !r.Format.Valid() ||
		!validText(r.Actor, true, maxActorBytes) ||
		!validText(r.Message, false, maxMessageBytes) {
		return ErrInvalidRequest
	}
	return nil
}

// Clone returns an independent request whose content can be safely retained.
func (r StageRequest) Clone() StageRequest {
	r.Content = append([]byte(nil), r.Content...)
	return r
}

// ActivateRequest atomically selects a staged Revision.
type ActivateRequest struct {
	Revision           string
	ExpectedGeneration uint64
	Actor              string
	Reason             string
}

// Validate checks bounded activation input. Generation zero is valid only as
// the expectation for the first activation.
func (r ActivateRequest) Validate() error {
	if !ValidRevisionID(r.Revision) ||
		!validText(r.Actor, true, maxActorBytes) ||
		!validText(r.Reason, true, maxMessageBytes) {
		return ErrInvalidRequest
	}
	return nil
}

// Store stages immutable documents and serializes activation history.
type Store interface {
	Stage(context.Context, StageRequest) (Revision, error)
	Revision(context.Context, string) (Revision, []byte, error)
	Active(context.Context) (Activation, error)
	Activate(context.Context, ActivateRequest) (Activation, error)
	History(context.Context, int) ([]Activation, error)
	Close() error
}

// RevisionID returns the lowercase SHA-256 content address.
func RevisionID(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

// ValidRevisionID reports whether value is one canonical lowercase SHA-256.
func ValidRevisionID(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

// NormalizeHistoryLimit applies the bounded default.
func NormalizeHistoryLimit(limit int) (int, error) {
	if limit == 0 {
		return DefaultHistoryLimit, nil
	}
	if limit < 1 || limit > MaxHistoryLimit {
		return 0, fmt.Errorf("%w: history limit must be between 1 and %d", ErrInvalidRequest, MaxHistoryLimit)
	}
	return limit, nil
}

// Valid reports whether format is supported by the first versioned contract.
func (format Format) Valid() bool {
	return format == FormatJSON || format == FormatYAML
}

func validText(value string, required bool, maxBytes int) bool {
	if !utf8.ValidString(value) || len(value) > maxBytes ||
		(required && strings.TrimSpace(value) == "") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
