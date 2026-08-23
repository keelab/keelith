package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Format identifies one built-in structured log encoding.
type Format string

const (
	// FormatJSON emits one JSON object per record and is the production default.
	FormatJSON Format = "json"
	// FormatText emits slog's stable key=value representation.
	FormatText Format = "text"
)

// Config defines the process-local logging policy used to construct a Handler.
// The zero value selects info-level JSON without source locations.
type Config struct {
	Level     string
	Format    Format
	AddSource bool
}

// Controller owns the concurrency-safe minimum level of one Handler tree.
// Derived and fan-out loggers retain the same live level gate.
type Controller struct {
	level    slog.LevelVar
	updates  atomic.Uint64
	mu       sync.Mutex
	baseline slog.Level
	revision uint64
	expires  time.Time
	timer    *time.Timer
}

// Status is a value-free logging policy snapshot suitable for diagnostics.
type Status struct {
	Level     string     `json:"level"`
	Baseline  string     `json:"baseline"`
	Revision  uint64     `json:"revision"`
	Updates   uint64     `json:"updates"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

// Update requests one compare-and-swap level transition. ExpectedRevision
// prevents concurrent operators from overwriting each other. TTL zero creates
// a permanent transition and updates the baseline; positive TTL automatically
// restores the prior baseline.
type Update struct {
	Level            string
	ExpectedRevision uint64
	TTL              time.Duration
}

// ErrRevisionConflict reports a stale logging policy update.
var ErrRevisionConflict = errors.New("logging: revision conflict")

// NewHandler constructs a built-in Handler and its dynamic level controller.
// Output ownership remains with the caller and is never closed by Keelith.
func NewHandler(output io.Writer, config Config) (slog.Handler, *Controller, error) {
	if output == nil {
		return nil, nil, fmt.Errorf("%w: logging output is nil", ErrInvalidOption)
	}
	level, err := ParseLevel(config.Level)
	if err != nil {
		return nil, nil, err
	}
	format := config.Format
	if format == "" {
		format = FormatJSON
	}
	controller := &Controller{}
	controller.level.Set(level)
	controller.baseline = level
	options := &slog.HandlerOptions{
		AddSource: config.AddSource,
		Level:     &controller.level,
	}
	var h slog.Handler
	switch format {
	case FormatJSON:
		h = slog.NewJSONHandler(output, options)
	case FormatText:
		h = slog.NewTextHandler(output, options)
	default:
		return nil, nil, fmt.Errorf("%w: unsupported logging format %q", ErrInvalidOption, format)
	}
	return h, controller, nil
}

// Wrap places this Controller's level gate outside an arbitrary Handler tree.
// Use it after fan-out composition so stdout and remote destinations share one
// policy.
func (controller *Controller) Wrap(h slog.Handler) (slog.Handler, error) {
	if controller == nil || nilHandler(h) {
		return nil, ErrInvalidOption
	}
	return &levelHandler{next: h, controller: controller}, nil
}

// ParseLevel validates a stable, case-insensitive slog level name.
func ParseLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%w: unsupported logging level %q", ErrInvalidOption, value)
	}
}

// SetLevel atomically changes the minimum level for future records.
func (controller *Controller) SetLevel(value string) error {
	if controller == nil {
		return fmt.Errorf("%w: logging controller is nil", ErrInvalidOption)
	}
	level, err := ParseLevel(value)
	if err != nil {
		return err
	}
	controller.mu.Lock()
	controller.cancelTimerLocked()
	controller.baseline = level
	controller.level.Set(level)
	controller.revision++
	controller.updates.Add(1)
	controller.mu.Unlock()
	return nil
}

// ApplyBaseline updates the configuration-owned baseline. An active temporary
// Ops override remains effective until expiry, then restores this new baseline.
func (controller *Controller) ApplyBaseline(value string) (Status, error) {
	if controller == nil {
		return Status{}, ErrInvalidOption
	}
	level, err := ParseLevel(value)
	if err != nil {
		return Status{}, err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	controller.baseline = level
	if controller.expires.IsZero() {
		controller.level.Set(level)
	}
	controller.revision++
	controller.updates.Add(1)
	if !controller.expires.IsZero() {
		remaining := time.Until(controller.expires)
		if remaining <= 0 {
			controller.cancelTimerLocked()
			controller.level.Set(level)
		} else {
			if controller.timer != nil {
				controller.timer.Stop()
			}
			revision := controller.revision
			controller.timer = time.AfterFunc(remaining, func() { controller.expire(revision) })
		}
	}
	return controller.statusLocked(), nil
}

// UpdateLevel atomically applies a revision-checked permanent or temporary
// level transition.
func (controller *Controller) UpdateLevel(update Update) (Status, error) {
	if controller == nil || update.TTL < 0 {
		return Status{}, ErrInvalidOption
	}
	level, err := ParseLevel(update.Level)
	if err != nil {
		return Status{}, err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if update.ExpectedRevision != controller.revision {
		return controller.statusLocked(), ErrRevisionConflict
	}
	controller.cancelTimerLocked()
	if update.TTL == 0 {
		controller.baseline = level
		controller.expires = time.Time{}
	} else {
		controller.expires = time.Now().Add(update.TTL)
	}
	controller.level.Set(level)
	controller.revision++
	controller.updates.Add(1)
	if update.TTL > 0 {
		revision := controller.revision
		controller.timer = time.AfterFunc(update.TTL, func() {
			controller.expire(revision)
		})
	}
	return controller.statusLocked(), nil
}

// Restore conditionally restores a previously captured Status. It is intended
// for audited control-plane rollback and refuses to overwrite a newer update.
func (controller *Controller) Restore(previous Status, expectedRevision uint64) (Status, error) {
	if controller == nil {
		return Status{}, ErrInvalidOption
	}
	level, err := ParseLevel(previous.Level)
	if err != nil {
		return Status{}, err
	}
	baseline, err := ParseLevel(previous.Baseline)
	if err != nil {
		return Status{}, err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.revision != expectedRevision {
		return controller.statusLocked(), ErrRevisionConflict
	}
	controller.cancelTimerLocked()
	controller.level.Set(level)
	controller.baseline = baseline
	controller.revision++
	controller.updates.Add(1)
	if previous.ExpiresAt != nil {
		remaining := time.Until(*previous.ExpiresAt)
		if remaining > 0 {
			controller.expires = *previous.ExpiresAt
			revision := controller.revision
			controller.timer = time.AfterFunc(remaining, func() { controller.expire(revision) })
		} else {
			controller.level.Set(baseline)
		}
	}
	return controller.statusLocked(), nil
}

// Level returns the current normalized minimum level.
func (controller *Controller) Level() string {
	if controller == nil {
		return ""
	}
	return controller.level.Level().String()
}

// Status returns a concurrency-safe value-free policy snapshot.
func (controller *Controller) Status() Status {
	if controller == nil {
		return Status{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.statusLocked()
}

// Shutdown cancels a pending temporary-level timer. It is idempotent.
func (controller *Controller) Shutdown() {
	if controller == nil {
		return
	}
	controller.mu.Lock()
	controller.cancelTimerLocked()
	controller.mu.Unlock()
}

func (controller *Controller) statusLocked() Status {
	status := Status{
		Level:    controller.level.Level().String(),
		Baseline: controller.baseline.String(),
		Revision: controller.revision,
		Updates:  controller.updates.Load(),
	}
	if !controller.expires.IsZero() {
		expires := controller.expires
		status.ExpiresAt = &expires
	}
	return status
}

func (controller *Controller) cancelTimerLocked() {
	if controller.timer != nil {
		controller.timer.Stop()
		controller.timer = nil
	}
	controller.expires = time.Time{}
}

func (controller *Controller) expire(revision uint64) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.revision != revision || controller.expires.IsZero() {
		return
	}
	controller.level.Set(controller.baseline)
	controller.expires = time.Time{}
	controller.timer = nil
	controller.revision++
	controller.updates.Add(1)
}

type levelHandler struct {
	next       slog.Handler
	controller *Controller
}

func (h *levelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.controller.level.Level() && h.next.Enabled(ctx, level)
}
func (h *levelHandler) Handle(ctx context.Context, record slog.Record) error {
	if !h.Enabled(ctx, record.Level) {
		return nil
	}
	return h.next.Handle(ctx, record)
}
func (h *levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelHandler{next: h.next.WithAttrs(attrs), controller: h.controller}
}
func (h *levelHandler) WithGroup(name string) slog.Handler {
	return &levelHandler{next: h.next.WithGroup(name), controller: h.controller}
}
