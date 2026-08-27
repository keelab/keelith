// Package cpu provides bounded, explicitly enabled runtime CPU profiling and
// low-cardinality Operation labels.
package cpu

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/keelab/contrib/profiling/internal/cpulease"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

const (
	defaultDuration = 10 * time.Second
	defaultMaximum  = 30 * time.Second
	maximumAllowed  = 2 * time.Minute
	minimumDuration = 100 * time.Millisecond
	defaultMaxBytes = 32 * 1024 * 1024
	maxAllowedBytes = 256 * 1024 * 1024
)

var (
	// ErrInvalidOption reports malformed duration or memory budgets.
	ErrInvalidOption = errors.New("cpu profiling: invalid option")
	// ErrBusy reports another process-wide CPU capture in progress.
	ErrBusy = errors.New("cpu profiling: another capture is active")
	// ErrTooLarge reports a profile beyond the configured memory budget.
	ErrTooLarge = errors.New("cpu profiling: profile is too large")
)

// Config controls one explicit CPU profile endpoint.
type Config struct {
	DefaultDuration time.Duration `config:"default_duration"`
	MaxDuration     time.Duration `config:"max_duration"`
	MaxBytes        int           `config:"max_bytes"`
}

// Description is a value-free aggregate snapshot.
type Description struct {
	Active   bool
	Captures uint64
	Rejected uint64
	Failures uint64
}

// Controller captures at most one process-wide CPU profile at a time.
type Controller struct {
	defaultDuration time.Duration
	maxDuration     time.Duration
	maxBytes        int

	mu       sync.Mutex
	active   bool
	captures uint64
	rejected uint64
	failures uint64
}

// New constructs a bounded Controller.
func New(config Config) (*Controller, error) {
	normalized, err := NormalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return &Controller{
		defaultDuration: normalized.DefaultDuration,
		maxDuration:     normalized.MaxDuration,
		maxBytes:        normalized.MaxBytes,
	}, nil
}

// NormalizeConfig applies bounded defaults.
func NormalizeConfig(input Config) (Config, error) {
	config := input
	if config.DefaultDuration == 0 {
		config.DefaultDuration = defaultDuration
	}
	if config.MaxDuration == 0 {
		config.MaxDuration = defaultMaximum
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = defaultMaxBytes
	}
	if config.DefaultDuration < minimumDuration ||
		config.MaxDuration < config.DefaultDuration ||
		config.MaxDuration > maximumAllowed ||
		config.MaxBytes < 1 ||
		config.MaxBytes > maxAllowedBytes {
		return Config{}, fmt.Errorf(
			"%w: duration or memory budget",
			ErrInvalidOption,
		)
	}
	return config, nil
}

// ValidateConfig validates resource budgets without constructing a Controller.
func ValidateConfig(config Config) error {
	_, err := NormalizeConfig(config)
	return err
}

// Capture records one in-memory CPU profile.
func (controller *Controller) Capture(
	ctx context.Context,
	duration time.Duration,
) ([]byte, error) {
	if controller == nil || ctx == nil {
		return nil, fmt.Errorf(
			"%w: controller or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if duration == 0 {
		duration = controller.defaultDuration
	}
	if duration < minimumDuration ||
		duration > controller.maxDuration {
		controller.recordRejected()
		return nil, fmt.Errorf(
			"%w: duration must be within %s..%s",
			ErrInvalidOption,
			minimumDuration,
			controller.maxDuration,
		)
	}
	lease, acquired := cpulease.TryAcquire()
	if !acquired {
		controller.recordRejected()
		return nil, ErrBusy
	}
	defer lease.Release()

	controller.mu.Lock()
	controller.active = true
	controller.mu.Unlock()
	defer func() {
		controller.mu.Lock()
		controller.active = false
		controller.mu.Unlock()
	}()

	output := newBoundedBuffer(controller.maxBytes)
	if err := pprof.StartCPUProfile(output); err != nil {
		controller.recordFailure()
		return nil, fmt.Errorf("cpu profiling: start: %w", err)
	}
	timer := time.NewTimer(duration)
	var captureErr error
	select {
	case <-timer.C:
	case <-ctx.Done():
		captureErr = context.Cause(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	pprof.StopCPUProfile()
	if captureErr == nil {
		captureErr = output.Err()
	}
	if captureErr != nil {
		controller.recordFailure()
		return nil, captureErr
	}
	content := output.Bytes()
	if len(content) == 0 {
		controller.recordFailure()
		return nil, errors.New("cpu profiling: empty profile")
	}
	controller.mu.Lock()
	controller.captures++
	controller.mu.Unlock()
	return content, nil
}

// Servehttp exposes standard GET /debug/pprof/profile?seconds=N semantics.
//
// Access control remains the Ops Server's responsibility.
func (controller *Controller) ServeHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	if controller == nil {
		http.Error(
			writer,
			"CPU profile unavailable",
			http.StatusServiceUnavailable,
		)
		return
	}
	if request == nil || request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	duration := controller.defaultDuration
	if raw := request.URL.Query().Get("seconds"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds <= 0 {
			controller.recordRejected()
			http.Error(writer, "invalid seconds", http.StatusBadRequest)
			return
		}
		duration = time.Duration(seconds) * time.Second
	}
	content, err := controller.Capture(request.Context(), duration)
	if err != nil {
		switch {
		case errors.Is(err, ErrBusy):
			http.Error(writer, "CPU profile is busy", http.StatusConflict)
		case errors.Is(err, ErrInvalidOption):
			http.Error(writer, "invalid profile duration", http.StatusBadRequest)
		case errors.Is(err, context.Canceled),
			errors.Is(err, context.DeadlineExceeded):
			http.Error(writer, "profile request canceled", http.StatusRequestTimeout)
		default:
			http.Error(writer, "CPU profile unavailable", http.StatusInternalServerError)
		}
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set(
		"Content-Disposition",
		`attachment; filename="cpu.pprof"`,
	)
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(content)
}

// Description returns process-local counters without request, profile, or
// Operation details.
func (controller *Controller) Description() Description {
	if controller == nil {
		return Description{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return Description{
		Active:   controller.active,
		Captures: controller.captures,
		Rejected: controller.rejected,
		Failures: controller.failures,
	}
}

// Middleware adds bounded service, method, transport, and kind labels while a
// unary/consumer/job Handler executes. It does not record request values.
func Middleware() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(
			ctx context.Context,
			request any,
		) (response any, err error) {
			target, ok := operation.FromContext(ctx)
			if !ok {
				return next(ctx, request)
			}
			labels := pprof.Labels(
				"keelith.service", target.Service(),
				"keelith.method", target.Method(),
				"keelith.transport", target.Transport(),
				"keelith.kind", string(target.Kind()),
			)
			pprof.Do(ctx, labels, func(labeled context.Context) {
				response, err = next(labeled, request)
			})
			return response, err
		}
	}
}

func (controller *Controller) recordRejected() {
	if controller == nil {
		return
	}
	controller.mu.Lock()
	controller.rejected++
	controller.mu.Unlock()
}

func (controller *Controller) recordFailure() {
	controller.mu.Lock()
	controller.failures++
	controller.mu.Unlock()
}

type boundedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	limit  int
	err    error
}

func newBoundedBuffer(limit int) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (buffer *boundedBuffer) Write(content []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if buffer.err != nil {
		return 0, buffer.err
	}
	if len(content) > buffer.limit-buffer.buffer.Len() {
		buffer.err = ErrTooLarge
		return 0, buffer.err
	}
	return buffer.buffer.Write(content)
}

func (buffer *boundedBuffer) Err() error {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.err
}

func (buffer *boundedBuffer) Bytes() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return append([]byte(nil), buffer.buffer.Bytes()...)
}
