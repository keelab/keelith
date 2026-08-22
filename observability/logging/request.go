package logging

import (
	"context"
	stderrors "errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/keelab/keelith/correlation"
	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/observability/completion"
	"github.com/keelab/keelith/operation"
)

const (
	defaultSlowThreshold   = 500 * time.Millisecond
	maxSlowThreshold       = time.Hour
	maxSuccessSampleEvery  = 1_000_000
	maxLoggedIdentityBytes = 256
	maxLoggedReasonBytes   = 128
)

// RequestLogConfig controls low-volume, payload-free completion logs.
type RequestLogConfig struct {
	// SuccessSampleEvery logs every Nth successful non-slow invocation. Zero
	// disables ordinary success logs; one logs every success.
	SuccessSampleEvery uint64

	// SlowThreshold promotes successful invocations at or above the threshold
	// to a warning. Zero uses 500ms unless DisableSlowLogs is true.
	SlowThreshold time.Duration

	// DisableSlowLogs disables the independent slow-success path.
	DisableSlowLogs bool
}

// RequestLogger emits bounded completion records for unary and stream calls.
// It never logs request/response payloads, metadata, peer addresses, or error
// messages.
type RequestLogger struct {
	logger             *slog.Logger
	successSampleEvery uint64
	slowThreshold      time.Duration
	disableSlowLogs    bool
	sequence           atomic.Uint64
}

// NewRequestLogger validates and creates an App-scoped request logger.
func NewRequestLogger(
	logger *slog.Logger,
	config RequestLogConfig,
) (*RequestLogger, error) {
	if logger == nil {
		return nil, ErrInvalidOption
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	slowThreshold := config.SlowThreshold
	if slowThreshold == 0 && !config.DisableSlowLogs {
		slowThreshold = defaultSlowThreshold
	}
	return &RequestLogger{
		logger:             logger,
		successSampleEvery: config.SuccessSampleEvery,
		slowThreshold:      slowThreshold,
		disableSlowLogs:    config.DisableSlowLogs,
	}, nil
}

// Validate checks request log budgets without constructing a logger.
func (config RequestLogConfig) Validate() error {
	if config.SuccessSampleEvery > maxSuccessSampleEvery {
		return ErrInvalidOption
	}
	if config.SlowThreshold < 0 || config.SlowThreshold > maxSlowThreshold {
		return ErrInvalidOption
	}
	if config.DisableSlowLogs && config.SlowThreshold != 0 {
		return ErrInvalidOption
	}
	return nil
}

// Middleware logs one completion record after a unary invocation finishes.
func (logger *RequestLogger) Middleware() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		if logger == nil || logger.logger == nil {
			return next
		}
		return func(ctx context.Context, request any) (any, error) {
			started := time.Now()
			response, err := next(ctx, request)
			logger.complete(ctx, err, time.Since(started))
			return response, err
		}
	}
}

// StreamMiddleware logs only the terminal stream event. Message values are
// never retained or inspected. The returned closure is scoped to one stream.
func (logger *RequestLogger) StreamMiddleware() middleware.StreamMiddleware {
	return func(next middleware.StreamHandler) middleware.StreamHandler {
		if logger == nil || logger.logger == nil {
			return next
		}
		var state streamLogState
		return func(ctx context.Context, event middleware.StreamEvent) error {
			started := state.start()
			err := next(ctx, event)
			if event.Phase == middleware.StreamPhaseFinish && state.finish() {
				logger.complete(
					ctx,
					stderrors.Join(event.Error, err),
					time.Since(started),
				)
			}
			return err
		}
	}
}

func (logger *RequestLogger) complete(
	ctx context.Context,
	err error,
	duration time.Duration,
) {
	info, ok := operation.RequestInfoFromContext(ctx)
	if !ok {
		return
	}
	decision := logger.decision(err, duration)
	if !decision.emit {
		return
	}
	target := info.Operation()
	attributes := []slog.Attr{
		slog.String("event", "request.completed"),
		slog.String("transport", boundedIdentity(target.Transport())),
		slog.String("service", boundedIdentity(target.Service())),
		slog.String("method", boundedIdentity(target.Method())),
		slog.String("kind", boundedIdentity(string(target.Kind()))),
		slog.Int("attempt", info.Attempt()),
		slog.Float64(
			"duration_ms",
			float64(duration.Nanoseconds())/float64(time.Millisecond),
		),
		slog.String("outcome", decision.outcome),
	}
	if decision.errorClass != "" {
		attributes = append(
			attributes,
			slog.String("error.class", decision.errorClass),
		)
	}
	if requestID, ok := correlation.RequestID(ctx); ok {
		attributes = append(attributes, slog.String("request.id", requestID))
	}
	attributes = append(attributes, safeErrorAttributes(err)...)
	logger.logger.LogAttrs(ctx, decision.level, "request completed", attributes...)
}

func (logger *RequestLogger) decision(
	err error,
	duration time.Duration,
) requestLogDecision {
	if err != nil {
		result := completion.Classify(err)
		return requestLogDecision{
			level:      slogLevel(result.Severity()),
			outcome:    string(result.Outcome()),
			errorClass: string(result.ErrorClass()),
			emit:       true,
		}
	}
	if !logger.disableSlowLogs && duration >= logger.slowThreshold {
		return requestLogDecision{
			level:   slog.LevelWarn,
			outcome: "slow",
			emit:    true,
		}
	}
	if logger.successSampleEvery == 0 {
		return requestLogDecision{level: slog.LevelInfo, outcome: "ok"}
	}
	number := logger.sequence.Add(1)
	return requestLogDecision{
		level:   slog.LevelInfo,
		outcome: "ok",
		emit:    number%logger.successSampleEvery == 0,
	}
}

type requestLogDecision struct {
	level      slog.Level
	outcome    string
	errorClass string
	emit       bool
}

func slogLevel(severity completion.Severity) slog.Level {
	switch severity {
	case completion.SeverityWarn:
		return slog.LevelWarn
	case completion.SeverityError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func safeErrorAttributes(err error) []slog.Attr {
	if err == nil {
		return nil
	}
	var applicationError *kerrors.Error
	if stderrors.As(err, &applicationError) {
		return []slog.Attr{
			slog.Int64("error.code", int64(applicationError.Code())),
			slog.String("error.reason", safeReason(applicationError.Reason())),
		}
	}
	switch {
	case stderrors.Is(err, context.Canceled):
		return []slog.Attr{slog.String("error.kind", "canceled")}
	case stderrors.Is(err, context.DeadlineExceeded):
		return []slog.Attr{slog.String("error.kind", "deadline_exceeded")}
	default:
		return []slog.Attr{slog.String("error.kind", "internal")}
	}
}

func safeReason(value string) string {
	if value == "" || len(value) > maxLoggedReasonBytes {
		return "INTERNAL"
	}
	for _, character := range value {
		if character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' {
			continue
		}
		return "INTERNAL"
	}
	return value
}

func boundedIdentity(value string) string {
	value = strings.ToValidUTF8(value, "")
	if len(value) <= maxLoggedIdentityBytes {
		return value
	}
	limit := maxLoggedIdentityBytes - 3
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + "..."
}

type streamLogState struct {
	mu       sync.Mutex
	started  time.Time
	finished bool
}

func (state *streamLogState) start() time.Time {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.started.IsZero() {
		state.started = time.Now()
	}
	return state.started
}

func (state *streamLogState) finish() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.finished {
		return false
	}
	state.finished = true
	return true
}
