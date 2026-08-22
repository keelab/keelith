package continuation

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidTimer reports a missing or structurally invalid timer deadline.
	ErrInvalidTimer = errors.New("continuation: invalid timer")
	// ErrTimerNotReady reports an early acquisition of a durable timer.
	ErrTimerNotReady = errors.New("continuation: timer is not ready")
)

// TimerNotReadyError carries the bounded recovery deadline for an early claim.
type TimerNotReadyError struct {
	ReadyAt time.Time
}

func (err *TimerNotReadyError) Error() string {
	if err == nil || err.ReadyAt.IsZero() {
		return ErrTimerNotReady.Error()
	}
	return fmt.Sprintf("%v: ready at %s", ErrTimerNotReady, err.ReadyAt.UTC().Format(time.RFC3339Nano))
}

// Unwrap exposes the stable timer-not-ready category.
func (err *TimerNotReadyError) Unwrap() error { return ErrTimerNotReady }

// SleepUntil constructs a durable suspension with an absolute wake time. The
// Store's authoritative clock, rather than an executor clock, decides when the
// resulting revision becomes claimable.
func SleepUntil(readyAt time.Time, frames ...Frame) Transition {
	return Transition{
		status:  StatusSuspended,
		frames:  cloneFrames(frames),
		readyAt: normalizeReadyAt(readyAt),
		timer:   true,
	}
}

// normalizeReadyAt rounds upward to PostgreSQL's microsecond resolution so a
// persisted deadline can never become eligible before the requested instant.
func normalizeReadyAt(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	value = value.Round(0).UTC()
	truncated := value.Truncate(time.Microsecond)
	if truncated.Equal(value) {
		return value
	}
	return truncated.Add(time.Microsecond)
}

// TimerNotReady returns a typed early-claim error when snapshot is a timer that
// is not yet due according to the supplied store-authoritative time.
func TimerNotReady(snapshot Snapshot, now time.Time) error {
	if snapshot.TimerDue(now) {
		return nil
	}
	return &TimerNotReadyError{ReadyAt: snapshot.ReadyAt()}
}
