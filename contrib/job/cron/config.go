// Package cron provides a local cron-expression-backed worker.Scheduler.
package cron

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/metadata"
	robfig "github.com/robfig/cron/v3"
)

const maxRetries = 10

var (
	// ErrInvalidOption reports malformed schedule or execution policy.
	ErrInvalidOption = errors.New("cron: invalid option")
	// ErrAlreadyScheduled reports a second Schedule call.
	ErrAlreadyScheduled = errors.New("cron: already scheduled")
	// ErrNotRunning reports a trigger before Schedule or after StopPulling.
	ErrNotRunning = errors.New("cron: not running")
	// ErrOverlap reports a trigger skipped by the forbid-overlap policy.
	ErrOverlap = errors.New("cron: overlapping execution")
	// ErrHandlerPanic reports a recovered job-handler panic.
	ErrHandlerPanic = errors.New("cron: handler panic")
)

// OverlapPolicy controls what happens when a tick arrives during execution.
type OverlapPolicy string

const (
	// OverlapForbid skips the new tick while any execution or retry is active.
	OverlapForbid OverlapPolicy = "forbid"
	// OverlapAllow permits independent executions to run concurrently.
	OverlapAllow OverlapPolicy = "allow"
)

// MisfirePolicy controls historical tick replay.
type MisfirePolicy string

const (
	// MisfireSkip never replays historical ticks or builds an unbounded queue.
	MisfireSkip MisfirePolicy = "skip"
)

// Config defines one local cron schedule and its execution policy.
type Config struct {
	Name       string
	Spec       string
	Location   *time.Location
	Seconds    bool
	Overlap    OverlapPolicy
	Misfire    MisfirePolicy
	MaxRetries int
	Payload    []byte
	Metadata   metadata.Metadata
}

type normalizedConfig struct {
	name       string
	spec       string
	location   *time.Location
	seconds    bool
	overlap    OverlapPolicy
	misfire    MisfirePolicy
	maxRetries int
	payload    []byte
	metadata   metadata.Metadata
}

func normalizeConfig(config Config) (normalizedConfig, error) {
	name := strings.TrimSpace(config.Name)
	if !validName(name) {
		return normalizedConfig{}, fmt.Errorf(
			"%w: name is empty or malformed",
			ErrInvalidOption,
		)
	}
	spec := strings.TrimSpace(config.Spec)
	if spec == "" {
		return normalizedConfig{}, fmt.Errorf(
			"%w: schedule spec is empty",
			ErrInvalidOption,
		)
	}
	location := config.Location
	if location == nil {
		location = time.UTC
	}
	overlap := config.Overlap
	if overlap == "" {
		overlap = OverlapForbid
	}
	switch overlap {
	case OverlapForbid, OverlapAllow:
	default:
		return normalizedConfig{}, fmt.Errorf(
			"%w: overlap policy %q",
			ErrInvalidOption,
			overlap,
		)
	}
	misfire := config.Misfire
	if misfire == "" {
		misfire = MisfireSkip
	}
	if misfire != MisfireSkip {
		return normalizedConfig{}, fmt.Errorf(
			"%w: misfire policy %q",
			ErrInvalidOption,
			misfire,
		)
	}
	if config.MaxRetries < 0 || config.MaxRetries > maxRetries {
		return normalizedConfig{}, fmt.Errorf(
			"%w: max retries must be between 0 and %d",
			ErrInvalidOption,
			maxRetries,
		)
	}
	return normalizedConfig{
		name:       name,
		spec:       spec,
		location:   location,
		seconds:    config.Seconds,
		overlap:    overlap,
		misfire:    misfire,
		maxRetries: config.MaxRetries,
		payload:    append([]byte(nil), config.Payload...),
		metadata:   config.Metadata.Clone(),
	}, nil
}

func newEngine(
	config normalizedConfig,
	job func(),
) (*robfig.Cron, robfig.EntryID, error) {
	options := []robfig.Option{robfig.WithLocation(config.location)}
	if config.seconds {
		options = append(options, robfig.WithSeconds())
	}
	engine := robfig.New(options...)
	entryid, err := engine.AddFunc(config.spec, job)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"%w: parse schedule %q: %w",
			ErrInvalidOption,
			config.spec,
			err,
		)
	}
	return engine, entryid, nil
}

func validName(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
