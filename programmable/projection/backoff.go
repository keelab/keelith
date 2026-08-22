package projection

import (
	"errors"
	"math"
	"time"
)

const maximumBackoff = time.Hour
const maximumBackoffAttempt = uint32(63)

var (
	// ErrInvalidBackoff reports a non-positive, unbounded, or unstable policy.
	ErrInvalidBackoff = errors.New("projection: invalid reconnect backoff")
)

// BackoffConfig defines one bounded deterministic exponential schedule.
type BackoffConfig struct {
	Initial    time.Duration
	Maximum    time.Duration
	Multiplier float64
	// Jitter is the symmetric fractional range in [0, 1]. The selected value
	// is deterministic for Seed and attempt; it does not use global randomness.
	Jitter float64
	Seed   uint64
}

// Backoff is an immutable reconnect delay policy.
type Backoff struct {
	initial    time.Duration
	maximum    time.Duration
	multiplier float64
	jitter     float64
	seed       uint64
	configured bool
}

// NewBackoff validates and snapshots one reconnect policy.
func NewBackoff(config BackoffConfig) (Backoff, error) {
	if config.Initial <= 0 ||
		config.Maximum < config.Initial ||
		config.Maximum > maximumBackoff ||
		math.IsNaN(config.Multiplier) ||
		math.IsInf(config.Multiplier, 0) ||
		config.Multiplier < 1 ||
		config.Multiplier > 16 ||
		math.IsNaN(config.Jitter) ||
		math.IsInf(config.Jitter, 0) ||
		config.Jitter < 0 ||
		config.Jitter > 1 {
		return Backoff{}, ErrInvalidBackoff
	}
	return Backoff{
		initial:    config.Initial,
		maximum:    config.Maximum,
		multiplier: config.Multiplier,
		jitter:     config.Jitter,
		seed:       config.Seed,
		configured: true,
	}, nil
}

// Delay returns the deterministic delay for consecutive failure attempt.
func (backoff Backoff) Delay(attempt uint32) time.Duration {
	if !backoff.configured {
		return 0
	}
	if attempt > maximumBackoffAttempt {
		attempt = maximumBackoffAttempt
	}
	base := float64(backoff.initial)
	maximum := float64(backoff.maximum)
	for range attempt {
		if base >= maximum/backoff.multiplier {
			base = maximum
			break
		}
		base *= backoff.multiplier
	}
	if backoff.jitter != 0 {
		unit := deterministicUnit(backoff.seed, attempt)
		factor := 1 - backoff.jitter + 2*backoff.jitter*unit
		base *= factor
	}
	if base > maximum {
		base = maximum
	}
	if base < 1 {
		base = 1
	}
	return time.Duration(base)
}

func defaultBackoff(seed uint64) Backoff {
	backoff, _ := NewBackoff(BackoffConfig{
		Initial:    defaultReconnectDelay,
		Maximum:    30 * time.Second,
		Multiplier: 2,
		Jitter:     0.2,
		Seed:       seed,
	})
	return backoff
}

func deterministicBackoffSeed(value string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	result := offset
	for index := range len(value) {
		result ^= uint64(value[index])
		result *= prime
	}
	return result
}

func deterministicUnit(seed uint64, attempt uint32) float64 {
	value := seed + 0x9e3779b97f4a7c15*(uint64(attempt)+1)
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	const denominator = float64(uint64(1) << 53)
	return float64(value>>11) / denominator
}
