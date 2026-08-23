package cache

import "fmt"

// Option configures Cache internals.
type Option interface {
	apply(*settings) error
}

type optionFunc func(*settings) error

func (f optionFunc) apply(settings *settings) error {
	return f(settings)
}

type settings struct {
	random     Random
	versioning bool
}

// WithRandom replaces the default TTL jitter source.
func WithRandom(random Random) Option {
	return optionFunc(func(settings *settings) error {
		if isNil(random) {
			return fmt.Errorf("random source is nil")
		}
		settings.random = random
		return nil
	})
}

// WithVersioning requires atomic backend invalidation watermarks. Redis keys
// using this option must follow the adapter's cluster-safe key rules.
func WithVersioning() Option {
	return optionFunc(func(settings *settings) error {
		settings.versioning = true
		return nil
	})
}
