package health

import (
	"fmt"
	"time"
)

const (
	defaultCheckTimeout = 2 * time.Second
	maxCheckTimeout     = time.Minute
	maxCheckCacheTTL    = time.Minute
	maxContributors     = 256
)

// RegistryOption configures bounded health evaluation.
type RegistryOption interface {
	// applyRegistry applies the option to the registry options.
	applyRegistry(*registryOptions) error
}

type registryOptionFunc func(*registryOptions) error

func (f registryOptionFunc) applyRegistry(options *registryOptions) error {
	return f(options)
}

type registryOptions struct {
	checkTimeout time.Duration // checkTimeout is the maximum duration for a single health check.
	cacheTTL     time.Duration // cacheTTL is the maximum duration for a cached Report.
}

// WithCheckTimeout bounds one aggregate health evaluation.
func WithCheckTimeout(timeout time.Duration) RegistryOption {
	return registryOptionFunc(func(options *registryOptions) error {
		if timeout <= 0 || timeout > maxCheckTimeout {
			return fmt.Errorf("check timeout must be within (0, %s]", maxCheckTimeout)
		}
		options.checkTimeout = timeout
		return nil
	})
}

// WithCacheTTL enables a short per-kind Report cache.
func WithCacheTTL(ttl time.Duration) RegistryOption {
	return registryOptionFunc(func(options *registryOptions) error {
		if ttl <= 0 || ttl > maxCheckCacheTTL {
			return fmt.Errorf("cache TTL must be within (0, %s]", maxCheckCacheTTL)
		}
		options.cacheTTL = ttl
		return nil
	})
}

type cachedReport struct {
	report     Report    // report is the cached Report.
	expiresAt  time.Time // expiresAt is the time at which the cache expires.
	generation uint64    // generation is the cache generation number.
}
