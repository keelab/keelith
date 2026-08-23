package grpc

import (
	"fmt"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	minKeepalivePingInterval = 10 * time.Second
	minKeepaliveTimeout      = time.Second
	minKeepaliveConnection   = time.Minute
	maxKeepaliveDuration     = 24 * time.Hour
)

// ServerKeepaliveConfig controls connection lifetime, server pings, and
// enforcement of client ping frequency.
//
// Zero connection lifetime fields preserve grpc-go's unlimited behavior.
type ServerKeepaliveConfig struct {
	MaxConnectionIdle     time.Duration
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
	PingInterval          time.Duration
	PingTimeout           time.Duration
	MinClientPingInterval time.Duration
	PermitWithoutStream   bool
}

// ClientKeepaliveConfig controls pings on a dynamically dialed grpc-go
// connection.
type ClientKeepaliveConfig struct {
	PingInterval        time.Duration
	PingTimeout         time.Duration
	PermitWithoutStream bool
}

// DefaultServerKeepaliveConfig returns Keelith's production profile defaults.
func DefaultServerKeepaliveConfig() ServerKeepaliveConfig {
	return ServerKeepaliveConfig{
		MaxConnectionIdle:     15 * time.Minute,
		PingInterval:          2 * time.Hour,
		PingTimeout:           20 * time.Second,
		MinClientPingInterval: 30 * time.Second,
		PermitWithoutStream:   false,
	}
}

// DefaultClientKeepaliveConfig returns Keelith's dynamic client defaults.
func DefaultClientKeepaliveConfig() ClientKeepaliveConfig {
	return ClientKeepaliveConfig{
		PingInterval:        time.Minute,
		PingTimeout:         20 * time.Second,
		PermitWithoutStream: false,
	}
}

// WithKeepalive applies validated server connection and ping policy.
func WithKeepalive(config ServerKeepaliveConfig) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if err := validateServerKeepalive(config); err != nil {
			return err
		}
		cloned := config
		options.keepalive = &cloned
		return nil
	})
}

func serverKeepaliveOptions(config ServerKeepaliveConfig) []ggrpc.ServerOption {
	return []ggrpc.ServerOption{
		ggrpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     config.MaxConnectionIdle,
			MaxConnectionAge:      config.MaxConnectionAge,
			MaxConnectionAgeGrace: config.MaxConnectionAgeGrace,
			Time:                  config.PingInterval,
			Timeout:               config.PingTimeout,
		}),
		ggrpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             config.MinClientPingInterval,
			PermitWithoutStream: config.PermitWithoutStream,
		}),
	}
}

func validateServerKeepalive(config ServerKeepaliveConfig) error {
	durations := []struct {
		name  string
		value time.Duration
	}{
		{name: "max connection idle", value: config.MaxConnectionIdle},
		{name: "max connection age", value: config.MaxConnectionAge},
		{
			name:  "max connection age grace",
			value: config.MaxConnectionAgeGrace,
		},
		{name: "ping interval", value: config.PingInterval},
		{name: "ping timeout", value: config.PingTimeout},
		{
			name:  "minimum client ping",
			value: config.MinClientPingInterval,
		},
	}
	for _, duration := range durations {
		if duration.value < 0 || duration.value > maxKeepaliveDuration {
			return fmt.Errorf(
				"%s is outside the supported range",
				duration.name,
			)
		}
	}
	if config.MaxConnectionIdle > 0 &&
		config.MaxConnectionIdle < minKeepaliveConnection {
		return fmt.Errorf("max connection idle must be at least %s", minKeepaliveConnection)
	}
	if config.MaxConnectionAge > 0 &&
		config.MaxConnectionAge < minKeepaliveConnection {
		return fmt.Errorf("max connection age must be at least %s", minKeepaliveConnection)
	}
	if config.MaxConnectionAgeGrace > 0 && config.MaxConnectionAge == 0 {
		return fmt.Errorf("max connection age grace requires max connection age")
	}
	if config.PingInterval > 0 &&
		config.PingInterval < minKeepalivePingInterval {
		return fmt.Errorf("ping interval must be at least %s", minKeepalivePingInterval)
	}
	if config.PingTimeout > 0 && config.PingTimeout < minKeepaliveTimeout {
		return fmt.Errorf("ping timeout must be at least %s", minKeepaliveTimeout)
	}
	if config.PingInterval > 0 &&
		config.PingTimeout > config.PingInterval {
		return fmt.Errorf("ping timeout must not exceed ping interval")
	}
	if config.MinClientPingInterval > 0 &&
		config.MinClientPingInterval < minKeepalivePingInterval {
		return fmt.Errorf(
			"minimum client ping interval must be at least %s",
			minKeepalivePingInterval,
		)
	}
	if config == (ServerKeepaliveConfig{}) {
		return fmt.Errorf("keepalive config is empty")
	}
	return nil
}

func validateClientKeepalive(config ClientKeepaliveConfig) error {
	if config.PingInterval < minKeepalivePingInterval ||
		config.PingInterval > maxKeepaliveDuration {
		return fmt.Errorf(
			"client ping interval must be within [%s, %s]",
			minKeepalivePingInterval,
			maxKeepaliveDuration,
		)
	}
	if config.PingTimeout < minKeepaliveTimeout ||
		config.PingTimeout > config.PingInterval {
		return fmt.Errorf(
			"client ping timeout must be within [%s, ping interval]",
			minKeepaliveTimeout,
		)
	}
	return nil
}
