package cron

import (
	"fmt"
	"strings"
	"time"

	keelithconfig "github.com/keelab/keelith/config"
)

// RuntimeConfig is the strict generated-project local Cron schema. Every
// field is scheduler identity or execution policy and is restart-bound.
type RuntimeConfig struct {
	Name       string        `config:"name"`
	Spec       string        `config:"spec"`
	Timezone   string        `config:"timezone"`
	Seconds    bool          `config:"seconds"`
	Overlap    OverlapPolicy `config:"overlap"`
	Misfire    MisfirePolicy `config:"misfire"`
	MaxRetries int           `config:"maxRetries"`
}

// NewConfigured creates one local Scheduler from strict runtime config.
func NewConfigured(config RuntimeConfig) (*Scheduler, error) {
	schedulerConfig, err := runtimeSchedulerConfig(config)
	if err != nil {
		return nil, err
	}
	return New(schedulerConfig)
}

// ValidateRuntimeConfig validates timezone, schedule syntax, overlap, misfire,
// and retry budgets without starting a Scheduler.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	schedulerConfig, err := runtimeSchedulerConfig(config)
	if err != nil {
		return err
	}
	normalized, err := normalizeConfig(schedulerConfig)
	if err != nil {
		return err
	}
	_, _, err = newEngine(normalized, func() {})
	return err
}

// NewRuntimeConfigBinding creates a strict restart-bound Cron binding.
func NewRuntimeConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[RuntimeConfig],
) (*keelithconfig.Component[RuntimeConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[RuntimeConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateRuntimeConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[RuntimeConfig](name, path, all...)
}

func runtimeSchedulerConfig(config RuntimeConfig) (Config, error) {
	timezone := config.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	if strings.TrimSpace(timezone) != timezone {
		return Config{}, fmt.Errorf(
			"%w: timezone is invalid",
			ErrInvalidOption,
		)
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return Config{}, fmt.Errorf(
			"%w: load timezone %q: %w",
			ErrInvalidOption,
			timezone,
			err,
		)
	}
	return Config{
		Name:       config.Name,
		Spec:       config.Spec,
		Location:   location,
		Seconds:    config.Seconds,
		Overlap:    config.Overlap,
		Misfire:    config.Misfire,
		MaxRetries: config.MaxRetries,
	}, nil
}
