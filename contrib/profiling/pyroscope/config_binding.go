package pyroscope

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed continuous profiling binding.
// Settings are construction-time. When WatchPassword is enabled, only the
// referenced password material rotates without rebuilding the component.
func NewConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[Config],
) (*keelithconfig.Component[Config], error) {
	all := make(
		[]keelithconfig.ComponentOption[Config],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[Config](name, path, all...)
}
