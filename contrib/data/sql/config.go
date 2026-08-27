package sql

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed SQL configuration binding.
//
// Load the binding through config.Manager, construct Database from Current,
// then attach hot pool updates with binding.BindApply(database.ApplyConfig).
func NewConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[Config],
) (*keelithconfig.Component[Config], error) {
	all := make([]keelithconfig.ComponentOption[Config], 0, len(options)+1)
	all = append(all, keelithconfig.WithComponentValidator(ValidateConfig))
	all = append(all, options...)
	return keelithconfig.NewComponent[Config](name, path, all...)
}

// NewConnectionConfigBinding creates a strict secret-safe SQL connection
// binding. Driver and DSN reference are restart fields; nested pool fields use
// Config's reload tags.
func NewConnectionConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[ConnectionConfig],
) (*keelithconfig.Component[ConnectionConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[ConnectionConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateConnectionConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[ConnectionConfig](name, path, all...)
}
