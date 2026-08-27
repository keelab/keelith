package gorm

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed GORM configuration binding.
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
