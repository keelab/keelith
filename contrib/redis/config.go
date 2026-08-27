package redis

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed Redis runtime binding. Every field
// is construction-time because topology and credentials require a new pool.
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
