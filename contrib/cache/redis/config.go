package redis

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed Redis cache configuration binding.
//
// Prefix and ownership define resource identity, so changes are reported as
// restart-required rather than applied to an active Client.
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
