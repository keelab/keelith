package nacos

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed nacos registry configuration
// binding. All fields define client identity and require replacement on change.
func NewConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[Options],
) (*keelithconfig.Component[Options], error) {
	all := make([]keelithconfig.ComponentOption[Options], 0, len(options)+1)
	all = append(all, keelithconfig.WithComponentValidator(ValidateOptions))
	all = append(all, options...)
	return keelithconfig.NewComponent[Options](name, path, all...)
}
