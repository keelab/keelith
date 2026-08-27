package kubernetes

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed Kubernetes coordination binding.
//
// Namespace, identity, and logical key mappings are construction-time fields;
// changes require replacing the Coordinator.
func NewConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[Options],
) (*keelithconfig.Component[Options], error) {
	all := make(
		[]keelithconfig.ComponentOption[Options],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateOptions),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[Options](name, path, all...)
}
