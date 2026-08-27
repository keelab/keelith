package etcd

import keelithconfig "github.com/keelab/keelith/config"

// NewConfigBinding creates a strict typed etcd registry configuration binding.
//
// Namespace, lease behavior, record budget, and ownership are construction
// boundaries, so changes require replacing the registry Client.
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
