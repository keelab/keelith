package control

import configfile "github.com/keelab/keelith/config/file"

// NewFileSource adapts a complete JSON/YAML file to revisioned candidates.
func NewFileSource(path string, options ...configfile.Option) (Source, error) {
	backend, err := configfile.New(path, options...)
	if err != nil {
		return nil, err
	}
	return newConfigSource(backend)
}
