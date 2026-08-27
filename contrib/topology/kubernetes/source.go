// Package kubernetes adapts configmap documents to topology candidates.
package kubernetes

import (
	configmap "github.com/keelab/contrib/config/kubernetes"
	"github.com/keelab/contrib/topology/internal/configsource"
	"github.com/keelab/keelith/programmable/topology/control"
	"k8s.io/client-go/rest"
)

// ConfigMapClient is the namespace-scoped official client watch surface.
type ConfigMapClient = configmap.ConfigMapClient

// configmapOptions identify one exact control envelope key.
type configmapOptions = configmap.Options

// NewconfigmapSource adapts a revision-safe configmap List/Watch source.
func NewconfigmapSource(
	client ConfigMapClient,
	options configmapOptions,
) (control.Source, error) {
	backend, err := configmap.New(client, options)
	if err != nil {
		return nil, err
	}
	return configsource.New(backend)
}

// OpenconfigmapSource constructs an explicit REST-config-backed source.
func OpenconfigmapSource(
	config *rest.Config,
	options configmapOptions,
) (control.Source, error) {
	backend, err := configmap.Open(config, options)
	if err != nil {
		return nil, err
	}
	return configsource.New(backend)
}

// OpenInClusterconfigmapSource uses the pod service account.
func OpenInClusterconfigmapSource(options configmapOptions) (control.Source, error) {
	backend, err := configmap.OpenInCluster(options)
	if err != nil {
		return nil, err
	}
	return configsource.New(backend)
}
