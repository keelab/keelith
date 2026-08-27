// Package etcd adapts etcd configuration documents to topology candidates.
package etcd

import (
	etcdconfig "github.com/keelab/contrib/config/etcd"
	"github.com/keelab/contrib/topology/internal/configsource"
	"github.com/keelab/keelith/programmable/topology/control"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcdOptions identify one exact control envelope key.
type etcdOptions = etcdconfig.Options

// NewetcdSource adapts an official linearizable etcd v3 client.
func NewetcdSource(
	client *clientv3.Client,
	options etcdOptions,
) (control.Source, error) {
	backend, err := etcdconfig.New(client, options)
	if err != nil {
		return nil, err
	}
	return configsource.New(backend)
}
