package http

import (
	"github.com/keelab/keelith/operation"
)

func newRequestInfo(
	target operation.Operation,
	network string,
	address string,
) (operation.RequestInfo, error) {
	if network == "" || address == "" {
		return operation.NewRequestInfo(target)
	}
	peer, err := operation.NewPeer(network, address)
	if err != nil {
		return operation.RequestInfo{}, err
	}
	return operation.NewRequestInfo(target, operation.WithPeer(peer))
}
