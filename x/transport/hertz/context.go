package hertz

import (
	"net"
	"sort"
	"strings"

	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/keelab/keelith/operation"
)

func requestInfo(
	target operation.Operation,
	address net.Addr,
) (operation.RequestInfo, error) {
	if address == nil {
		return operation.NewRequestInfo(target)
	}
	peer, err := operation.NewPeer(address.Network(), address.String())
	if err != nil {
		return operation.RequestInfo{}, err
	}
	return operation.NewRequestInfo(target, operation.WithPeer(peer))
}

type hertzMetadataCarrier struct {
	header *protocol.RequestHeader
}

func (carrier hertzMetadataCarrier) Values(key string) []string {
	if carrier.header == nil {
		return nil
	}
	values := carrier.header.PeekAll(key)
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	return result
}

func (carrier hertzMetadataCarrier) Set(key string, values []string) {
	if carrier.header == nil {
		return
	}
	carrier.header.Del(key)
	for _, value := range values {
		carrier.header.Add(key, value)
	}
}

type hertzPropagationCarrier struct {
	header *protocol.RequestHeader
}

func (carrier hertzPropagationCarrier) Get(key string) string {
	if carrier.header == nil {
		return ""
	}
	return string(carrier.header.Peek(key))
}

func (carrier hertzPropagationCarrier) Set(key, value string) {
	if carrier.header != nil {
		carrier.header.Set(key, value)
	}
}

func (carrier hertzPropagationCarrier) Keys() []string {
	if carrier.header == nil {
		return nil
	}
	seen := make(map[string]struct{})
	carrier.header.VisitAll(func(key, _ []byte) {
		seen[strings.ToLower(string(key))] = struct{}{}
	})
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type hertzResponseMetadataCarrier struct {
	header *protocol.ResponseHeader
}

func (carrier hertzResponseMetadataCarrier) Values(key string) []string {
	if carrier.header == nil {
		return nil
	}
	values := carrier.header.PeekAll(key)
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	return result
}

func (carrier hertzResponseMetadataCarrier) Set(
	key string,
	values []string,
) {
	if carrier.header == nil {
		return
	}
	carrier.header.Del(key)
	for _, value := range values {
		carrier.header.Add(key, value)
	}
}
