package genericrpc

import (
	"context"
	"fmt"

	dproto "github.com/cloudwego/dynamicgo/proto"
	"github.com/cloudwego/kitex/pkg/generic"
)

func parseProtoDescriptor(
	ctx context.Context,
	mainPath string,
	mainContent string,
	includes map[string]string,
) (
	descriptor *dproto.ServiceDescriptor,
	err error,
) {
	defer func() {
		if recover() != nil {
			descriptor = nil
			err = fmt.Errorf(
				"%w: Proto descriptor parser failed",
				ErrInvalidConfig,
			)
		}
	}()
	return (dproto.Options{}).NewDesccriptorFromContent(
		ctx,
		mainPath,
		mainContent,
		includes,
	)
}

func newProtoProvider(
	ctx context.Context,
	mainPath string,
	mainContent string,
	includes map[string]string,
) (
	provider generic.PbDescriptorProviderDynamicGo,
	err error,
) {
	defer func() {
		if recover() != nil {
			provider = nil
			err = fmt.Errorf(
				"%w: Proto descriptor provider failed",
				ErrInvalidConfig,
			)
		}
	}()
	return generic.NewPbContentProviderWithDynamicGo(
		ctx,
		dproto.Options{},
		mainPath,
		mainContent,
		includes,
	)
}

func newProtoJSONCodec(
	provider generic.PbDescriptorProviderDynamicGo,
) (
	codec generic.Generic,
	err error,
) {
	defer func() {
		if recover() != nil {
			codec = nil
			err = fmt.Errorf(
				"%w: Proto JSON codec failed",
				ErrInvalidConfig,
			)
		}
	}()
	return generic.JSONPbGeneric(provider)
}
