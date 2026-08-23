package idempotency

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/operation"
	"google.golang.org/protobuf/proto"
)

// DefaultMetadataKey is the standard metadata key carrying idempotency keys.
const DefaultMetadataKey = "idempotency-key"

// ProtoRequest derives a key from inbound Metadata and a deterministic
// fingerprint from one protobuf request.
func ProtoRequest(metadataKey string) (RequestFunc, error) {
	metadataKey = strings.ToLower(strings.TrimSpace(metadataKey))
	if metadataKey == "" {
		metadataKey = DefaultMetadataKey
	}
	if !validMetadataKey(metadataKey) {
		return nil, fmt.Errorf("%w: metadata key", ErrInvalidConfig)
	}
	return func(ctx context.Context, _ operation.Operation, request any) (RequestIdentity, error) {
		if ctx == nil {
			return RequestIdentity{}, ErrInvalidRequest
		}
		inbound, ok := metadata.Inbound(ctx)
		if !ok {
			return RequestIdentity{}, ErrInvalidRequest
		}
		values := inbound.Values(metadataKey)
		if len(values) != 1 {
			return RequestIdentity{}, ErrInvalidRequest
		}
		message, ok := request.(proto.Message)
		if !ok || nilProto(message) {
			return RequestIdentity{}, ErrInvalidRequest
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
		if err != nil {
			return RequestIdentity{}, ErrInvalidRequest
		}
		identity := RequestIdentity{
			Key:         values[0],
			Fingerprint: Fingerprint(encoded),
		}
		if err := validateIdentity(identity); err != nil {
			return RequestIdentity{}, err
		}
		return identity, nil
	}, nil
}

// NewProtoCodec constructs an Operation-specific deterministic result codec.
func NewProtoCodec(factory func() proto.Message) (Codec, error) {
	if factory == nil {
		return nil, fmt.Errorf("%w: proto factory is nil", ErrInvalidConfig)
	}
	probe := factory()
	if nilProto(probe) {
		return nil, fmt.Errorf("%w: proto factory returned nil", ErrInvalidConfig)
	}
	return CodecFuncs{
		EncodeFunc: func(value any) ([]byte, error) {
			message, ok := value.(proto.Message)
			if !ok || nilProto(message) {
				return nil, fmt.Errorf("%w: result is not protobuf", ErrResultInvalid)
			}
			return (proto.MarshalOptions{Deterministic: true}).Marshal(message)
		},
		DecodeFunc: func(encoded []byte) (any, error) {
			message := factory()
			if nilProto(message) {
				return nil, fmt.Errorf("%w: proto factory returned nil", ErrResultInvalid)
			}
			if err := proto.Unmarshal(encoded, message); err != nil {
				return nil, fmt.Errorf("%w: protobuf decode", ErrResultInvalid)
			}
			return message, nil
		},
	}, nil
}

func nilProto(message proto.Message) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func validMetadataKey(value string) bool {
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return value != ""
}
