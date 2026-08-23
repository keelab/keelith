package generator

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	validatepb "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	errorReasonFieldNumber  protowire.Number = 51002
	dependencyFieldNumber   protowire.Number = 51004
	errorCodeFieldNumber    protowire.Number = 51005
	idempotencyFieldNumber  protowire.Number = 51006
	continuationFieldNumber protowire.Number = 51007
	projectionFieldNumber   protowire.Number = 51008
)

const (
	defaultIdempotencyMetadataKey          = "idempotency-key"
	defaultIdempotencyProcessingTTLSeconds = int64(30)
	defaultIdempotencyResultTTLSeconds     = int64(24 * 60 * 60)
	defaultContinuationInlineBudgetMillis  = int64(50)
	defaultContinuationRetentionSeconds    = int64(24 * 60 * 60)
	defaultContinuationMaxPayloadBytes     = int64(1024 * 1024)
	minimumContinuationInlineBudgetMillis  = int64(1)
	maximumContinuationInlineBudgetMillis  = int64(60 * 1000)
	minimumContinuationRetentionSeconds    = int64(60)
	maximumContinuationRetentionSeconds    = int64(30 * 24 * 60 * 60)
	minimumContinuationMaxPayloadBytes     = int64(1)
	maximumContinuationMaxPayloadBytes     = int64(1024 * 1024)
	maximumHTTPBindingsPerMethod           = 16
)

type httpRule struct {
	Method       string
	Path         string
	Body         string
	ResponseBody string
}

type declaredServiceDependency struct {
	Service   string
	Transport string
	Reason    string
	Binding   string
}

type idempotencyRule struct {
	Namespace           string
	MetadataKey         string
	ProcessingTTLSecond int64
	ResultTTLSecond     int64
}

type continuationRule struct {
	MachineVersion     string
	InlineBudgetMillis int64
	RetentionSeconds   int64
	MaxPayloadBytes    int64
}

type projectionRule struct {
	ID          string
	Message     string
	KeyFields   []string
	SchemaMajor uint32
	Migrations  []projectionMigration
}

type projectionMigration struct {
	PreviousFingerprint string
	PreviousMessage     string
	PreviousSchemaMajor uint32
	Fields              []projectionFieldMigration
	PreviousKeyFields   []string
}

type projectionFieldMigration struct {
	Previous   string
	Current    string
	Default    string
	HasDefault bool
	AllowDrop  bool
}

func methodIdempotencyRule(
	method *protogen.Method,
) (idempotencyRule, bool, error) {
	value, ok, err := unknownBytes(
		method.Desc.Options().ProtoReflect().GetUnknown(),
		idempotencyFieldNumber,
	)
	if err != nil || !ok {
		return idempotencyRule{}, ok, err
	}
	if method.Desc.IsStreamingClient() || method.Desc.IsStreamingServer() {
		return idempotencyRule{}, false, fmt.Errorf(
			"idempotency option on %s requires a unary method",
			method.Desc.FullName(),
		)
	}
	rule, err := decodeIdempotencyRule(value)
	if err != nil {
		return idempotencyRule{}, false, fmt.Errorf(
			"idempotency option on %s: %w",
			method.Desc.FullName(),
			err,
		)
	}
	return rule, true, nil
}

func decodeIdempotencyRule(payload []byte) (idempotencyRule, error) {
	result := idempotencyRule{
		MetadataKey:         defaultIdempotencyMetadataKey,
		ProcessingTTLSecond: defaultIdempotencyProcessingTTLSeconds,
		ResultTTLSecond:     defaultIdempotencyResultTTLSeconds,
	}
	seen := make(map[protowire.Number]struct{}, 4)
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return idempotencyRule{}, fmt.Errorf("invalid field tag")
		}
		payload = payload[tagSize:]
		if _, duplicate := seen[number]; duplicate {
			return idempotencyRule{}, fmt.Errorf("field %d is duplicated", number)
		}
		seen[number] = struct{}{}
		switch number {
		case 1, 2:
			if wireType != protowire.BytesType {
				return idempotencyRule{}, fmt.Errorf("field %d must be a string", number)
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return idempotencyRule{}, fmt.Errorf("field %d has invalid bytes", number)
			}
			payload = payload[size:]
			normalized := strings.TrimSpace(string(value))
			if number == 1 {
				result.Namespace = normalized
			} else if normalized != "" {
				result.MetadataKey = strings.ToLower(normalized)
			}
		case 3, 4:
			if wireType != protowire.VarintType {
				return idempotencyRule{}, fmt.Errorf("field %d must be an integer", number)
			}
			value, size := protowire.ConsumeVarint(payload)
			if size < 0 || value > 1<<63-1 {
				return idempotencyRule{}, fmt.Errorf("field %d has invalid value", number)
			}
			payload = payload[size:]
			if number == 3 && value != 0 {
				result.ProcessingTTLSecond = int64(value)
			}
			if number == 4 && value != 0 {
				result.ResultTTLSecond = int64(value)
			}
		default:
			return idempotencyRule{}, fmt.Errorf("field %d is unknown", number)
		}
	}
	if !validIdempotencyNamespace(result.Namespace) ||
		!validIdempotencyMetadataKey(result.MetadataKey) ||
		result.ProcessingTTLSecond < 1 ||
		result.ProcessingTTLSecond > 15*60 ||
		result.ResultTTLSecond < 60 ||
		result.ResultTTLSecond > 24*60*60 {
		return idempotencyRule{}, fmt.Errorf("namespace, metadata key, or ttl is invalid")
	}
	return result, nil
}

func validIdempotencyNamespace(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.' ||
			r == '/' {
			continue
		}
		return false
	}
	return true
}

func validIdempotencyMetadataKey(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func methodServiceDependencies(
	method *protogen.Method,
) ([]declaredServiceDependency, error) {
	values, err := unknownByteValues(
		method.Desc.Options().ProtoReflect().GetUnknown(),
		dependencyFieldNumber,
	)
	if err != nil {
		return nil, err
	}
	result := make([]declaredServiceDependency, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		dependency, err := decodeServiceDependency(value)
		if err != nil {
			return nil, fmt.Errorf(
				"dependency option %d on %s: %w",
				index,
				method.Desc.FullName(),
				err,
			)
		}
		key := dependency.Transport + "\x00" +
			dependency.Service + "\x00" +
			dependency.Reason + "\x00" +
			dependency.Binding
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf(
				"dependency option %d on %s is duplicated",
				index,
				method.Desc.FullName(),
			)
		}
		seen[key] = struct{}{}
		result = append(result, dependency)
	}
	return result, nil
}

func decodeServiceDependency(
	payload []byte,
) (declaredServiceDependency, error) {
	var result declaredServiceDependency
	seen := make(map[protowire.Number]struct{}, 4)
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return declaredServiceDependency{}, fmt.Errorf("invalid field tag")
		}
		payload = payload[tagSize:]
		if _, duplicate := seen[number]; duplicate {
			return declaredServiceDependency{}, fmt.Errorf(
				"field %d is duplicated",
				number,
			)
		}
		seen[number] = struct{}{}
		switch number {
		case 1, 2, 3:
			if wireType != protowire.BytesType {
				return declaredServiceDependency{}, fmt.Errorf(
					"field %d must be a string",
					number,
				)
			}
			value, valueSize := protowire.ConsumeBytes(payload)
			if valueSize < 0 {
				return declaredServiceDependency{}, fmt.Errorf(
					"field %d has invalid bytes",
					number,
				)
			}
			payload = payload[valueSize:]
			normalized := strings.TrimSpace(string(value))
			switch number {
			case 1:
				result.Service = normalized
			case 2:
				result.Transport = normalized
			case 3:
				result.Reason = normalized
			}
		case 4:
			if wireType != protowire.VarintType {
				return declaredServiceDependency{}, fmt.Errorf(
					"field 4 must be an enum",
				)
			}
			value, valueSize := protowire.ConsumeVarint(payload)
			if valueSize < 0 {
				return declaredServiceDependency{}, fmt.Errorf(
					"field 4 has invalid value",
				)
			}
			payload = payload[valueSize:]
			switch value {
			case 1:
				result.Binding = "REMOTE"
			case 2:
				result.Binding = "AUTO"
			case 3:
				result.Binding = "LOCAL"
			default:
				return declaredServiceDependency{}, fmt.Errorf(
					"binding enum value %d is unknown",
					value,
				)
			}
		default:
			return declaredServiceDependency{}, fmt.Errorf(
				"field %d is unknown",
				number,
			)
		}
	}
	if !validDependencyOptionValue(result.Service) ||
		(result.Transport != "grpc" && result.Transport != "http") ||
		!validDependencyOptionValue(result.Reason) ||
		result.Binding == "" {
		return declaredServiceDependency{}, fmt.Errorf(
			"service, grpc or http transport, reason, and explicit binding are required",
		)
	}
	return result, nil
}

func methodContinuationRule(
	method *protogen.Method,
) (continuationRule, bool, error) {
	values, err := unknownByteValues(
		method.Desc.Options().ProtoReflect().GetUnknown(),
		continuationFieldNumber,
	)
	if err != nil {
		return continuationRule{}, false, err
	}
	if len(values) == 0 {
		return continuationRule{}, false, nil
	}
	if len(values) != 1 {
		return continuationRule{}, false, fmt.Errorf(
			"continuation option on %s is duplicated",
			method.Desc.FullName(),
		)
	}
	if method.Desc.IsStreamingClient() || method.Desc.IsStreamingServer() {
		return continuationRule{}, false, fmt.Errorf(
			"continuation option on %s requires a unary method",
			method.Desc.FullName(),
		)
	}
	rule, err := decodeContinuationRule(values[0])
	if err != nil {
		return continuationRule{}, false, fmt.Errorf(
			"continuation option on %s: %w",
			method.Desc.FullName(),
			err,
		)
	}
	return rule, true, nil
}

func decodeContinuationRule(payload []byte) (continuationRule, error) {
	result := continuationRule{
		InlineBudgetMillis: defaultContinuationInlineBudgetMillis,
		RetentionSeconds:   defaultContinuationRetentionSeconds,
		MaxPayloadBytes:    defaultContinuationMaxPayloadBytes,
	}
	seen := make(map[protowire.Number]struct{}, 4)
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return continuationRule{}, fmt.Errorf("invalid field tag")
		}
		payload = payload[tagSize:]
		if _, duplicate := seen[number]; duplicate {
			return continuationRule{}, fmt.Errorf(
				"field %d is duplicated",
				number,
			)
		}
		seen[number] = struct{}{}
		switch number {
		case 1:
			if wireType != protowire.BytesType {
				return continuationRule{}, fmt.Errorf(
					"machine version must be a string",
				)
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return continuationRule{}, fmt.Errorf(
					"machine version has invalid bytes",
				)
			}
			payload = payload[size:]
			result.MachineVersion = strings.TrimSpace(string(value))
		case 2, 3, 4:
			if wireType != protowire.VarintType {
				return continuationRule{}, fmt.Errorf(
					"field %d must be an integer",
					number,
				)
			}
			value, size := protowire.ConsumeVarint(payload)
			if size < 0 || value > 1<<63-1 {
				return continuationRule{}, fmt.Errorf(
					"field %d has invalid value",
					number,
				)
			}
			payload = payload[size:]
			if value == 0 {
				continue
			}
			switch number {
			case 2:
				result.InlineBudgetMillis = int64(value)
			case 3:
				result.RetentionSeconds = int64(value)
			case 4:
				result.MaxPayloadBytes = int64(value)
			}
		default:
			return continuationRule{}, fmt.Errorf(
				"field %d is unknown",
				number,
			)
		}
	}
	if !validDependencyOptionValue(result.MachineVersion) {
		return continuationRule{}, fmt.Errorf(
			"machine version is required",
		)
	}
	if result.InlineBudgetMillis < minimumContinuationInlineBudgetMillis ||
		result.InlineBudgetMillis > maximumContinuationInlineBudgetMillis ||
		result.RetentionSeconds < minimumContinuationRetentionSeconds ||
		result.RetentionSeconds > maximumContinuationRetentionSeconds ||
		result.MaxPayloadBytes < minimumContinuationMaxPayloadBytes ||
		result.MaxPayloadBytes > maximumContinuationMaxPayloadBytes {
		return continuationRule{}, fmt.Errorf(
			"continuation budget is outside the supported range",
		)
	}
	return result, nil
}

func fileProjectionRules(file *protogen.File) ([]projectionRule, error) {
	result := make([]projectionRule, 0)
	identities := make(map[string]struct{})
	for _, message := range file.Messages {
		if err := rejectNestedProjectionRules(message); err != nil {
			return nil, err
		}
		rule, ok, err := messageProjectionRule(message)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if _, duplicate := identities[rule.ID]; duplicate {
			return nil, fmt.Errorf(
				"projection id %q is duplicated",
				rule.ID,
			)
		}
		identities[rule.ID] = struct{}{}
		result = append(result, rule)
	}
	return result, nil
}

func rejectNestedProjectionRules(message *protogen.Message) error {
	for _, nested := range message.Messages {
		values, err := unknownByteValues(
			nested.Desc.Options().ProtoReflect().GetUnknown(),
			projectionFieldNumber,
		)
		if err != nil {
			return err
		}
		if len(values) > 0 {
			return fmt.Errorf(
				"projection option on %s requires a top-level message",
				nested.Desc.FullName(),
			)
		}
		if err := rejectNestedProjectionRules(nested); err != nil {
			return err
		}
	}
	return nil
}

func messageProjectionRule(
	message *protogen.Message,
) (projectionRule, bool, error) {
	values, err := unknownByteValues(
		message.Desc.Options().ProtoReflect().GetUnknown(),
		projectionFieldNumber,
	)
	if err != nil {
		return projectionRule{}, false, err
	}
	if len(values) == 0 {
		return projectionRule{}, false, nil
	}
	if len(values) != 1 {
		return projectionRule{}, false, fmt.Errorf(
			"projection option on %s is duplicated",
			message.Desc.FullName(),
		)
	}
	rule, err := decodeProjectionRule(message, values[0])
	if err != nil {
		return projectionRule{}, false, fmt.Errorf(
			"projection option on %s: %w",
			message.Desc.FullName(),
			err,
		)
	}
	return rule, true, nil
}

func decodeProjectionRule(
	message *protogen.Message,
	payload []byte,
) (projectionRule, error) {
	result := projectionRule{Message: string(message.Desc.FullName())}
	seen := make(map[protowire.Number]struct{}, 4)
	keyFields := make(map[string]struct{})
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return projectionRule{}, fmt.Errorf("invalid field tag")
		}
		payload = payload[tagSize:]
		if number != 2 && number != 4 {
			if _, duplicate := seen[number]; duplicate {
				return projectionRule{}, fmt.Errorf(
					"field %d is duplicated",
					number,
				)
			}
			seen[number] = struct{}{}
		}
		switch number {
		case 1, 2:
			if wireType != protowire.BytesType {
				return projectionRule{}, fmt.Errorf(
					"field %d must be a string",
					number,
				)
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return projectionRule{}, fmt.Errorf(
					"field %d has invalid bytes",
					number,
				)
			}
			payload = payload[size:]
			normalized := strings.TrimSpace(string(value))
			if number == 1 {
				result.ID = normalized
				continue
			}
			if !protoreflect.Name(normalized).IsValid() {
				return projectionRule{}, fmt.Errorf(
					"key field %q is malformed",
					normalized,
				)
			}
			if _, duplicate := keyFields[normalized]; duplicate {
				return projectionRule{}, fmt.Errorf(
					"key field %q is duplicated",
					normalized,
				)
			}
			keyFields[normalized] = struct{}{}
			result.KeyFields = append(result.KeyFields, normalized)
		case 3:
			if wireType != protowire.VarintType {
				return projectionRule{}, fmt.Errorf(
					"schema major must be an integer",
				)
			}
			value, size := protowire.ConsumeVarint(payload)
			if size < 0 || value > uint64(^uint32(0)) {
				return projectionRule{}, fmt.Errorf(
					"schema major has invalid value",
				)
			}
			payload = payload[size:]
			result.SchemaMajor = uint32(value)
		case 4:
			if wireType != protowire.BytesType {
				return projectionRule{}, fmt.Errorf(
					"migration must be a message",
				)
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return projectionRule{}, fmt.Errorf(
					"migration has invalid bytes",
				)
			}
			payload = payload[size:]
			migration, err := decodeProjectionMigration(value)
			if err != nil {
				return projectionRule{}, fmt.Errorf(
					"migration %d: %w",
					len(result.Migrations),
					err,
				)
			}
			result.Migrations = append(result.Migrations, migration)
		default:
			return projectionRule{}, fmt.Errorf(
				"field %d is unknown",
				number,
			)
		}
	}
	if !validDependencyOptionValue(result.ID) {
		return projectionRule{}, fmt.Errorf("id is required")
	}
	if result.SchemaMajor == 0 {
		return projectionRule{}, fmt.Errorf("schema major must be positive")
	}
	if len(result.KeyFields) == 0 || len(result.KeyFields) > 8 {
		return projectionRule{}, fmt.Errorf(
			"projection requires 1 to 8 key fields",
		)
	}
	if len(result.Migrations) > 16 {
		return projectionRule{}, fmt.Errorf("projection has too many migrations")
	}
	seenFingerprints := make(map[string]struct{}, len(result.Migrations))
	for _, migration := range result.Migrations {
		if _, duplicate := seenFingerprints[migration.PreviousFingerprint]; duplicate {
			return projectionRule{}, fmt.Errorf(
				"migration previous fingerprint is duplicated",
			)
		}
		seenFingerprints[migration.PreviousFingerprint] = struct{}{}
	}
	fields := message.Desc.Fields()
	for _, name := range result.KeyFields {
		field := fields.ByName(protoreflect.Name(name))
		if field == nil {
			return projectionRule{}, fmt.Errorf(
				"key field %q does not exist",
				name,
			)
		}
		if field.IsList() || field.IsMap() || field.Message() != nil {
			return projectionRule{}, fmt.Errorf(
				"key field %q must be a non-repeated scalar",
				name,
			)
		}
	}
	return result, nil
}

func decodeProjectionMigration(payload []byte) (projectionMigration, error) {
	var result projectionMigration
	seen := make(map[protowire.Number]struct{}, 4)
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return projectionMigration{}, fmt.Errorf("invalid field tag")
		}
		payload = payload[tagSize:]
		if number != 4 && number != 5 {
			if _, duplicate := seen[number]; duplicate {
				return projectionMigration{}, fmt.Errorf("field %d is duplicated", number)
			}
			seen[number] = struct{}{}
		}
		switch number {
		case 1, 2:
			if wireType != protowire.BytesType {
				return projectionMigration{}, fmt.Errorf("field %d must be a string", number)
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return projectionMigration{}, fmt.Errorf("field %d has invalid bytes", number)
			}
			payload = payload[size:]
			if number == 1 {
				result.PreviousFingerprint = strings.TrimSpace(string(value))
			} else {
				result.PreviousMessage = strings.TrimSpace(string(value))
			}
		case 3:
			if wireType != protowire.VarintType {
				return projectionMigration{}, fmt.Errorf("previous schema major must be an integer")
			}
			value, size := protowire.ConsumeVarint(payload)
			if size < 0 || value > uint64(^uint32(0)) {
				return projectionMigration{}, fmt.Errorf("previous schema major has invalid value")
			}
			payload = payload[size:]
			result.PreviousSchemaMajor = uint32(value)
		case 4:
			if wireType != protowire.BytesType {
				return projectionMigration{}, fmt.Errorf("field migration must be a message")
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return projectionMigration{}, fmt.Errorf("field migration has invalid bytes")
			}
			payload = payload[size:]
			field, err := decodeProjectionFieldMigration(value)
			if err != nil {
				return projectionMigration{}, fmt.Errorf("field migration %d: %w", len(result.Fields), err)
			}
			result.Fields = append(result.Fields, field)
		case 5:
			if wireType != protowire.BytesType {
				return projectionMigration{}, fmt.Errorf("previous key field must be a string")
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return projectionMigration{}, fmt.Errorf("previous key field has invalid bytes")
			}
			payload = payload[size:]
			name := strings.TrimSpace(string(value))
			if !protoreflect.Name(name).IsValid() {
				return projectionMigration{}, fmt.Errorf("previous key field %q is malformed", name)
			}
			result.PreviousKeyFields = append(result.PreviousKeyFields, name)
		default:
			return projectionMigration{}, fmt.Errorf("field %d is unknown", number)
		}
	}
	if !validProjectionFingerprint(result.PreviousFingerprint) ||
		!protoreflect.FullName(result.PreviousMessage).IsValid() ||
		result.PreviousSchemaMajor == 0 || len(result.Fields) > 256 ||
		len(result.PreviousKeyFields) == 0 || len(result.PreviousKeyFields) > 8 {
		return projectionMigration{}, fmt.Errorf("previous fingerprint, message, major, or fields are invalid")
	}
	previous := make(map[string]struct{}, len(result.Fields))
	current := make(map[string]struct{}, len(result.Fields))
	previousKeys := make(map[string]struct{}, len(result.PreviousKeyFields))
	for _, name := range result.PreviousKeyFields {
		if _, duplicate := previousKeys[name]; duplicate {
			return projectionMigration{}, fmt.Errorf("previous key field %q is duplicated", name)
		}
		previousKeys[name] = struct{}{}
	}
	for _, field := range result.Fields {
		if field.Previous != "" {
			if _, duplicate := previous[field.Previous]; duplicate {
				return projectionMigration{}, fmt.Errorf("previous field %q is duplicated", field.Previous)
			}
			previous[field.Previous] = struct{}{}
		}
		if field.Current != "" {
			if _, duplicate := current[field.Current]; duplicate {
				return projectionMigration{}, fmt.Errorf("current field %q is duplicated", field.Current)
			}
			current[field.Current] = struct{}{}
		}
	}
	return result, nil
}

func decodeProjectionFieldMigration(
	payload []byte,
) (projectionFieldMigration, error) {
	var result projectionFieldMigration
	seen := make(map[protowire.Number]struct{}, 4)
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return projectionFieldMigration{}, fmt.Errorf("invalid field tag")
		}
		payload = payload[tagSize:]
		if _, duplicate := seen[number]; duplicate {
			return projectionFieldMigration{}, fmt.Errorf("field %d is duplicated", number)
		}
		seen[number] = struct{}{}
		switch number {
		case 1, 2, 3:
			if wireType != protowire.BytesType {
				return projectionFieldMigration{}, fmt.Errorf("field %d must be a string", number)
			}
			value, size := protowire.ConsumeBytes(payload)
			if size < 0 {
				return projectionFieldMigration{}, fmt.Errorf("field %d has invalid bytes", number)
			}
			payload = payload[size:]
			normalized := strings.TrimSpace(string(value))
			switch number {
			case 1:
				result.Previous = normalized
			case 2:
				result.Current = normalized
			case 3:
				result.Default = string(value)
				result.HasDefault = true
			}
		case 4:
			if wireType != protowire.VarintType {
				return projectionFieldMigration{}, fmt.Errorf("drop policy must be an enum")
			}
			value, size := protowire.ConsumeVarint(payload)
			if size < 0 || value > 1 {
				return projectionFieldMigration{}, fmt.Errorf("drop policy is unknown")
			}
			payload = payload[size:]
			result.AllowDrop = value == 1
		default:
			return projectionFieldMigration{}, fmt.Errorf("field %d is unknown", number)
		}
	}
	if result.Previous != "" && !protoreflect.Name(result.Previous).IsValid() ||
		result.Current != "" && !protoreflect.Name(result.Current).IsValid() {
		return projectionFieldMigration{}, fmt.Errorf("field name is malformed")
	}
	mapping := result.Previous != "" && result.Current != "" &&
		!result.HasDefault && !result.AllowDrop
	defaulting := result.Previous == "" && result.Current != "" &&
		result.HasDefault && !result.AllowDrop
	dropping := result.Previous != "" && result.Current == "" &&
		!result.HasDefault && result.AllowDrop
	if !mapping && !defaulting && !dropping {
		return projectionFieldMigration{}, fmt.Errorf("field migration must be exactly map, default, or allow-drop")
	}
	return result, nil
}

func validProjectionFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if r >= '0' && r <= '9' ||
			r >= 'a' && r <= 'f' {
			continue
		}
		return false
	}
	return true
}

func validDependencyOptionValue(value string) bool {
	if value == "" ||
		len(value) > 4*1024 ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func methodHTTPRule(method *protogen.Method) (httpRule, bool, error) {
	rules, ok, err := methodHTTPRules(method)
	if err != nil || !ok {
		return httpRule{}, ok, err
	}
	return rules[0], true, nil
}

func methodHTTPRules(method *protogen.Method) ([]httpRule, bool, error) {
	return standardHTTPRules(method)
}

func standardHTTPRules(
	method *protogen.Method,
) ([]httpRule, bool, error) {
	options, ok := method.Desc.Options().(*descriptorpb.MethodOptions)
	if !ok || options == nil || !proto.HasExtension(options, annotations.E_Http) {
		return nil, false, nil
	}
	extension := proto.GetExtension(options, annotations.E_Http)
	value, ok := extension.(*annotations.HttpRule)
	if !ok || value == nil {
		return nil, false, fmt.Errorf(
			"google.api.http on %s has unexpected type %T",
			method.Desc.FullName(),
			extension,
		)
	}
	if len(value.GetAdditionalBindings())+1 > maximumHTTPBindingsPerMethod {
		return nil, false, fmt.Errorf(
			"google.api.http on %s exceeds %d bindings",
			method.Desc.FullName(),
			maximumHTTPBindingsPerMethod,
		)
	}
	rules := make([]httpRule, 0, len(value.GetAdditionalBindings())+1)
	primary, err := decodeStandardHTTPRule(method, value, false)
	if err != nil {
		return nil, false, err
	}
	rules = append(rules, primary)
	seen := map[string]struct{}{httpRuleKey(primary): {}}
	for index, additional := range value.GetAdditionalBindings() {
		rule, err := decodeStandardHTTPRule(method, additional, true)
		if err != nil {
			return nil, false, fmt.Errorf(
				"google.api.http additional binding %d on %s: %w",
				index,
				method.Desc.FullName(),
				err,
			)
		}
		key := httpRuleKey(rule)
		if _, duplicate := seen[key]; duplicate {
			return nil, false, fmt.Errorf(
				"google.api.http binding %s %s on %s is duplicated",
				rule.Method,
				rule.Path,
				method.Desc.FullName(),
			)
		}
		seen[key] = struct{}{}
		rules = append(rules, rule)
	}
	return rules, true, nil
}

func decodeStandardHTTPRule(
	method *protogen.Method,
	value *annotations.HttpRule,
	additional bool,
) (httpRule, error) {
	if value == nil {
		return httpRule{}, fmt.Errorf("rule is empty")
	}
	if additional && len(value.GetAdditionalBindings()) > 0 {
		return httpRule{}, fmt.Errorf("nested additional_bindings are unsupported")
	}
	rule := httpRule{
		Body:         strings.TrimSpace(value.GetBody()),
		ResponseBody: strings.TrimSpace(value.GetResponseBody()),
	}
	switch pattern := value.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		rule.Method, rule.Path = "GET", pattern.Get
	case *annotations.HttpRule_Put:
		rule.Method, rule.Path = "PUT", pattern.Put
	case *annotations.HttpRule_Post:
		rule.Method, rule.Path = "POST", pattern.Post
	case *annotations.HttpRule_Delete:
		rule.Method, rule.Path = "DELETE", pattern.Delete
	case *annotations.HttpRule_Patch:
		rule.Method, rule.Path = "PATCH", pattern.Patch
	case *annotations.HttpRule_Custom:
		if pattern.Custom == nil {
			return httpRule{}, fmt.Errorf(
				"google.api.http custom rule on %s is empty",
				method.Desc.FullName(),
			)
		}
		rule.Method = strings.ToUpper(strings.TrimSpace(pattern.Custom.GetKind()))
		rule.Path = pattern.Custom.GetPath()
	default:
		return httpRule{}, fmt.Errorf(
			"google.api.http on %s has no HTTP pattern",
			method.Desc.FullName(),
		)
	}
	rule.Path = strings.TrimSpace(rule.Path)
	if err := validateHTTPRule(rule); err != nil {
		return httpRule{}, fmt.Errorf(
			"google.api.http on %s: %w",
			method.Desc.FullName(),
			err,
		)
	}
	return rule, nil
}

func httpRuleKey(rule httpRule) string {
	return rule.Method + "\x00" + rule.Path
}

func validateHTTPRule(rule httpRule) error {
	switch rule.Method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		return fmt.Errorf("http method %q is unsupported", rule.Method)
	}
	if rule.Path == "" || !strings.HasPrefix(rule.Path, "/") {
		return fmt.Errorf("http rule needs an absolute path")
	}
	if strings.ContainsAny(rule.Path, "?#\r\n") {
		return fmt.Errorf("http path %q contains an unsafe character", rule.Path)
	}
	if rule.Body != "" && rule.Body != "*" && !validHTTPBodyFieldPath(rule.Body) {
		return fmt.Errorf(
			"HTTP body mapping %q is not a valid Protobuf field path",
			rule.Body,
		)
	}
	if rule.ResponseBody != "" &&
		!validHTTPResponseBodyFieldPath(rule.ResponseBody) {
		return fmt.Errorf(
			"HTTP response body mapping %q is not a valid Protobuf field path",
			rule.ResponseBody,
		)
	}
	if (rule.Method == "GET" || rule.Method == "DELETE" ||
		rule.Method == "HEAD") && rule.Body != "" {
		return fmt.Errorf("%s http rule cannot have a body", rule.Method)
	}
	return nil
}

func validHTTPBodyFieldPath(path string) bool {
	return validHTTPFieldPath(path, true)
}

func validHTTPResponseBodyFieldPath(path string) bool {
	return validHTTPFieldPath(path, true)
}

func validHTTPFieldPath(path string, allowNested bool) bool {
	if path == "" || len(path) > 1024 || strings.TrimSpace(path) != path {
		return false
	}
	segments := strings.Split(path, ".")
	if !allowNested && len(segments) != 1 || len(segments) > 16 {
		return false
	}
	for _, segment := range segments {
		if segment == "" {
			return false
		}
		for index, r := range segment {
			valid := r == '_' ||
				r >= 'a' && r <= 'z' ||
				r >= 'A' && r <= 'Z' ||
				index > 0 && r >= '0' && r <= '9'
			if !valid {
				return false
			}
		}
	}
	return true
}

func httpBodyFieldPath(
	message *protogen.Message,
	path string,
) ([]protoreflect.FieldDescriptor, error) {
	if message == nil || !validHTTPBodyFieldPath(path) {
		return nil, fmt.Errorf("http body field path %q is invalid", path)
	}
	segments := strings.Split(path, ".")
	descriptor := message.Desc
	result := make([]protoreflect.FieldDescriptor, 0, len(segments))
	for index, segment := range segments {
		field := lookupHTTPField(descriptor.Fields(), segment)
		if field == nil {
			return nil, fmt.Errorf(
				"HTTP body field %q is absent from %s",
				strings.Join(segments[:index+1], "."),
				descriptor.FullName(),
			)
		}
		result = append(result, field)
		if index == len(segments)-1 {
			continue
		}
		if field.IsList() || field.IsMap() || field.Message() == nil {
			return nil, fmt.Errorf(
				"HTTP body field %q is not a singular message",
				strings.Join(segments[:index+1], "."),
			)
		}
		descriptor = field.Message()
	}
	return result, nil
}

func httpResponseBodyField(
	message *protogen.Message,
	path string,
) (protoreflect.FieldDescriptor, error) {
	if message == nil || !validHTTPResponseBodyFieldPath(path) {
		return nil, fmt.Errorf("http response body field %q is invalid", path)
	}
	segments := strings.Split(path, ".")
	descriptor := message.Desc
	var field protoreflect.FieldDescriptor
	for index, segment := range segments {
		field = lookupHTTPField(descriptor.Fields(), segment)
		if field == nil {
			return nil, fmt.Errorf(
				"HTTP response body field %q is absent from %s",
				strings.Join(segments[:index+1], "."),
				descriptor.FullName(),
			)
		}
		if index == len(segments)-1 {
			continue
		}
		if field.IsList() || field.IsMap() || field.Message() == nil {
			return nil, fmt.Errorf(
				"HTTP response body field %q is not a singular message",
				strings.Join(segments[:index+1], "."),
			)
		}
		descriptor = field.Message()
	}
	return field, nil
}

const googleAPIHTTPBodyFullName protoreflect.FullName = "google.api.HttpBody"

func httpRequestUsesHTTPBody(
	message *protogen.Message,
	body string,
) bool {
	if message == nil || body == "" {
		return false
	}
	if body == "*" {
		return message.Desc.FullName() == googleAPIHTTPBodyFullName
	}
	path, err := httpBodyFieldPath(message, body)
	if err != nil || len(path) == 0 {
		return false
	}
	return httpFieldUsesHTTPBody(path[len(path)-1])
}

func httpResponseUsesHTTPBody(
	message *protogen.Message,
	responseBody string,
) bool {
	if message == nil {
		return false
	}
	if responseBody == "" {
		return message.Desc.FullName() == googleAPIHTTPBodyFullName
	}
	field, err := httpResponseBodyField(message, responseBody)
	return err == nil && httpFieldUsesHTTPBody(field)
}

func httpFieldUsesHTTPBody(field protoreflect.FieldDescriptor) bool {
	return field != nil && !field.IsList() && !field.IsMap() &&
		field.Message() != nil &&
		field.Message().FullName() == googleAPIHTTPBodyFullName
}

func lookupHTTPField(
	fields protoreflect.FieldDescriptors,
	name string,
) protoreflect.FieldDescriptor {
	if field := fields.ByName(protoreflect.Name(name)); field != nil {
		return field
	}
	for index := range fields.Len() {
		field := fields.Get(index)
		if field.JSONName() == name {
			return field
		}
	}
	return nil
}

func methodErrorReason(method *protogen.Method) (string, bool, error) {
	value, ok, err := unknownBytes(
		method.Desc.Options().ProtoReflect().GetUnknown(),
		errorReasonFieldNumber,
	)
	if err != nil || !ok {
		return "", false, err
	}
	reason := strings.TrimSpace(string(value))
	if reason == "" {
		return "", false, fmt.Errorf("error reason is empty")
	}
	return reason, true, nil
}

func fieldRequired(field *protogen.Field) bool {
	options, ok := field.Desc.Options().(*descriptorpb.FieldOptions)
	if !ok || !proto.HasExtension(options, validatepb.E_Field) {
		return false
	}
	extension := proto.GetExtension(options, validatepb.E_Field)
	rules, ok := extension.(*validatepb.FieldRules)
	return ok && rules.GetRequired()
}

func unknownBytes(
	unknown []byte,
	want protowire.Number,
) ([]byte, bool, error) {
	for len(unknown) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(unknown)
		if tagSize < 0 {
			return nil, false, fmt.Errorf("invalid option tag")
		}
		unknown = unknown[tagSize:]
		if number == want {
			if wireType != protowire.BytesType {
				return nil, false, fmt.Errorf(
					"option %d has wire type %d, want bytes",
					want,
					wireType,
				)
			}
			value, valueSize := protowire.ConsumeBytes(unknown)
			if valueSize < 0 {
				return nil, false, fmt.Errorf("invalid option %d bytes", want)
			}
			return append([]byte(nil), value...), true, nil
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, unknown)
		if valueSize < 0 {
			return nil, false, fmt.Errorf("invalid option %d value", number)
		}
		unknown = unknown[valueSize:]
	}
	return nil, false, nil
}

func unknownByteValues(
	unknown []byte,
	want protowire.Number,
) ([][]byte, error) {
	result := make([][]byte, 0)
	for len(unknown) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(unknown)
		if tagSize < 0 {
			return nil, fmt.Errorf("invalid option tag")
		}
		unknown = unknown[tagSize:]
		if number == want {
			if wireType != protowire.BytesType {
				return nil, fmt.Errorf(
					"option %d has wire type %d, want bytes",
					want,
					wireType,
				)
			}
			value, valueSize := protowire.ConsumeBytes(unknown)
			if valueSize < 0 {
				return nil, fmt.Errorf("invalid option %d bytes", want)
			}
			result = append(result, append([]byte(nil), value...))
			unknown = unknown[valueSize:]
			continue
		}
		fieldSize := protowire.ConsumeFieldValue(number, wireType, unknown)
		if fieldSize < 0 {
			return nil, fmt.Errorf("invalid option %d value", number)
		}
		unknown = unknown[fieldSize:]
	}
	return result, nil
}

func unknownVarint(
	unknown []byte,
	want protowire.Number,
) (uint64, bool, error) {
	var result uint64
	found := false
	for len(unknown) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(unknown)
		if tagSize < 0 {
			return 0, false, fmt.Errorf("invalid option tag")
		}
		unknown = unknown[tagSize:]
		if number == want {
			if found {
				return 0, false, fmt.Errorf(
					"option %d is duplicated",
					want,
				)
			}
			if wireType != protowire.VarintType {
				return 0, false, fmt.Errorf(
					"option %d has wire type %d, want varint",
					want,
					wireType,
				)
			}
			value, valueSize := protowire.ConsumeVarint(unknown)
			if valueSize < 0 {
				return 0, false, fmt.Errorf("invalid option %d varint", want)
			}
			result = value
			found = true
			unknown = unknown[valueSize:]
			continue
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, unknown)
		if valueSize < 0 {
			return 0, false, fmt.Errorf("invalid option %d value", number)
		}
		unknown = unknown[valueSize:]
	}
	return result, found, nil
}
