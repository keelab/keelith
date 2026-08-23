package generator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	componentPackage = protogen.GoImportPath(
		"github.com/keelab/keelith/programmable/component",
	)
	continuationPackage = protogen.GoImportPath(
		"github.com/keelab/keelith/programmable/continuation",
	)
	projectionPackage = protogen.GoImportPath(
		"github.com/keelab/keelith/programmable/projection",
	)
	topologyPackage = protogen.GoImportPath(
		"github.com/keelab/keelith/programmable/topology",
	)
	mathPackage      = protogen.GoImportPath("math")
	protowirePackage = protogen.GoImportPath(
		"google.golang.org/protobuf/encoding/protowire",
	)
)

func generateProgrammable(
	output *protogen.GeneratedFile,
	plugin *protogen.Plugin,
	file *protogen.File,
) error {
	if err := generateContinuationHelpers(output, file); err != nil {
		return err
	}
	if err := generateComponentRefHelpers(output, plugin, file); err != nil {
		return err
	}
	return generateProjectionHelpers(output, file)
}

func generateContinuationHelpers(
	output *protogen.GeneratedFile,
	file *protogen.File,
) error {
	for _, service := range file.Services {
		for _, method := range service.Methods {
			_, enabled, err := methodContinuationRule(method)
			if err != nil {
				return err
			}
			if !enabled {
				continue
			}
			generateContinuationHelper(output, service, method)
		}
	}
	return nil
}

func generateContinuationHelper(
	output *protogen.GeneratedFile,
	service *protogen.Service,
	method *protogen.Method,
) {
	prefix := service.GoName + method.GoName + "Continuation"
	operationName := prefix + "Operation"
	requireName := "require" + prefix
	operation := "/" + string(service.Desc.FullName()) + "/" +
		string(method.Desc.Name())
	requestType := method.Input.GoIdent
	responseType := method.Output.GoIdent
	snapshotType := output.QualifiedGoIdent(
		continuationPackage.Ident("Snapshot"),
	)

	output.P(
		"// ", operationName,
		" is the fixed durable operation for this generated method contract.",
	)
	output.P(
		"const ", operationName, " = ", fmt.Sprintf("%q", operation),
	)
	output.P()
	output.P(
		"func ", requireName, "(snapshot ", snapshotType, ") error {",
	)
	output.P(
		"if snapshot.Operation().String() != ", operationName, " {",
	)
	output.P(
		"return ", output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": operation %q does not match generated operation %q",
		), ", snapshot.Operation().String(), ", operationName, ")",
	)
	output.P("}")
	output.P("return nil")
	output.P("}")
	output.P()

	startName := "Start" + prefix
	output.P(
		"// ", startName,
		" marshals the fixed request type and persists a new durable call.",
	)
	output.P(
		"func ", startName, "(",
		"ctx ", output.QualifiedGoIdent(contextPackage.Ident("Context")), ", ",
		"runtime *", output.QualifiedGoIdent(
			continuationPackage.Ident("Runtime"),
		), ", ",
		"callID ", output.QualifiedGoIdent(
			continuationPackage.Ident("CallID"),
		), ", ",
		"input *", requestType,
		") (", snapshotType, ", error) {",
	)
	output.P("if input == nil {")
	output.P(
		"return ", snapshotType, "{}, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": input is nil",
		), ")",
	)
	output.P("}")
	output.P(
		"payload, err := ",
		output.QualifiedGoIdent(protoPackage.Ident("Marshal")),
		"(input)",
	)
	output.P(
		"if err != nil { return ", snapshotType, "{}, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": marshal input: %w",
		), ", err) }",
	)
	output.P(
		"operation, err := ",
		output.QualifiedGoIdent(
			continuationPackage.Ident("NewOperation"),
		),
		"(", operationName, ")",
	)
	output.P(
		"if err != nil { return ", snapshotType, "{}, err }",
	)
	output.P(
		"return runtime.StartCall(ctx, callID, operation, payload)",
	)
	output.P("}")
	output.P()

	callName := "Call" + prefix
	output.P(
		"// ", callName,
		" invokes the fixed durable operation and decodes its terminal result.",
	)
	output.P(
		"func ", callName, "(",
		"ctx ", output.QualifiedGoIdent(contextPackage.Ident("Context")), ", ",
		"client *", output.QualifiedGoIdent(
			httpPackage.Ident("ContinuationClient"),
		), ", ",
		"callID ", output.QualifiedGoIdent(
			continuationPackage.Ident("CallID"),
		), ", ",
		"input *", requestType,
		") (*", responseType, ", error) {",
	)
	output.P("if client == nil || input == nil {")
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": client or input is nil",
		), ")",
	)
	output.P("}")
	output.P(
		"payload, err := ",
		output.QualifiedGoIdent(protoPackage.Ident("Marshal")),
		"(input)",
	)
	output.P(
		"if err != nil { return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": marshal input: %w",
		), ", err) }",
	)
	output.P(
		"operation, err := ",
		output.QualifiedGoIdent(
			continuationPackage.Ident("NewOperation"),
		),
		"(", operationName, ")",
	)
	output.P("if err != nil { return nil, err }")
	output.P(
		"terminal, err := client.Call(ctx, callID, operation, payload)",
	)
	output.P("if err != nil { return nil, err }")
	output.P("result := new(", responseType, ")")
	output.P(
		"if err := ",
		output.QualifiedGoIdent(protoPackage.Ident("Unmarshal")),
		"(terminal, result); err != nil {",
	)
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": unmarshal terminal result: %w",
		), ", err)",
	)
	output.P("}")
	output.P("return result, nil")
	output.P("}")
	output.P()

	decodeName := "Decode" + prefix + "Input"
	output.P(
		"// ", decodeName,
		" verifies the fixed operation and decodes its durable input.",
	)
	output.P(
		"func ", decodeName, "(snapshot ", snapshotType,
		") (*", requestType, ", error) {",
	)
	output.P(
		"if err := ", requireName,
		"(snapshot); err != nil { return nil, err }",
	)
	output.P("frames := snapshot.Frames()")
	output.P(
		"if len(frames) == 0 || frames[0].Sequence() != 1 || ",
		"frames[0].Kind() != ",
		output.QualifiedGoIdent(continuationPackage.Ident("FrameAccepted")),
		" {",
	)
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": accepted input frame is absent",
		), ")",
	)
	output.P("}")
	output.P("input := new(", requestType, ")")
	output.P(
		"if err := ",
		output.QualifiedGoIdent(protoPackage.Ident("Unmarshal")),
		"(snapshot.Input(), input); err != nil {",
	)
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": unmarshal input: %w",
		), ", err)",
	)
	output.P("}")
	output.P("return input, nil")
	output.P("}")
	output.P()

	attachName := "Attach" + prefix
	attachmentType := output.QualifiedGoIdent(
		continuationPackage.Ident("Attachment"),
	)
	output.P(
		"// ", attachName,
		" reads one bounded page and rejects calls for another operation.",
	)
	output.P(
		"func ", attachName, "(",
		"ctx ", output.QualifiedGoIdent(contextPackage.Ident("Context")), ", ",
		"runtime *", output.QualifiedGoIdent(
			continuationPackage.Ident("Runtime"),
		), ", ",
		"callID ", output.QualifiedGoIdent(
			continuationPackage.Ident("CallID"),
		), ", after uint64, limit int",
		") (", attachmentType, ", error) {",
	)
	output.P("attachment, err := runtime.Attach(ctx, callID, after, limit)")
	output.P(
		"if err != nil { return ", attachmentType, "{}, err }",
	)
	output.P(
		"if err := ", requireName,
		"(attachment.Snapshot); err != nil { return ",
		attachmentType, "{}, err }",
	)
	output.P("return attachment, nil")
	output.P("}")
	output.P()

	signalName := "Signal" + prefix
	output.P(
		"// ", signalName,
		" marshals the fixed request type as one durable signal.",
	)
	output.P(
		"func ", signalName, "(",
		"ctx ", output.QualifiedGoIdent(contextPackage.Ident("Context")), ", ",
		"runtime *", output.QualifiedGoIdent(
			continuationPackage.Ident("Runtime"),
		), ", ",
		"callID ", output.QualifiedGoIdent(
			continuationPackage.Ident("CallID"),
		), ", commandID string, signal *", requestType,
		") (", snapshotType, ", error) {",
	)
	output.P("if signal == nil {")
	output.P(
		"return ", snapshotType, "{}, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": signal is nil",
		), ")",
	)
	output.P("}")
	output.P(
		"if _, err := ", attachName,
		"(ctx, runtime, callID, 0, 1); err != nil { return ",
		snapshotType, "{}, err }",
	)
	output.P(
		"payload, err := ",
		output.QualifiedGoIdent(protoPackage.Ident("Marshal")),
		"(signal)",
	)
	output.P(
		"if err != nil { return ", snapshotType, "{}, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"continuation "+service.GoName+"."+method.GoName+
				": marshal signal: %w",
		), ", err) }",
	)
	output.P(
		"return runtime.SubmitSignal(ctx, callID, commandID, payload)",
	)
	output.P("}")
	output.P()

	cancelName := "Cancel" + prefix
	output.P(
		"// ", cancelName,
		" requests cancellation only after verifying the fixed operation.",
	)
	output.P(
		"func ", cancelName, "(",
		"ctx ", output.QualifiedGoIdent(contextPackage.Ident("Context")), ", ",
		"runtime *", output.QualifiedGoIdent(
			continuationPackage.Ident("Runtime"),
		), ", ",
		"callID ", output.QualifiedGoIdent(
			continuationPackage.Ident("CallID"),
		), ", commandID string",
		") (", snapshotType, ", error) {",
	)
	output.P(
		"if _, err := ", attachName,
		"(ctx, runtime, callID, 0, 1); err != nil { return ",
		snapshotType, "{}, err }",
	)
	output.P("return runtime.RequestCancel(ctx, callID, commandID)")
	output.P("}")
	output.P()
}

func generateComponentRefHelpers(
	output *protogen.GeneratedFile,
	plugin *protogen.Plugin,
	file *protogen.File,
) error {
	targets, err := manifestDependencyTargets(plugin)
	if err != nil {
		return err
	}
	generated := make(map[string]string)
	for _, service := range file.Services {
		for _, method := range service.Methods {
			dependencies, err := methodServiceDependencies(method)
			if err != nil {
				return err
			}
			for _, dependency := range dependencies {
				if dependency.Binding == "" {
					continue
				}
				target, exists := targets[dependency.Service]
				if !exists {
					return fmt.Errorf(
						"programmable binding %s.%s: target service %q "+
							"is not present in the CodeGeneratorRequest",
						service.Desc.FullName(),
						method.Desc.Name(),
						dependency.Service,
					)
				}
				functionName := "Bind" + service.GoName +
					target.descriptor.GoName + "Ref"
				if previous, duplicate := generated[functionName]; duplicate {
					if previous != dependency.Service {
						return fmt.Errorf(
							"programmable binding: generated helper %s "+
								"is ambiguous for %q and %q",
							functionName,
							previous,
							dependency.Service,
						)
					}
					continue
				}
				generated[functionName] = dependency.Service
				generateComponentRefHelper(
					output,
					service,
					target,
					functionName,
				)
			}
		}
	}
	return nil
}

func generateComponentRefHelper(
	output *protogen.GeneratedFile,
	source *protogen.Service,
	target manifestDependencyTarget,
	functionName string,
) {
	clientType := output.QualifiedGoIdent(protogen.GoIdent{
		GoName:       target.descriptor.GoName + "KeelithClient",
		GoImportPath: protogen.GoImportPath(target.goImportPath),
	})
	refType := output.QualifiedGoIdent(componentPackage.Ident("Ref"))
	output.P(
		"// ", functionName,
		" binds the generated source and target component identities.",
	)
	output.P(
		"func ", functionName, "(runtime *",
		output.QualifiedGoIdent(componentPackage.Ident("Runtime")),
		") (", refType, "[", clientType, "], error) {",
	)
	output.P(
		"return ",
		output.QualifiedGoIdent(componentPackage.Ident("Bind")),
		"[", clientType, "](",
		"runtime, ",
		output.QualifiedGoIdent(topologyPackage.Ident("ComponentID")),
		"(", fmt.Sprintf("%q", string(source.Desc.FullName())), "), ",
		output.QualifiedGoIdent(topologyPackage.Ident("ComponentID")),
		"(", fmt.Sprintf("%q", target.service), "))",
	)
	output.P("}")
	output.P()

	factoryType := output.QualifiedGoIdent(componentPackage.Ident("Factory"))
	componentID := output.QualifiedGoIdent(topologyPackage.Ident("ComponentID"))
	localFactoryName := "Register" + source.GoName +
		target.descriptor.GoName + "LocalFactory"
	output.P(
		"// ", localFactoryName,
		" registers a typed lazy local provider for the generated target.",
	)
	output.P(
		"func ", localFactoryName, "(runtime *",
		output.QualifiedGoIdent(componentPackage.Ident("Runtime")),
		", factory ", factoryType, "[", clientType, "]) error {",
	)
	output.P(
		"return ",
		output.QualifiedGoIdent(
			componentPackage.Ident("RegisterLocalFactory"),
		),
		"(runtime, ", componentID,
		"(", fmt.Sprintf("%q", target.service), "), factory)",
	)
	output.P("}")
	output.P()

	remoteFactoryName := "Register" + source.GoName +
		target.descriptor.GoName + "RemoteFactory"
	output.P(
		"// ", remoteFactoryName,
		" registers a typed lazy remote provider for the generated target.",
	)
	output.P(
		"func ", remoteFactoryName, "(runtime *",
		output.QualifiedGoIdent(componentPackage.Ident("Runtime")),
		", factory ", factoryType, "[", clientType, "]) error {",
	)
	output.P(
		"return ",
		output.QualifiedGoIdent(
			componentPackage.Ident("RegisterRemoteFactory"),
		),
		"(runtime, ", componentID,
		"(", fmt.Sprintf("%q", target.service), "), factory)",
	)
	output.P("}")
	output.P()
}

func generateProjectionHelpers(
	output *protogen.GeneratedFile,
	file *protogen.File,
) error {
	for _, message := range file.Messages {
		rule, enabled, err := messageProjectionRule(message)
		if err != nil {
			return err
		}
		if !enabled {
			continue
		}
		if err := generateProjectionHelper(output, file, message, rule); err != nil {
			return err
		}
	}
	return nil
}

func generateProjectionHelper(
	output *protogen.GeneratedFile,
	file *protogen.File,
	message *protogen.Message,
	rule projectionRule,
) error {
	fields := make([]*protogen.Field, 0, len(rule.KeyFields))
	for _, name := range rule.KeyFields {
		for _, field := range message.Fields {
			if string(field.Desc.Name()) == name {
				fields = append(fields, field)
				break
			}
		}
	}
	sort.Slice(fields, func(first, second int) bool {
		return fields[first].Desc.Number() < fields[second].Desc.Number()
	})
	schemaFingerprint, keyFingerprint, err :=
		projectionFingerprints(message, rule, fields)
	if err != nil {
		return fmt.Errorf(
			"projection %s fingerprints: %w",
			rule.ID,
			err,
		)
	}
	migrations, err := prepareProjectionMigrations(
		file,
		message,
		rule,
		schemaFingerprint,
	)
	if err != nil {
		return fmt.Errorf("projection %s migrations: %w", rule.ID, err)
	}

	prefix := message.GoIdent.GoName + "Projection"
	schemaMajorName := prefix + "SchemaMajor"
	schemaName := prefix + "Schema"
	keyName := prefix + "Key"
	replicaName := "New" + prefix + "Replica"
	compatibleReplicaName := replicaName + "WithCompatibleDecoders"
	hashResolverName := "New" + prefix + "HashShardResolver"
	shardedReplicaName := "New" + prefix + "ShardedReplica"
	compatibleShardedReplicaName := shardedReplicaName +
		"WithCompatibleDecoders"
	migrationRegistryName := prefix + "MigrationRegistry"
	messageType := message.GoIdent
	schemaType := output.QualifiedGoIdent(projectionPackage.Ident("Schema"))

	output.P(
		"// ", schemaMajorName,
		" is the generated immutable projection schema major.",
	)
	output.P(
		"const ", schemaMajorName, " uint32 = ", rule.SchemaMajor,
	)
	output.P()
	output.P(
		"// ", schemaName,
		" returns the fixed generated projection schema.",
	)
	output.P("func ", schemaName, "() ", schemaType, " {")
	output.P(
		"schema := ", schemaType, "{",
		"ID: ", output.QualifiedGoIdent(
			projectionPackage.Ident("ProjectionID"),
		), "(", fmt.Sprintf("%q", rule.ID), "), ",
		"Fingerprint: ", fmt.Sprintf("%q", schemaFingerprint), ", ",
		"KeyFingerprint: ", fmt.Sprintf("%q", keyFingerprint),
		"}",
	)
	if len(migrations) > 0 {
		output.P(
			"schema, _ = schema.WithCompatibleFingerprints(",
			migrationFingerprintArguments(migrations),
			")",
		)
	}
	output.P("return schema")
	output.P("}")
	output.P()

	output.P(
		"// ", keyName,
		" encodes key fields in canonical protobuf field-number order.",
	)
	output.P(
		"func ", keyName, "(value *", messageType, ") ([]byte, error) {",
	)
	output.P("if value == nil {")
	output.P(
		"return nil, ",
		output.QualifiedGoIdent(fmtPackage.Ident("Errorf")),
		"(", fmt.Sprintf(
			"%q",
			"projection "+rule.ID+": key message is nil",
		), ")",
	)
	output.P("}")
	output.P("key := make([]byte, 0, 64)")
	for _, field := range fields {
		generateProjectionKeyField(output, field)
	}
	output.P("return key, nil")
	output.P("}")
	output.P()

	decodeName := "decode" + prefix + "Value"
	output.P(
		"func ", decodeName, "(payload []byte) (*", messageType,
		", error) {",
	)
	output.P("value := new(", messageType, ")")
	output.P(
		"if err := ",
		output.QualifiedGoIdent(protoPackage.Ident("Unmarshal")),
		"(payload, value); err != nil { return nil, err }",
	)
	output.P("return value, nil")
	output.P("}")
	output.P()

	for _, migration := range migrations {
		generateProjectionMigrationDecoder(
			output,
			message,
			prefix,
			migration,
		)
	}
	migrationRegistryType := output.QualifiedGoIdent(
		projectionPackage.Ident("MigrationRegistry"),
	)
	output.P(
		"// ", migrationRegistryName,
		" returns the generated direct-to-current typed upcaster registry.",
	)
	output.P(
		"func ", migrationRegistryName, "() (*", migrationRegistryType,
		"[*", messageType, "], error) {",
	)
	output.P(
		"return ", output.QualifiedGoIdent(
			projectionPackage.Ident("NewMigrationRegistry"),
		), "[*", messageType, "](",
		fmt.Sprintf("%q", schemaFingerprint), ", ", schemaMajorName,
		migrationStepArguments(output, message, prefix, schemaFingerprint, migrations),
		")",
	)
	output.P("}")
	output.P()

	replicaType := output.QualifiedGoIdent(
		projectionPackage.Ident("Replica"),
	)
	output.P(
		"// ", replicaName,
		" constructs a typed replica with fixed schema, key, and protobuf decoder.",
	)
	output.P(
		"func ", replicaName, "(",
		"store ", output.QualifiedGoIdent(
			projectionPackage.Ident("Store"),
		), ", options ...",
		output.QualifiedGoIdent(projectionPackage.Ident("ReplicaOption")),
		") (*", replicaType, "[*", messageType, ", *", messageType,
		"], error) {",
	)
	output.P(
		"registry, err := ", migrationRegistryName, "()",
	)
	output.P("if err != nil { return nil, err }")
	output.P(
		"return ",
		output.QualifiedGoIdent(projectionPackage.Ident("NewReplica")),
		"WithCompatibleDecoders[*", messageType, ", *", messageType, "](",
		schemaName, "(), store, ", keyName, ", ", decodeName,
		", registry.Decoders(), options...)",
	)
	output.P("}")
	output.P()

	valueDecoderType := output.QualifiedGoIdent(
		projectionPackage.Ident("ValueDecoder"),
	)
	output.P(
		"// ", compatibleReplicaName,
		" registers one typed decoder for every explicitly compatible value schema.",
	)
	output.P(
		"func ", compatibleReplicaName, "(",
		"store ", output.QualifiedGoIdent(
			projectionPackage.Ident("Store"),
		), ", compatible map[string]", valueDecoderType,
		"[*", messageType, "], options ...",
		output.QualifiedGoIdent(projectionPackage.Ident("ReplicaOption")),
		") (*", replicaType, "[*", messageType, ", *", messageType,
		"], error) {",
	)
	output.P("fingerprints := make([]string, 0, len(compatible))")
	output.P("for fingerprint := range compatible {")
	output.P("fingerprints = append(fingerprints, fingerprint)")
	output.P("}")
	output.P(
		"schema, err := ", schemaName,
		"().WithCompatibleFingerprints(fingerprints...)",
	)
	output.P("if err != nil { return nil, err }")
	output.P(
		"return ",
		output.QualifiedGoIdent(
			projectionPackage.Ident("NewReplicaWithCompatibleDecoders"),
		),
		"[*", messageType, ", *", messageType, "](",
		"schema, store, ", keyName, ", ", decodeName,
		", compatible, options...)",
	)
	output.P("}")
	output.P()

	shardIDType := output.QualifiedGoIdent(
		projectionPackage.Ident("ShardID"),
	)
	shardResolverType := output.QualifiedGoIdent(
		projectionPackage.Ident("ShardResolver"),
	)
	output.P(
		"// ", hashResolverName,
		" constructs a stable hash resolver bound to the generated key schema.",
	)
	output.P(
		"func ", hashResolverName, "(shards ...", shardIDType,
		") (", shardResolverType, "[*", messageType, "], error) {",
	)
	output.P(
		"schema := ", schemaName, "()",
	)
	output.P(
		"return ",
		output.QualifiedGoIdent(
			projectionPackage.Ident("NewHashShardResolver"),
		),
		"[*", messageType, "](schema.KeyFingerprint, ", keyName,
		", shards...)",
	)
	output.P("}")
	output.P()

	storeType := output.QualifiedGoIdent(projectionPackage.Ident("Store"))
	shardedReplicaType := output.QualifiedGoIdent(
		projectionPackage.Ident("ShardedReplica"),
	)
	output.P(
		"// ", shardedReplicaName,
		" constructs a typed shard router with fixed schema and protobuf decoder.",
	)
	output.P(
		"func ", shardedReplicaName, "(",
		"stores map[", shardIDType, "]", storeType, ", ",
		"resolver ", shardResolverType, "[*", messageType, "], options ...",
		output.QualifiedGoIdent(projectionPackage.Ident("ReplicaOption")),
		") (*", shardedReplicaType, "[*", messageType, ", *", messageType,
		"], error) {",
	)
	output.P(
		"registry, err := ", migrationRegistryName, "()",
	)
	output.P("if err != nil { return nil, err }")
	output.P(
		"return ", output.QualifiedGoIdent(
			projectionPackage.Ident("NewShardedReplicaWithCompatibleDecoders"),
		),
		"[*", messageType, ", *", messageType, "](",
		schemaName, "(), stores, resolver, ", keyName, ", ", decodeName,
		", registry.Decoders(), options...)",
	)
	output.P("}")
	output.P()

	output.P(
		"// ", compatibleShardedReplicaName,
		" registers explicit historical decoders for every shard.",
	)
	output.P(
		"func ", compatibleShardedReplicaName, "(",
		"stores map[", shardIDType, "]", storeType, ", ",
		"resolver ", shardResolverType, "[*", messageType, "], ",
		"compatible map[string]", valueDecoderType, "[*", messageType,
		"], options ...",
		output.QualifiedGoIdent(projectionPackage.Ident("ReplicaOption")),
		") (*", shardedReplicaType, "[*", messageType, ", *", messageType,
		"], error) {",
	)
	output.P("fingerprints := make([]string, 0, len(compatible))")
	output.P("for fingerprint := range compatible { fingerprints = append(fingerprints, fingerprint) }")
	output.P("schema, err := ", schemaName, "().WithCompatibleFingerprints(fingerprints...)")
	output.P("if err != nil { return nil, err }")
	output.P(
		"return ", output.QualifiedGoIdent(
			projectionPackage.Ident("NewShardedReplicaWithCompatibleDecoders"),
		),
		"[*", messageType, ", *", messageType, "](",
		"schema, stores, resolver, ", keyName, ", ", decodeName,
		", compatible, options...)",
	)
	output.P("}")
	output.P()
	return nil
}

type preparedProjectionMigration struct {
	rule     projectionMigration
	previous *protogen.Message
	defaults []preparedProjectionDefault
	index    int
}

type preparedProjectionDefault struct {
	field   *protogen.Field
	literal string
}

func prepareProjectionMigrations(
	file *protogen.File,
	current *protogen.Message,
	rule projectionRule,
	currentFingerprint string,
) ([]preparedProjectionMigration, error) {
	result := make([]preparedProjectionMigration, 0, len(rule.Migrations))
	currentByName := make(map[string]*protogen.Field, len(current.Fields))
	currentByNumber := make(map[protoreflect.FieldNumber]*protogen.Field, len(current.Fields))
	for _, field := range current.Fields {
		currentByName[string(field.Desc.Name())] = field
		currentByNumber[field.Desc.Number()] = field
	}
	currentKeys := make(map[string]struct{}, len(rule.KeyFields))
	for _, name := range rule.KeyFields {
		currentKeys[name] = struct{}{}
	}
	for index, migration := range rule.Migrations {
		if migration.PreviousFingerprint == currentFingerprint {
			return nil, fmt.Errorf("migration %d forms a fingerprint cycle", index)
		}
		if migration.PreviousSchemaMajor != rule.SchemaMajor {
			return nil, fmt.Errorf("migration %d crosses schema major", index)
		}
		previous := findProjectionMessage(file.Messages, protoreflect.FullName(migration.PreviousMessage))
		if previous == nil || previous == current {
			return nil, fmt.Errorf("migration %d previous message is absent or current", index)
		}
		previousByName := make(map[string]*protogen.Field, len(previous.Fields))
		for _, field := range previous.Fields {
			previousByName[string(field.Desc.Name())] = field
		}
		directives := make(map[string]projectionFieldMigration, len(migration.Fields))
		defaults := make(map[string]projectionFieldMigration, len(migration.Fields))
		for _, field := range migration.Fields {
			if field.Previous != "" {
				if previousByName[field.Previous] == nil {
					return nil, fmt.Errorf("migration %d previous field %q is absent", index, field.Previous)
				}
				directives[field.Previous] = field
			}
			if field.HasDefault {
				if currentByName[field.Current] == nil {
					return nil, fmt.Errorf("migration %d current field %q is absent", index, field.Current)
				}
				defaults[field.Current] = field
			}
		}
		mappedCurrent := make(map[string]struct{}, len(current.Fields))
		previousToCurrent := make(map[string]*protogen.Field, len(previous.Fields))
		for _, oldField := range previous.Fields {
			oldName := string(oldField.Desc.Name())
			directive, explicit := directives[oldName]
			if explicit && directive.AllowDrop {
				continue
			}
			var newField *protogen.Field
			if explicit {
				newField = currentByName[directive.Current]
			} else {
				candidate := currentByNumber[oldField.Desc.Number()]
				if candidate != nil && candidate.Desc.Name() == oldField.Desc.Name() {
					newField = candidate
				}
			}
			if newField == nil {
				return nil, fmt.Errorf(
					"migration %d field %q requires rename or allow-drop policy",
					index,
					oldName,
				)
			}
			if oldField.Desc.Number() != newField.Desc.Number() ||
				!sameMigrationFieldShape(oldField, newField) {
				return nil, fmt.Errorf(
					"migration %d field %q changes number or narrows type",
					index,
					oldName,
				)
			}
			newName := string(newField.Desc.Name())
			if _, duplicate := mappedCurrent[newName]; duplicate {
				return nil, fmt.Errorf("migration %d maps current field %q twice", index, newName)
			}
			mappedCurrent[newName] = struct{}{}
			previousToCurrent[oldName] = newField
		}
		preparedDefaults := make([]preparedProjectionDefault, 0, len(defaults))
		for _, newField := range current.Fields {
			newName := string(newField.Desc.Name())
			if _, mapped := mappedCurrent[newName]; mapped {
				continue
			}
			configured, exists := defaults[newName]
			if !exists {
				return nil, fmt.Errorf(
					"migration %d added field %q requires an explicit default",
					index,
					newName,
				)
			}
			literal, err := projectionDefaultLiteral(newField, configured.Default)
			if err != nil {
				return nil, fmt.Errorf("migration %d field %q default: %w", index, newName, err)
			}
			preparedDefaults = append(preparedDefaults, preparedProjectionDefault{
				field:   newField,
				literal: literal,
			})
		}
		if len(migration.PreviousKeyFields) != len(rule.KeyFields) {
			return nil, fmt.Errorf("migration %d changes key field count", index)
		}
		mappedKeys := make(map[string]struct{}, len(rule.KeyFields))
		for _, oldName := range migration.PreviousKeyFields {
			oldField := previousByName[oldName]
			newField := previousToCurrent[oldName]
			if oldField == nil || newField == nil {
				return nil, fmt.Errorf("migration %d drops or misses key field %q", index, oldName)
			}
			newName := string(newField.Desc.Name())
			if _, key := currentKeys[newName]; !key {
				return nil, fmt.Errorf("migration %d changes key field %q", index, oldName)
			}
			mappedKeys[newName] = struct{}{}
		}
		if len(mappedKeys) != len(currentKeys) {
			return nil, fmt.Errorf("migration %d changes key schema", index)
		}
		result = append(result, preparedProjectionMigration{
			rule:     migration,
			previous: previous,
			defaults: preparedDefaults,
			index:    index,
		})
	}
	return result, nil
}

func findProjectionMessage(
	messages []*protogen.Message,
	name protoreflect.FullName,
) *protogen.Message {
	for _, message := range messages {
		if message.Desc.FullName() == name {
			return message
		}
		if nested := findProjectionMessage(message.Messages, name); nested != nil {
			return nested
		}
	}
	return nil
}

func sameMigrationFieldShape(first, second *protogen.Field) bool {
	if first.Desc.Kind() != second.Desc.Kind() ||
		first.Desc.Cardinality() != second.Desc.Cardinality() ||
		first.Desc.IsMap() != second.Desc.IsMap() ||
		first.Desc.HasOptionalKeyword() != second.Desc.HasOptionalKeyword() {
		return false
	}
	if first.Desc.Message() != nil || second.Desc.Message() != nil {
		return first.Desc.Message() != nil && second.Desc.Message() != nil &&
			first.Desc.Message().FullName() == second.Desc.Message().FullName()
	}
	if first.Desc.Enum() != nil || second.Desc.Enum() != nil {
		return first.Desc.Enum() != nil && second.Desc.Enum() != nil &&
			first.Desc.Enum().FullName() == second.Desc.Enum().FullName()
	}
	return true
}

func projectionDefaultLiteral(
	field *protogen.Field,
	value string,
) (string, error) {
	if field.Desc.IsList() || field.Desc.IsMap() || field.Desc.Message() != nil ||
		field.Desc.Enum() != nil || field.Oneof != nil {
		return "", fmt.Errorf("only singular non-oneof scalar defaults are supported")
	}
	switch field.Desc.Kind() {
	case protoreflect.StringKind:
		return strconv.Quote(value), nil
	case protoreflect.BytesKind:
		return "[]byte(" + strconv.Quote(value) + ")", nil
	case protoreflect.BoolKind:
		parsed, err := strconv.ParseBool(value)
		if err != nil || value != strconv.FormatBool(parsed) {
			return "", fmt.Errorf("invalid bool")
		}
		return value, nil
	case protoreflect.Int32Kind,
		protoreflect.Sint32Kind,
		protoreflect.Sfixed32Kind:
		if _, err := strconv.ParseInt(value, 10, 32); err != nil {
			return "", fmt.Errorf("invalid int32")
		}
		return "int32(" + value + ")", nil
	case protoreflect.Int64Kind,
		protoreflect.Sint64Kind,
		protoreflect.Sfixed64Kind:
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return "", fmt.Errorf("invalid int64")
		}
		return "int64(" + value + ")", nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if _, err := strconv.ParseUint(value, 10, 32); err != nil {
			return "", fmt.Errorf("invalid uint32")
		}
		return "uint32(" + value + ")", nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if _, err := strconv.ParseUint(value, 10, 64); err != nil {
			return "", fmt.Errorf("invalid uint64")
		}
		return "uint64(" + value + ")", nil
	default:
		return "", fmt.Errorf("unsupported scalar default")
	}
}

func migrationFingerprintArguments(
	migrations []preparedProjectionMigration,
) string {
	values := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		values = append(values, strconv.Quote(migration.rule.PreviousFingerprint))
	}
	return strings.Join(values, ", ")
}

func migrationDecoderName(
	prefix string,
	migration preparedProjectionMigration,
) string {
	return "decode" + prefix + "From" + migration.previous.GoIdent.GoName +
		strconv.Itoa(migration.index+1)
}

func generateProjectionMigrationDecoder(
	output *protogen.GeneratedFile,
	current *protogen.Message,
	prefix string,
	migration preparedProjectionMigration,
) {
	name := migrationDecoderName(prefix, migration)
	output.P("func ", name, "(payload []byte) (*", current.GoIdent, ", error) {")
	output.P("value := new(", current.GoIdent, ")")
	output.P(
		"if err := ", output.QualifiedGoIdent(protoPackage.Ident("Unmarshal")),
		"(payload, value); err != nil { return nil, err }",
	)
	for _, configured := range migration.defaults {
		output.P("value.", configured.field.GoName, " = ", configured.literal)
	}
	output.P("return value, nil")
	output.P("}")
	output.P()
}

func migrationStepArguments(
	output *protogen.GeneratedFile,
	current *protogen.Message,
	prefix string,
	currentFingerprint string,
	migrations []preparedProjectionMigration,
) string {
	if len(migrations) == 0 {
		return ""
	}
	stepType := output.QualifiedGoIdent(projectionPackage.Ident("MigrationStep"))
	currentType := output.QualifiedGoIdent(current.GoIdent)
	parts := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		parts = append(parts, fmt.Sprintf(
			", %s[*%s]{PreviousFingerprint: %q, CurrentFingerprint: %q, PreviousMajor: %d, CurrentMajor: %s, Upcast: %s}",
			stepType,
			currentType,
			migration.rule.PreviousFingerprint,
			currentFingerprint,
			migration.rule.PreviousSchemaMajor,
			prefix+"SchemaMajor",
			migrationDecoderName(prefix, migration),
		))
	}
	return strings.Join(parts, "")
}

func generateProjectionKeyField(
	output *protogen.GeneratedFile,
	field *protogen.Field,
) {
	getter := "value.Get" + field.GoName + "()"
	number := field.Desc.Number()
	wireType := ""
	switch field.Desc.Kind() {
	case protoreflect.StringKind, protoreflect.BytesKind:
		wireType = "BytesType"
	case protoreflect.Fixed32Kind,
		protoreflect.Sfixed32Kind,
		protoreflect.FloatKind:
		wireType = "Fixed32Type"
	case protoreflect.Fixed64Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.DoubleKind:
		wireType = "Fixed64Type"
	default:
		wireType = "VarintType"
	}
	output.P(
		"key = ",
		output.QualifiedGoIdent(protowirePackage.Ident("AppendTag")),
		"(key, ", number, ", ",
		output.QualifiedGoIdent(protowirePackage.Ident(wireType)),
		")",
	)
	switch field.Desc.Kind() {
	case protoreflect.StringKind:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendBytes")),
			"(key, []byte(", getter, "))",
		)
	case protoreflect.BytesKind:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendBytes")),
			"(key, ", getter, ")",
		)
	case protoreflect.BoolKind:
		valueName := "field" + field.GoIdent.GoName
		output.P(valueName, " := uint64(0)")
		output.P("if ", getter, " { ", valueName, " = 1 }")
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendVarint")),
			"(key, ", valueName, ")",
		)
	case protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendVarint")),
			"(key, ",
			output.QualifiedGoIdent(protowirePackage.Ident("EncodeZigZag")),
			"(int64(", getter, ")))",
		)
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendFixed32")),
			"(key, uint32(", getter, "))",
		)
	case protoreflect.FloatKind:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendFixed32")),
			"(key, ",
			output.QualifiedGoIdent(mathPackage.Ident("Float32bits")),
			"(", getter, "))",
		)
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendFixed64")),
			"(key, uint64(", getter, "))",
		)
	case protoreflect.DoubleKind:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendFixed64")),
			"(key, ",
			output.QualifiedGoIdent(mathPackage.Ident("Float64bits")),
			"(", getter, "))",
		)
	default:
		output.P(
			"key = ",
			output.QualifiedGoIdent(protowirePackage.Ident("AppendVarint")),
			"(key, uint64(", getter, "))",
		)
	}
}

func projectionFingerprints(
	message *protogen.Message,
	rule projectionRule,
	fields []*protogen.Field,
) (string, string, error) {
	descriptorProto := protodesc.ToDescriptorProto(message.Desc)
	if descriptorProto.Options != nil {
		unknown, err := stripProjectionMigrationOptions(
			descriptorProto.Options.ProtoReflect().GetUnknown(),
		)
		if err != nil {
			return "", "", err
		}
		descriptorProto.Options.ProtoReflect().SetUnknown(unknown)
	}
	descriptor, err := proto.MarshalOptions{Deterministic: true}.Marshal(descriptorProto)
	if err != nil {
		return "", "", err
	}
	var major [4]byte
	binary.BigEndian.PutUint32(major[:], rule.SchemaMajor)
	schema := programmableFingerprint(
		[]byte("keelith-projection-schema-v1"),
		[]byte(rule.ID),
		[]byte(message.Desc.FullName()),
		major[:],
		descriptor,
	)
	keyParts := make([][]byte, 0, 4+len(fields)*3)
	keyParts = append(
		keyParts,
		[]byte("keelith-projection-key-v1"),
		[]byte(rule.ID),
		[]byte(message.Desc.FullName()),
		major[:],
	)
	for _, field := range fields {
		var number [4]byte
		binary.BigEndian.PutUint32(number[:], uint32(field.Desc.Number()))
		keyParts = append(
			keyParts,
			number[:],
			[]byte(field.Desc.Name()),
			[]byte(field.Desc.Kind().String()),
		)
	}
	return schema, programmableFingerprint(keyParts...), nil
}

func stripProjectionMigrationOptions(payload []byte) ([]byte, error) {
	result := make([]byte, 0, len(payload))
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return nil, fmt.Errorf("projection fingerprint option tag is invalid")
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, payload[tagSize:])
		if valueSize < 0 {
			return nil, fmt.Errorf("projection fingerprint option value is invalid")
		}
		raw := payload[:tagSize+valueSize]
		if number != projectionFieldNumber {
			result = append(result, raw...)
			payload = payload[len(raw):]
			continue
		}
		if wireType != protowire.BytesType {
			return nil, fmt.Errorf("projection fingerprint option must be bytes")
		}
		rule, size := protowire.ConsumeBytes(payload[tagSize:])
		if size < 0 {
			return nil, fmt.Errorf("projection fingerprint rule is invalid")
		}
		stripped, err := stripWireField(rule, 4)
		if err != nil {
			return nil, err
		}
		result = protowire.AppendTag(result, number, protowire.BytesType)
		result = protowire.AppendBytes(result, stripped)
		payload = payload[len(raw):]
	}
	return result, nil
}

func stripWireField(
	payload []byte,
	removed protowire.Number,
) ([]byte, error) {
	result := make([]byte, 0, len(payload))
	for len(payload) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(payload)
		if tagSize < 0 {
			return nil, fmt.Errorf("projection fingerprint rule tag is invalid")
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, payload[tagSize:])
		if valueSize < 0 {
			return nil, fmt.Errorf("projection fingerprint rule value is invalid")
		}
		rawSize := tagSize + valueSize
		if number != removed {
			result = append(result, payload[:rawSize]...)
		}
		payload = payload[rawSize:]
	}
	return result, nil
}

func programmableFingerprint(parts ...[]byte) string {
	hasher := sha256.New()
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write(part)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
