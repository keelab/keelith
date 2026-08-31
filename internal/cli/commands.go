package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	core "github.com/keelab/keelith/config/versioned"
	"github.com/spf13/cobra"
)

type commandRuntime struct {
	stdout       io.Writer
	stderr       io.Writer
	exitCode     int
	configOpener configStoreOpener
}

func newRootCommand(runtime *commandRuntime) *cobra.Command {
	root := &cobra.Command{
		Use:           "keelith",
		Short:         "Keelith developer tooling",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	root.SetOut(runtime.stdout)
	root.SetErr(runtime.stderr)
	root.AddCommand(
		newAddCommand(runtime),
		newConfigCommand(runtime),
		newDoctorCommand(runtime),
		newGenerateCommand(runtime),
		newGraphCommand(runtime),
		newNewCommand(runtime),
		newVersionCommand(runtime),
		newWiringCommand(runtime),
	)
	return root
}

func commandGroup(use, short string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
}

func runCommand(
	command *cobra.Command,
	runtime *commandRuntime,
	action func(context.Context) int,
) error {
	runtime.exitCode = action(command.Context())
	return nil
}

func validateTextJSON(value string) error {
	if value != "text" && value != "json" {
		return errors.New("--format must be text or json")
	}
	return nil
}

func newVersionCommand(runtime *commandRuntime) *cobra.Command {
	format := "text"
	command := &cobra.Command{
		Use:   "version",
		Short: "Print build version",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if err := validateTextJSON(format); err != nil {
				return err
			}
			return runCommand(command, runtime, func(context.Context) int {
				return executeVersion(format, runtime.stdout, runtime.stderr)
			})
		},
	}
	command.Flags().StringVar(&format, "format", format, "output format: text or json")
	return command
}

func newDoctorCommand(runtime *commandRuntime) *cobra.Command {
	options := doctorOptions{format: "text"}
	command := &cobra.Command{
		Use:   "doctor",
		Short: "Inspect toolchain and optional project integrity",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			options.inspect = command.Flags().Changed("path")
			if options.inspect && strings.TrimSpace(options.path) == "" {
				return errors.New("--path must not be empty")
			}
			if err := validateTextJSON(options.format); err != nil {
				return err
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeDoctor(ctx, options, runtime.stdout, runtime.stderr)
			})
		},
	}
	command.Flags().StringVar(&options.path, "path", "", "project directory to inspect")
	command.Flags().StringVar(&options.format, "format", options.format, "output format: text or json")
	return command
}

func newGraphCommand(runtime *commandRuntime) *cobra.Command {
	options := graphOptions{path: ".", format: "text"}
	command := &cobra.Command{
		Use:   "graph",
		Short: "Inspect generated service and dependency contracts",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if strings.TrimSpace(options.path) == "" {
				return errors.New("--path must not be empty")
			}
			if command.Flags().Changed("plan") && strings.TrimSpace(options.plan) == "" {
				return errors.New("--plan must not be empty")
			}
			switch options.format {
			case "text", "json", "dot":
			default:
				return errors.New("--format must be text, json, or dot")
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeGraph(ctx, options, runtime.stdout, runtime.stderr)
			})
		},
	}
	flags := command.Flags()
	flags.StringVar(&options.path, "path", options.path, "project directory")
	flags.StringVar(&options.plan, "plan", "", "project-relative topology plan")
	flags.StringVar(&options.format, "format", options.format, "output format: text, json, or dot")
	return command
}

func newGenerateCommand(runtime *commandRuntime) *cobra.Command {
	options := generateOptions{path: ".", format: "text"}
	command := commandGroup("generate", "Generate adapters, facades, or offline data models")
	command.RunE = func(command *cobra.Command, _ []string) error {
		if strings.TrimSpace(options.path) == "" {
			return errors.New("--path must not be empty")
		}
		if err := validateTextJSON(options.format); err != nil {
			return err
		}
		return runCommand(command, runtime, func(ctx context.Context) int {
			return executeGenerate(ctx, options, runtime.stdout, runtime.stderr)
		})
	}
	command.Flags().StringVar(&options.path, "path", options.path, "project directory")
	command.Flags().StringVar(&options.format, "format", options.format, "output format: text or json")
	command.AddCommand(newGenerateDataCommand(runtime), newGenerateKitexFacadeCommand(runtime))
	return command
}

func newGenerateDataCommand(runtime *commandRuntime) *cobra.Command {
	options := dataOptions{
		path: ".", packageID: "model",
		output: "internal/data/model/models.keelith.gen.go", format: "text",
	}
	command := &cobra.Command{
		Use:   "data",
		Short: "Generate offline Go data models from SQL schema",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if strings.TrimSpace(options.path) == "" ||
				strings.TrimSpace(options.schema) == "" ||
				strings.TrimSpace(options.packageID) == "" ||
				strings.TrimSpace(options.output) == "" {
				return errors.New("--path, --schema, --package, and --output must not be empty")
			}
			if filepath.Ext(options.output) != ".go" {
				return errors.New("--output must be a Go source file")
			}
			if err := validateTextJSON(options.format); err != nil {
				return err
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeGenerateData(ctx, options, runtime.stdout, runtime.stderr)
			})
		},
	}
	flags := command.Flags()
	flags.StringVar(&options.path, "path", options.path, "project directory")
	flags.StringVar(&options.schema, "schema", "", "project-relative SQL schema file")
	flags.StringVar(&options.packageID, "package", options.packageID, "generated Go package")
	flags.StringVar(&options.output, "output", options.output, "project-relative generated Go file")
	flags.BoolVar(&options.check, "check", false, "report drift without writing files")
	flags.StringVar(&options.format, "format", options.format, "output format: text or json")
	return command
}

func newGenerateKitexFacadeCommand(runtime *commandRuntime) *cobra.Command {
	options := kitexFacadeOptions{path: ".", source: "kitex_gen", format: "text"}
	command := &cobra.Command{
		Use:   "kitex-facade",
		Short: "Generate bounded Kitex client facades",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if strings.TrimSpace(options.path) == "" || strings.TrimSpace(options.source) == "" {
				return errors.New("--path and --source must not be empty")
			}
			if err := validateTextJSON(options.format); err != nil {
				return err
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeGenerateKitexFacade(ctx, options, runtime.stdout, runtime.stderr)
			})
		},
	}
	flags := command.Flags()
	flags.StringVar(&options.path, "path", options.path, "project directory")
	flags.StringVar(&options.source, "source", options.source, "project-relative Kitex source directory")
	flags.BoolVar(&options.check, "check", false, "report drift without writing files")
	flags.StringVar(&options.format, "format", options.format, "output format: text or json")
	return command
}

func newConfigCommand(runtime *commandRuntime) *cobra.Command {
	command := commandGroup("config", "Manage versioned configuration")
	command.Example = strings.Join([]string{
		"  keelith config stage --endpoint https://etcd:2379 --prefix /apps/orders --file config.yaml --actor platform",
		"  keelith config active --endpoint https://etcd:2379 --prefix /apps/orders",
		"  keelith config history --endpoint https://etcd:2379 --prefix /apps/orders",
		"  keelith config activate --endpoint https://etcd:2379 --prefix /apps/orders --revision SHA256 --expected-generation 1 --actor platform --reason rollout",
		"  keelith config rollback --endpoint https://etcd:2379 --prefix /apps/orders --revision SHA256 --expected-generation 2 --actor platform --reason rollback",
	}, "\n")
	for _, name := range []string{"stage", "active", "history", "activate", "rollback"} {
		command.AddCommand(newConfigOperationCommand(runtime, name))
	}
	return command
}

func newConfigOperationCommand(runtime *commandRuntime, name string) *cobra.Command {
	options := configCommandOptions{
		command: name, format: "text", limit: core.DefaultHistoryLimit,
		connection: configConnectionOptions{dialTimeout: 5 * time.Second},
	}
	documentFormat := ""
	command := &cobra.Command{
		Use:   name,
		Short: configCommandSummary(name),
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			options.documentFormat = core.Format(documentFormat)
			options.expectedGenerationSet = command.Flags().Changed("expected-generation")
			if err := validateConfigCommandOptions(options); err != nil {
				return err
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeConfigWithOpener(
					ctx, options, runtime.stdout, runtime.stderr, runtime.configOpener,
				)
			})
		},
	}
	bindConfigConnectionFlags(command, &options.connection)
	flags := command.Flags()
	flags.StringVar(&options.format, "format", options.format, "output format: text or json")
	switch name {
	case "stage":
		flags.StringVar(&options.file, "file", "", "configuration document")
		flags.StringVar(&documentFormat, "document-format", "", "document format: json or yaml")
		flags.StringVar(&options.actor, "actor", "", "change actor")
		flags.StringVar(&options.message, "message", "", "optional change message")
	case "history":
		flags.IntVar(&options.limit, "limit", options.limit, "maximum history entries")
	case "activate", "rollback":
		flags.StringVar(&options.revision, "revision", "", "configuration revision SHA-256")
		flags.Uint64Var(&options.expectedGeneration, "expected-generation", 0, "expected active generation")
		flags.StringVar(&options.actor, "actor", "", "change actor")
		flags.StringVar(&options.reason, "reason", "", "activation reason")
	}
	return command
}

func configCommandSummary(name string) string {
	switch name {
	case "stage":
		return "Stage a versioned configuration revision"
	case "active":
		return "Inspect the active configuration revision"
	case "history":
		return "List configuration activation history"
	case "activate":
		return "Activate a staged configuration revision"
	default:
		return "Roll back to a previous configuration revision"
	}
}

func bindConfigConnectionFlags(command *cobra.Command, options *configConnectionOptions) {
	flags := command.Flags()
	flags.StringArrayVar(&options.endpoints, "endpoint", nil, "Etcd endpoint (repeatable)")
	flags.StringVar(&options.prefix, "prefix", "", "absolute Etcd path")
	flags.DurationVar(&options.dialTimeout, "dial-timeout", options.dialTimeout, "Etcd dial timeout")
	flags.StringVar(&options.username, "username", "", "Etcd username")
	flags.StringVar(&options.passwordEnv, "password-env", "", "environment variable containing the Etcd password")
	flags.StringVar(&options.caFile, "ca-file", "", "private CA PEM file")
	flags.StringVar(&options.certFile, "cert-file", "", "client certificate PEM file")
	flags.StringVar(&options.keyFile, "key-file", "", "client private key PEM file")
	flags.StringVar(&options.serverName, "server-name", "", "TLS server name")
	flags.BoolVar(&options.allowInsecure, "allow-insecure", false, "allow plaintext HTTP Etcd endpoints")
}

func validateConfigCommandOptions(options configCommandOptions) error {
	if err := validateConfigConnection(options.connection); err != nil {
		return err
	}
	if err := validateTextJSON(options.format); err != nil {
		return err
	}
	switch options.command {
	case "stage":
		if strings.TrimSpace(options.file) == "" || strings.TrimSpace(options.actor) == "" {
			return errors.New("stage requires --file and --actor")
		}
		if options.documentFormat != "" && !options.documentFormat.Valid() {
			return errors.New("--document-format must be json or yaml")
		}
	case "history":
		if _, err := core.NormalizeHistoryLimit(options.limit); err != nil {
			return err
		}
	case "activate", "rollback":
		if !options.expectedGenerationSet {
			return errors.New("--expected-generation is required")
		}
		request := core.ActivateRequest{
			Revision: options.revision, ExpectedGeneration: options.expectedGeneration,
			Actor: options.actor, Reason: options.reason,
		}
		if err := request.Validate(); err != nil {
			return errors.New("activation requires valid --revision, --actor, and --reason")
		}
	}
	return nil
}

func newAddCommand(runtime *commandRuntime) *cobra.Command {
	command := commandGroup("add", "Add a service, API, dependency, or application component")
	command.AddCommand(
		newAddServiceCommand(runtime),
		newAddAPICommand(runtime),
		newAddErrorCommand(runtime),
		newAddDependencyCommand(runtime),
		newAddComponentCommand(runtime),
	)
	return command
}

func newAddServiceCommand(runtime *commandRuntime) *cobra.Command {
	options := addOptions{kind: "service", path: ".", format: "text"}
	command := &cobra.Command{
		Use:   "service [NAME]",
		Short: "Add a runnable unary service and generated binding",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if len(args) == 1 {
				if options.name != "" && options.name != args[0] {
					return errors.New("service name is specified more than once")
				}
				options.name = args[0]
			}
			if err := validateAddOptions(options); err != nil {
				return err
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeAdd(ctx, options, runtime.stdout, runtime.stderr)
			})
		},
	}
	bindAddProjectFlags(command, &options)
	flags := command.Flags()
	flags.StringVar(&options.name, "name", "", "service short name")
	flags.StringVar(&options.packageID, "package", "", "protobuf package")
	flags.StringVar(&options.service, "service", "", "service name")
	flags.StringVar(&options.method, "method", "", "method name")
	flags.StringVar(&options.httpMethod, "http-method", "", "HTTP method")
	flags.StringVar(&options.httpPath, "http-path", "", "literal HTTP path")
	return command
}

func newAddAPICommand(runtime *commandRuntime) *cobra.Command {
	options := addOptions{kind: "api", path: ".", format: "text"}
	command := newAddLeaf(runtime, "api", "Add an API operation", &options)
	flags := command.Flags()
	bindAddContractFlags(command, &options)
	flags.StringVar(&options.httpMethod, "http-method", "", "HTTP method")
	flags.StringVar(&options.httpPath, "http-path", "", "HTTP path template")
	return command
}

func newAddErrorCommand(runtime *commandRuntime) *cobra.Command {
	options := addOptions{kind: "error", path: ".", format: "text"}
	command := newAddLeaf(runtime, "error", "Add a declared service error", &options)
	bindAddContractFlags(command, &options)
	flags := command.Flags()
	flags.StringVar(&options.enumName, "enum", "", "error enum name")
	flags.StringVar(&options.reason, "reason", "", "stable error reason")
	flags.IntVar(&options.enumNumber, "number", 0, "stable enum number")
	flags.IntVar(&options.errorCode, "code", 0, "HTTP error code")
	return command
}

func newAddDependencyCommand(runtime *commandRuntime) *cobra.Command {
	options := addOptions{kind: "dependency", path: ".", format: "text"}
	command := newAddLeaf(runtime, "dependency", "Add a service dependency", &options)
	bindAddContractFlags(command, &options)
	flags := command.Flags()
	flags.StringVar(&options.target, "target", "", "target package and service")
	flags.StringVar(&options.transport, "transport", "", "transport: grpc or http")
	flags.StringVar(&options.binding, "binding", "", "binding: remote, auto, or local")
	flags.StringVar(&options.reason, "reason", "", "stable dependency reason")
	flags.StringVar(&options.protoImport, "proto-import", "", "target proto import")
	return command
}

func bindAddContractFlags(command *cobra.Command, options *addOptions) {
	bindAddProjectFlags(command, options)
	flags := command.Flags()
	flags.StringVar(&options.packageID, "package", "", "protobuf package")
	flags.StringVar(&options.service, "service", "", "service name")
	flags.StringVar(&options.method, "method", "", "method name")
}

func newAddComponentCommand(runtime *commandRuntime) *cobra.Command {
	command := commandGroup("component", "Add an application component")
	for _, component := range []string{
		"redis", "sql", "kafka-producer", "kafka-consumer", "cron-job",
		"job-ownership", "outbox", "inbox", "outbox-maintenance",
		"inbox-maintenance", "saga", "idempotency", "distributed-rate-limit",
	} {
		command.AddCommand(newAddComponentLeaf(runtime, component))
	}
	return command
}

func newAddComponentLeaf(runtime *commandRuntime, component string) *cobra.Command {
	options := addOptions{kind: "component", component: component, path: ".", format: "text"}
	command := newAddLeaf(runtime, component, "Add a "+component+" component", &options)
	bindAddComponentFlags(command, &options, component)
	return command
}

func newAddLeaf(
	runtime *commandRuntime,
	use string,
	short string,
	options *addOptions,
) *cobra.Command {
	command := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if err := validateAddOptions(*options); err != nil {
				return err
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeAdd(ctx, *options, runtime.stdout, runtime.stderr)
			})
		},
	}
	if options.kind != "component" {
		bindAddProjectFlags(command, options)
	}
	return command
}

func bindAddProjectFlags(command *cobra.Command, options *addOptions) {
	flags := command.Flags()
	if flags.Lookup("path") == nil {
		flags.StringVar(&options.path, "path", options.path, "project directory")
	}
	if flags.Lookup("format") == nil {
		flags.StringVar(&options.format, "format", options.format, "output format: text or json")
	}
}

func bindAddComponentFlags(command *cobra.Command, options *addOptions, component string) {
	bindAddProjectFlags(command, options)
	flags := command.Flags()
	flags.StringVar(&options.name, "name", "", "component name")
	switch component {
	case "redis":
		flags.StringVar(&options.mode, "mode", "", "Redis mode: standalone, cluster, or sentinel")
		flags.StringArrayVar(&options.addresses, "address", nil, "Redis address (repeatable)")
		flags.StringVar(&options.username, "username", "", "Redis username")
		flags.StringVar(&options.passwordRef, "password-reference", "", "Redis password secret reference")
		flags.StringVar(&options.sentinelUser, "sentinel-username", "", "Sentinel username")
		flags.StringVar(&options.sentinelPasswordRef, "sentinel-password-reference", "", "Sentinel password secret reference")
		flags.StringVar(&options.masterName, "master-name", "", "Sentinel master name")
		flags.StringVar(&options.clientName, "client-name", "", "Redis client name")
		flags.IntVar(&options.protocol, "protocol", 0, "RESP protocol version")
		flags.IntVar(&options.db, "db", 0, "Redis database")
		flags.IntVar(&options.maxRetries, "max-retries", 0, "maximum retries")
		flags.IntVar(&options.poolSize, "pool-size", 0, "connection pool size")
		flags.IntVar(&options.minIdleConnections, "min-idle-connections", 0, "minimum idle connections")
		flags.IntVar(&options.maxIdleConnections, "max-idle-connections", 0, "maximum idle connections")
	case "sql":
		flags.StringVar(&options.driver, "driver", "", "SQL driver: mysql or postgres")
		flags.StringVar(&options.dsnReference, "dsn-reference", "", "DSN secret reference")
		flags.StringVar(&options.system, "system", "", "database system")
		flags.StringVar(&options.database, "database", "", "database name")
		flags.IntVar(&options.maxIdle, "max-idle", 0, "maximum idle connections")
		flags.IntVar(&options.maxOpen, "max-open", 0, "maximum open connections")
	case "kafka-producer", "kafka-consumer":
		bindKafkaFlags(command, options, component == "kafka-consumer")
	case "cron-job":
		flags.StringVar(&options.cronSpec, "spec", "", "cron expression")
		flags.StringVar(&options.cronTimezone, "timezone", "", "IANA timezone")
		flags.BoolVar(&options.cronSeconds, "seconds", false, "enable a seconds field")
		flags.StringVar(&options.cronOverlap, "overlap", "", "overlap policy: forbid or allow")
		flags.IntVar(&options.maxRetries, "max-retries", 0, "maximum retries")
	case "job-ownership":
		flags.StringVar(&options.componentCronJob, "cron-job", "", "cron-job component")
		flags.StringVar(&options.ownershipProvider, "provider", "", "provider: redis or kubernetes")
		flags.StringVar(&options.componentRedis, "redis", "", "Redis component")
		flags.StringVar(&options.ownershipKey, "key", "", "stable logical job key")
		flags.DurationVar(&options.leaseTTL, "lease-ttl", 0, "ownership lease TTL")
		flags.StringVar(&options.coordinationPrefix, "coordination-prefix", "", "coordination key prefix")
		flags.StringVar(&options.ownershipNamespace, "namespace", "", "Kubernetes namespace")
		flags.StringVar(&options.ownershipLeaseName, "lease-name", "", "pre-created Kubernetes Lease")
	case "outbox":
		flags.StringVar(&options.componentSQL, "sql", "", "SQL component")
		flags.StringVar(&options.componentProducer, "producer", "", "Kafka producer component")
		bindTransactionalStorageFlags(command, options)
		flags.DurationVar(&options.pollInterval, "poll-interval", 0, "poll interval")
		flags.DurationVar(&options.errorDelay, "error-delay", 0, "error delay")
		flags.DurationVar(&options.leaseTTL, "lease-ttl", 0, "lease TTL")
		flags.DurationVar(&options.publishTimeout, "publish-timeout", 0, "publish timeout")
		flags.IntVar(&options.batchSize, "batch-size", 0, "batch size")
		flags.IntVar(&options.maxAttempts, "max-attempts", 0, "maximum attempts")
		flags.DurationVar(&options.retryBase, "retry-base", 0, "retry base delay")
		flags.DurationVar(&options.retryMax, "retry-max", 0, "maximum retry delay")
	case "inbox":
		flags.StringVar(&options.componentSQL, "sql", "", "SQL component")
		flags.StringVar(&options.componentConsumer, "kafka-consumer", "", "Kafka consumer component")
		bindTransactionalStorageFlags(command, options)
		flags.StringVar(&options.consumerScope, "consumer-scope", "", "stable consumer scope")
		flags.DurationVar(&options.retryAfter, "retry-after", 0, "retry delay")
	case "outbox-maintenance":
		flags.StringVar(&options.componentOutbox, "outbox", "", "outbox component")
		flags.StringVar(&options.componentCronJob, "cron-job", "", "cron-job component")
		flags.DurationVar(&options.publishedRetention, "published-retention", 0, "published record retention")
		flags.DurationVar(&options.terminalRetention, "terminal-retention", 0, "terminal record retention")
		bindMaintenanceFlags(command, options)
	case "inbox-maintenance":
		flags.StringVar(&options.componentInbox, "inbox", "", "inbox component")
		flags.StringVar(&options.componentCronJob, "cron-job", "", "cron-job component")
		flags.DurationVar(&options.processedRetention, "processed-retention", 0, "processed record retention")
		bindMaintenanceFlags(command, options)
	case "saga":
		flags.StringVar(&options.componentSQL, "sql", "", "SQL component")
		flags.StringVar(&options.componentRedis, "redis", "", "Redis component")
		flags.StringVar(&options.table, "table", "", "saga table")
		flags.StringVar(&options.coordinationPrefix, "coordination-prefix", "", "coordination key prefix")
		flags.DurationVar(&options.leaseTTL, "lease-ttl", 0, "lease TTL")
		flags.DurationVar(&options.stepTimeout, "step-timeout", 0, "step timeout")
		flags.IntVar(&options.maxCompAttempts, "max-compensation-attempts", 0, "maximum compensation attempts")
	case "idempotency":
		flags.StringVar(&options.componentRedis, "redis", "", "Redis component")
		flags.StringVar(&options.idempotencyPrefix, "prefix", "", "key prefix")
		flags.DurationVar(&options.backendTimeout, "backend-timeout", 0, "backend timeout")
		flags.IntVar(&options.maxResultBytes, "max-result-bytes", 0, "maximum cached result size")
	case "distributed-rate-limit":
		flags.StringVar(&options.componentRedis, "redis", "", "Redis component")
		flags.StringVar(&options.rateLimitPrefix, "prefix", "", "key prefix")
		flags.StringVar(&options.rateLimitFailureMode, "failure-mode", "", "failure mode: fail-closed, fail-open, or local-fallback")
		flags.DurationVar(&options.rateLimitBackendTimeout, "backend-timeout", 0, "backend timeout")
	}
}

func bindKafkaFlags(command *cobra.Command, options *addOptions, consumer bool) {
	flags := command.Flags()
	flags.StringArrayVar(&options.brokers, "broker", nil, "Kafka broker (repeatable)")
	flags.StringVar(&options.kafkaClientID, "client-id", "", "Kafka client ID")
	if consumer {
		flags.StringVar(&options.kafkaGroup, "group", "", "consumer group")
		flags.StringArrayVar(&options.kafkaTopics, "topic", nil, "topic (repeatable)")
		flags.StringVar(&options.deadLetterTopic, "dead-letter-topic", "", "dead-letter topic")
		flags.BoolVar(&options.resetAtStart, "reset-at-start", false, "reset offsets at startup")
	}
	flags.BoolVar(&options.tracePropagation, "trace-propagation", false, "propagate tracing headers")
	flags.IntVar(&options.maxHeaders, "max-headers", 0, "maximum propagated headers")
	flags.IntVar(&options.maxBytes, "max-bytes", 0, "maximum propagated header bytes")
	flags.StringVar(&options.tlsBundleReference, "tls-bundle-reference", "", "TLS bundle secret reference")
	flags.StringVar(&options.tlsServerName, "tls-server-name", "", "TLS server name")
	flags.BoolVar(&options.mutualTLS, "mutual-tls", false, "require mutual TLS")
	flags.StringVar(&options.saslMechanism, "sasl-mechanism", "", "SASL mechanism")
	flags.StringVar(&options.saslCredentialsRef, "sasl-credentials-reference", "", "SASL credentials secret reference")
	flags.BoolVar(&options.allowInsecure, "allow-insecure", false, "allow insecure transport")
}

func bindTransactionalStorageFlags(command *cobra.Command, options *addOptions) {
	flags := command.Flags()
	flags.StringVar(&options.table, "table", "", "database table")
	flags.StringVar(&options.isolation, "isolation", "", "transaction isolation")
}

func bindMaintenanceFlags(command *cobra.Command, options *addOptions) {
	flags := command.Flags()
	flags.IntVar(&options.batchSize, "batch-size", 0, "batch size")
	flags.IntVar(&options.maxBatches, "max-batches", 0, "maximum batches per run")
	flags.DurationVar(&options.retryAfter, "retry-after", 0, "retry delay")
}

func validateAddOptions(options addOptions) error {
	if strings.TrimSpace(options.path) == "" {
		return errors.New("--path must not be empty")
	}
	if err := validateTextJSON(options.format); err != nil {
		return err
	}
	require := func(name, value string) error {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("--%s is required", name)
		}
		return nil
	}
	switch options.kind {
	case "service":
		if strings.TrimSpace(options.name) == "" && strings.TrimSpace(options.service) == "" {
			return errors.New("service name is required (argument or --name)")
		}
		return nil
	case "api":
		return nil
	case "error":
		if err := require("enum", options.enumName); err != nil {
			return err
		}
		if err := require("reason", options.reason); err != nil {
			return err
		}
		if options.enumNumber <= 0 || options.errorCode <= 0 {
			return errors.New("--number and --code must be positive")
		}
		return nil
	case "dependency":
		for _, required := range []struct{ name, value string }{
			{name: "target", value: options.target},
			{name: "binding", value: options.binding},
			{name: "reason", value: options.reason},
			{name: "proto-import", value: options.protoImport},
		} {
			if err := require(required.name, required.value); err != nil {
				return err
			}
		}
		return nil
	case "component":
		if err := require("name", options.name); err != nil {
			return err
		}
	default:
		return errUsage
	}
	switch options.component {
	case "redis":
		return nil
	case "sql":
		if err := require("dsn-reference", options.dsnReference); err != nil {
			return err
		}
		return require("database", options.database)
	case "kafka-producer":
		if len(options.brokers) == 0 {
			return errors.New("--broker is required")
		}
	case "kafka-consumer":
		if len(options.brokers) == 0 || len(options.kafkaTopics) == 0 {
			return errors.New("--broker and --topic are required")
		}
		return require("group", options.kafkaGroup)
	case "cron-job":
		return require("spec", options.cronSpec)
	case "job-ownership":
		if err := require("cron-job", options.componentCronJob); err != nil {
			return err
		}
		if err := require("key", options.ownershipKey); err != nil {
			return err
		}
		provider := strings.ToLower(strings.TrimSpace(options.ownershipProvider))
		if provider == "" || provider == "redis" {
			if options.ownershipNamespace != "" || options.ownershipLeaseName != "" {
				return errors.New("--namespace and --lease-name require --provider kubernetes")
			}
			return require("redis", options.componentRedis)
		}
		if provider != "kubernetes" {
			return errors.New("--provider must be redis or kubernetes")
		}
		if options.componentRedis != "" || options.coordinationPrefix != "" {
			return errors.New("kubernetes ownership does not accept --redis or --coordination-prefix")
		}
		if err := require("namespace", options.ownershipNamespace); err != nil {
			return err
		}
		return require("lease-name", options.ownershipLeaseName)
	case "outbox":
		if err := require("sql", options.componentSQL); err != nil {
			return err
		}
		return require("producer", options.componentProducer)
	case "inbox":
		if err := require("sql", options.componentSQL); err != nil {
			return err
		}
		return require("kafka-consumer", options.componentConsumer)
	case "outbox-maintenance":
		if err := require("outbox", options.componentOutbox); err != nil {
			return err
		}
		return require("cron-job", options.componentCronJob)
	case "inbox-maintenance":
		if err := require("inbox", options.componentInbox); err != nil {
			return err
		}
		if err := require("cron-job", options.componentCronJob); err != nil {
			return err
		}
		if options.processedRetention <= 0 {
			return errors.New("--processed-retention must be positive")
		}
	case "saga":
		if err := require("sql", options.componentSQL); err != nil {
			return err
		}
		return require("redis", options.componentRedis)
	case "idempotency", "distributed-rate-limit":
		return require("redis", options.componentRedis)
	default:
		return fmt.Errorf("unknown component %q", options.component)
	}
	return nil
}
