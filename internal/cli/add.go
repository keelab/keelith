package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
	"unicode"

	"github.com/keelab/keelith/internal/projectinfo"
	"github.com/keelab/keelith/internal/scaffold"
)

type addOptions struct {
	kind                    string
	path                    string
	packageID               string
	service                 string
	method                  string
	httpMethod              string
	httpPath                string
	enumName                string
	enumNumber              int
	errorCode               int
	target                  string
	transport               string
	binding                 string
	reason                  string
	protoImport             string
	component               string
	name                    string
	mode                    string
	addresses               []string
	username                string
	passwordRef             string
	sentinelUser            string
	sentinelPasswordRef     string
	masterName              string
	clientName              string
	protocol                int
	db                      int
	maxRetries              int
	poolSize                int
	minIdleConnections      int
	maxIdleConnections      int
	driver                  string
	dsnReference            string
	system                  string
	database                string
	maxIdle                 int
	maxOpen                 int
	brokers                 []string
	kafkaClientID           string
	kafkaGroup              string
	kafkaTopics             []string
	deadLetterTopic         string
	resetAtStart            bool
	tracePropagation        bool
	maxHeaders              int
	maxBytes                int
	allowInsecure           bool
	tlsBundleReference      string
	tlsServerName           string
	mutualTLS               bool
	saslMechanism           string
	saslCredentialsRef      string
	cronSpec                string
	cronTimezone            string
	cronSeconds             bool
	cronOverlap             string
	ownershipKey            string
	ownershipProvider       string
	ownershipNamespace      string
	ownershipLeaseName      string
	componentSQL            string
	componentProducer       string
	componentConsumer       string
	table                   string
	isolation               string
	pollInterval            time.Duration
	errorDelay              time.Duration
	leaseTTL                time.Duration
	publishTimeout          time.Duration
	batchSize               int
	maxAttempts             int
	retryBase               time.Duration
	retryMax                time.Duration
	consumerScope           string
	retryAfter              time.Duration
	componentOutbox         string
	componentInbox          string
	componentCronJob        string
	publishedRetention      time.Duration
	terminalRetention       time.Duration
	processedRetention      time.Duration
	maxBatches              int
	componentRedis          string
	coordinationPrefix      string
	stepTimeout             time.Duration
	maxCompAttempts         int
	idempotencyPrefix       string
	backendTimeout          time.Duration
	maxResultBytes          int
	rateLimitPrefix         string
	rateLimitFailureMode    string
	rateLimitBackendTimeout time.Duration
	format                  string
}

type addResult struct {
	Kind      string   `json:"kind"`
	Project   string   `json:"project"`
	Created   []string `json:"created"`
	Updated   []string `json:"updated"`
	Unchanged []string `json:"unchanged"`
}

func executeAdd(
	ctx context.Context,
	options addOptions,
	stdout io.Writer,
	stderr io.Writer,
) int {
	project, err := existingProjectRoot(options.path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith add: %v\n", err)
		return 1
	}
	identity, err := projectinfo.Load(project)
	if err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"keelith add: read Go project: %v\n",
			err,
		)
		return 1
	}
	serviceVariable := path.Base(identity.Module)
	var created, updated, unchanged []string
	addedProject := project
	switch options.kind {
	case "service":
		created, updated, unchanged, err = executeAddService(
			ctx,
			project,
			identity.Module,
			options,
		)
	case "api":
		if options.packageID == "" {
			options.packageID = strings.ToLower(identifier(serviceVariable)) + ".v1"
		}
		if options.service == "" {
			options.service = exportedIdentifier(serviceVariable) + "Service"
		}
		if options.method == "" {
			options.method = "Get"
		}
		if options.httpMethod == "" {
			options.httpMethod = "GET"
		}
		if options.httpPath == "" {
			resource := strings.ToLower(identifier(serviceVariable))
			options.httpPath = "/v1/" + resource + "/{id}"
		}
		var added scaffold.AddAPIResult
		added, err = scaffold.AddAPI(ctx, scaffold.AddAPIOptions{
			Project:    project,
			Package:    options.packageID,
			Service:    options.service,
			Method:     options.method,
			HTTPMethod: options.httpMethod,
			HTTPPath:   options.httpPath,
			Module:     identity.Module,
		})
		addedProject = added.Project
		created = added.Created
		updated = added.Updated
		unchanged = added.Unchanged
	case "error":
		if options.packageID == "" {
			options.packageID = strings.ToLower(identifier(serviceVariable)) + ".v1"
		}
		if options.service == "" {
			options.service = exportedIdentifier(serviceVariable) + "Service"
		}
		var added scaffold.AddAPIResult
		added, err = scaffold.AddError(ctx, scaffold.AddErrorOptions{
			Project: project,
			Package: options.packageID,
			Service: options.service,
			Enum:    options.enumName,
			Reason:  options.reason,
			Number:  int32(options.enumNumber),
			Code:    int32(options.errorCode),
		})
		addedProject = added.Project
		created = added.Created
		updated = added.Updated
		unchanged = added.Unchanged
	case "dependency":
		if options.packageID == "" {
			options.packageID = strings.ToLower(identifier(serviceVariable)) + ".v1"
		}
		if options.service == "" {
			options.service = exportedIdentifier(serviceVariable) + "Service"
		}
		if options.method == "" {
			options.method = "Get"
		}
		if options.transport == "" {
			options.transport = "grpc"
		}
		var added scaffold.AddAPIResult
		added, err = scaffold.AddDependency(
			ctx,
			scaffold.AddDependencyOptions{
				Project:     project,
				Package:     options.packageID,
				Service:     options.service,
				Method:      options.method,
				Target:      options.target,
				Transport:   options.transport,
				Binding:     options.binding,
				Reason:      options.reason,
				ProtoImport: options.protoImport,
			},
		)
		addedProject = added.Project
		created = added.Created
		updated = added.Updated
		unchanged = added.Unchanged
	case "component":
		var added scaffold.AddComponentResult
		switch options.component {
		case "redis":
			if options.mode == "" {
				options.mode = "standalone"
			}
			if len(options.addresses) == 0 {
				options.addresses = []string{"127.0.0.1:6379"}
			}
			added, err = scaffold.AddRedisComponent(
				ctx,
				scaffold.AddRedisComponentOptions{
					Project:                   project,
					Name:                      options.name,
					Mode:                      options.mode,
					Addresses:                 options.addresses,
					Username:                  options.username,
					PasswordReference:         options.passwordRef,
					SentinelUsername:          options.sentinelUser,
					SentinelPasswordReference: options.sentinelPasswordRef,
					MasterName:                options.masterName,
					ClientName:                options.clientName,
					Protocol:                  options.protocol,
					DB:                        options.db,
					MaxRetries:                options.maxRetries,
					PoolSize:                  options.poolSize,
					MinIdleConnections:        options.minIdleConnections,
					MaxIdleConnections:        options.maxIdleConnections,
				},
			)
		case "sql":
			if options.driver == "" {
				options.driver = "mysql"
			}
			if options.system == "" {
				switch strings.ToLower(options.driver) {
				case "postgres", "postgresql", "pgx":
					options.system = "postgresql"
				default:
					options.system = "mysql"
				}
			}
			added, err = scaffold.AddSQLComponent(
				ctx,
				scaffold.AddSQLComponentOptions{
					Project:      project,
					Name:         options.name,
					Driver:       options.driver,
					DSNReference: options.dsnReference,
					System:       options.system,
					Database:     options.database,
					MaxIdle:      options.maxIdle,
					MaxOpen:      options.maxOpen,
				},
			)
		case "kafka-producer":
			added, err = scaffold.AddKafkaProducerComponent(
				ctx,
				scaffold.AddKafkaProducerComponentOptions{
					Project:                  project,
					Name:                     options.name,
					Brokers:                  options.brokers,
					ClientID:                 options.kafkaClientID,
					TracePropagation:         options.tracePropagation,
					MaxHeaders:               options.maxHeaders,
					MaxBytes:                 options.maxBytes,
					AllowInsecure:            options.allowInsecure,
					TLSBundleReference:       options.tlsBundleReference,
					TLSServerName:            options.tlsServerName,
					MutualTLS:                options.mutualTLS,
					SASLMechanism:            options.saslMechanism,
					SASLCredentialsReference: options.saslCredentialsRef,
				},
			)
		case "kafka-consumer":
			added, err = scaffold.AddKafkaConsumerComponent(
				ctx,
				scaffold.AddKafkaConsumerComponentOptions{
					Project:                  project,
					Name:                     options.name,
					Brokers:                  options.brokers,
					Group:                    options.kafkaGroup,
					Topics:                   options.kafkaTopics,
					ClientID:                 options.kafkaClientID,
					DeadLetterTopic:          options.deadLetterTopic,
					ResetAtStart:             options.resetAtStart,
					TracePropagation:         options.tracePropagation,
					MaxHeaders:               options.maxHeaders,
					MaxBytes:                 options.maxBytes,
					AllowInsecure:            options.allowInsecure,
					TLSBundleReference:       options.tlsBundleReference,
					TLSServerName:            options.tlsServerName,
					MutualTLS:                options.mutualTLS,
					SASLMechanism:            options.saslMechanism,
					SASLCredentialsReference: options.saslCredentialsRef,
				},
			)
		case "cron-job":
			added, err = scaffold.AddCronJobComponent(
				ctx,
				scaffold.AddCronJobComponentOptions{
					Project:    project,
					Name:       options.name,
					Spec:       options.cronSpec,
					Timezone:   options.cronTimezone,
					Seconds:    options.cronSeconds,
					Overlap:    options.cronOverlap,
					MaxRetries: options.maxRetries,
				},
			)
		case "job-ownership":
			provider := strings.ToLower(strings.TrimSpace(options.ownershipProvider))
			if provider == "" {
				provider = "redis"
			}
			service := ""
			if provider == "kubernetes" {
				service = serviceVariable
			}
			added, err = scaffold.AddJobOwnershipComponent(
				ctx,
				scaffold.AddJobOwnershipComponentOptions{
					Project:            project,
					Service:            service,
					Name:               options.name,
					Provider:           provider,
					CronJob:            options.componentCronJob,
					Redis:              options.componentRedis,
					Key:                options.ownershipKey,
					CoordinationPrefix: options.coordinationPrefix,
					LeaseTTL:           options.leaseTTL,
					Namespace:          options.ownershipNamespace,
					LeaseName:          options.ownershipLeaseName,
				},
			)
		case "outbox":
			added, err = scaffold.AddOutboxComponent(
				ctx,
				scaffold.AddOutboxComponentOptions{
					Project:        project,
					Name:           options.name,
					SQL:            options.componentSQL,
					KafkaProducer:  options.componentProducer,
					Table:          options.table,
					Isolation:      options.isolation,
					PollInterval:   options.pollInterval,
					ErrorDelay:     options.errorDelay,
					LeaseTTL:       options.leaseTTL,
					PublishTimeout: options.publishTimeout,
					BatchSize:      options.batchSize,
					MaxAttempts:    options.maxAttempts,
					RetryBase:      options.retryBase,
					RetryMax:       options.retryMax,
				},
			)
		case "inbox":
			added, err = scaffold.AddInboxComponent(
				ctx,
				scaffold.AddInboxComponentOptions{
					Project:       project,
					Name:          options.name,
					SQL:           options.componentSQL,
					KafkaConsumer: options.componentConsumer,
					Table:         options.table,
					Isolation:     options.isolation,
					Consumer:      options.consumerScope,
					RetryAfter:    options.retryAfter,
				},
			)
		case "outbox-maintenance":
			added, err = scaffold.AddOutboxMaintenanceComponent(
				ctx,
				scaffold.AddOutboxMaintenanceComponentOptions{
					Project:            project,
					Name:               options.name,
					Outbox:             options.componentOutbox,
					CronJob:            options.componentCronJob,
					PublishedRetention: options.publishedRetention,
					TerminalRetention:  options.terminalRetention,
					BatchSize:          options.batchSize,
					MaxBatches:         options.maxBatches,
					RetryAfter:         options.retryAfter,
				},
			)
		case "inbox-maintenance":
			added, err = scaffold.AddInboxMaintenanceComponent(
				ctx,
				scaffold.AddInboxMaintenanceComponentOptions{
					Project:            project,
					Name:               options.name,
					Inbox:              options.componentInbox,
					CronJob:            options.componentCronJob,
					ProcessedRetention: options.processedRetention,
					BatchSize:          options.batchSize,
					MaxBatches:         options.maxBatches,
					RetryAfter:         options.retryAfter,
				},
			)
		case "saga":
			added, err = scaffold.AddSagaComponent(
				ctx,
				scaffold.AddSagaComponentOptions{
					Project:                 project,
					Name:                    options.name,
					SQL:                     options.componentSQL,
					Redis:                   options.componentRedis,
					Table:                   options.table,
					CoordinationPrefix:      options.coordinationPrefix,
					LeaseTTL:                options.leaseTTL,
					StepTimeout:             options.stepTimeout,
					MaxCompensationAttempts: options.maxCompAttempts,
				},
			)
		case "idempotency":
			added, err = scaffold.AddIdempotencyComponent(
				ctx,
				scaffold.AddIdempotencyComponentOptions{
					Project:        project,
					Name:           options.name,
					Redis:          options.componentRedis,
					Prefix:         options.idempotencyPrefix,
					BackendTimeout: options.backendTimeout,
					MaxResultBytes: options.maxResultBytes,
				},
			)
		case "distributed-rate-limit":
			added, err = scaffold.AddDistributedRateLimitComponent(
				ctx,
				scaffold.AddDistributedRateLimitComponentOptions{
					Project:        project,
					Name:           options.name,
					Redis:          options.componentRedis,
					Prefix:         options.rateLimitPrefix,
					FailureMode:    options.rateLimitFailureMode,
					BackendTimeout: options.rateLimitBackendTimeout,
				},
			)
		}
		addedProject = added.Project
		created = added.Created
		updated = added.Updated
		unchanged = added.Unchanged
	default:
		err = errUsage
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith add: %v\n", err)
		return 1
	}
	resultKind := options.kind
	if options.kind == "component" {
		resultKind += "." + options.component
	}
	result := addResult{
		Kind:      resultKind,
		Project:   addedProject,
		Created:   created,
		Updated:   updated,
		Unchanged: unchanged,
	}
	if options.format == "json" {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith add: encode result: %v\n", err)
			return 1
		}
		return 0
	}
	_, _ = fmt.Fprintf(
		stdout,
		"added %s declaration in %s (%d created, %d updated, %d unchanged)\n",
		result.Kind,
		result.Project,
		len(result.Created),
		len(result.Updated),
		len(result.Unchanged),
	)
	return 0
}

func exportedIdentifier(value string) string {
	normalized := identifier(value)
	runes := []rune(normalized)
	if len(runes) == 0 {
		return "Service"
	}
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}
