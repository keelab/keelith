package scaffold

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/secret"
	robfig "github.com/robfig/cron/v3"
)

const (
	componentMaxRedisAddresses   = 32
	componentMaxRedisPoolSize    = 100_000
	componentMaxKafkaHeaders     = 256
	componentMaxKafkaBytes       = 1024 * 1024
	componentMaxRetentionBatches = 100
	componentMaxRetentionBatch   = 10_000
)

var (
	dns1123LabelPattern = regexp.MustCompile(
		`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`,
	)
	dns1123SubdomainPattern = regexp.MustCompile(
		`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*$`,
	)
)

type componentKafkaSecurityConfig struct {
	TLSEnabled         bool
	TLSBundleReference string
	TLSServerName      string
	MutualTLS          bool
	AllowInsecure      bool
	SASLMechanism      string
	SASLCredentialRef  string
}

type outboxYAMLConfig struct {
	Table          string        `yaml:"table"`
	Isolation      string        `yaml:"isolation"`
	PollInterval   time.Duration `yaml:"pollInterval"`
	ErrorDelay     time.Duration `yaml:"errorDelay"`
	LeaseTTL       time.Duration `yaml:"leaseTTL"`
	PublishTimeout time.Duration `yaml:"publishTimeout"`
	BatchSize      int           `yaml:"batchSize"`
	MaxAttempts    int           `yaml:"maxAttempts"`
	RetryBase      time.Duration `yaml:"retryBase"`
	RetryMax       time.Duration `yaml:"retryMax"`
}

type outboxRetentionYAMLConfig struct {
	PublishedRetention time.Duration `yaml:"publishedRetention"`
	TerminalRetention  time.Duration `yaml:"terminalRetention"`
	BatchSize          int           `yaml:"batchSize"`
	MaxBatches         int           `yaml:"maxBatches"`
	RetryAfter         time.Duration `yaml:"retryAfter"`
}

type inboxRetentionYAMLConfig struct {
	ProcessedRetention time.Duration `yaml:"processedRetention"`
	BatchSize          int           `yaml:"batchSize"`
	MaxBatches         int           `yaml:"maxBatches"`
	RetryAfter         time.Duration `yaml:"retryAfter"`
}

type sagaYAMLConfig struct {
	Table                   string        `yaml:"table"`
	CoordinationPrefix      string        `yaml:"coordinationPrefix"`
	LeaseTTL                time.Duration `yaml:"leaseTTL"`
	StepTimeout             time.Duration `yaml:"stepTimeout"`
	MaxCompensationAttempts int           `yaml:"maxCompensationAttempts"`
}

type inboxYAMLConfig struct {
	Table      string        `yaml:"table"`
	Isolation  string        `yaml:"isolation"`
	Consumer   string        `yaml:"consumer"`
	RetryAfter time.Duration `yaml:"retryAfter"`
}

func validateRedisComponentConfig(config redisYAMLConfig) error {
	switch config.Mode {
	case "standalone", "cluster", "sentinel":
	default:
		return fmt.Errorf("mode %q is unsupported", config.Mode)
	}
	if len(config.Addresses) == 0 ||
		len(config.Addresses) > componentMaxRedisAddresses {
		return fmt.Errorf("address count is outside 1..%d", componentMaxRedisAddresses)
	}
	seen := make(map[string]struct{}, len(config.Addresses))
	for _, address := range config.Addresses {
		if !validComponentText(address, 512) || !validHostPort(address) {
			return fmt.Errorf("address %q must be host:port", address)
		}
		if _, duplicate := seen[address]; duplicate {
			return fmt.Errorf("address %q is duplicated", address)
		}
		seen[address] = struct{}{}
	}
	for _, value := range []string{
		config.Username,
		config.SentinelUsername,
		config.ClientName,
		config.MasterName,
	} {
		if value != "" && !validComponentText(value, 512) {
			return fmt.Errorf("identity contains invalid text")
		}
	}
	for _, reference := range []string{
		config.PasswordReference,
		config.SentinelPasswordReference,
	} {
		if reference != "" {
			if _, err := secret.Parse(reference); err != nil {
				return fmt.Errorf("secret reference is malformed")
			}
		}
	}
	switch config.Mode {
	case "standalone":
		if len(config.Addresses) != 1 || config.MasterName != "" ||
			config.SentinelUsername != "" ||
			config.SentinelPasswordReference != "" {
			return fmt.Errorf("standalone topology contains sentinel settings")
		}
	case "cluster":
		if config.DB != 0 || config.MasterName != "" ||
			config.SentinelUsername != "" ||
			config.SentinelPasswordReference != "" {
			return fmt.Errorf("cluster topology contains standalone settings")
		}
	case "sentinel":
		if config.MasterName == "" {
			return fmt.Errorf("sentinel requires masterName")
		}
	}
	if config.Protocol != 0 && config.Protocol != 2 && config.Protocol != 3 ||
		config.DB < 0 ||
		config.MaxRetries < -1 || config.MaxRetries > 100 ||
		config.PoolSize < 0 || config.PoolSize > componentMaxRedisPoolSize ||
		config.MinIdleConnections < 0 ||
		config.MinIdleConnections > componentMaxRedisPoolSize ||
		config.MaxIdleConnections < 0 ||
		config.MaxIdleConnections > componentMaxRedisPoolSize ||
		config.PoolSize > 0 &&
			(config.MinIdleConnections > config.PoolSize ||
				config.MaxIdleConnections > config.PoolSize) {
		return fmt.Errorf("db, retry, or pool settings are invalid")
	}
	return nil
}

func validateSQLComponentConfig(config sqlYAMLConfig) error {
	if config.Driver != "mysql" && config.Driver != "pgx" {
		return fmt.Errorf("sql driver name is invalid")
	}
	if _, err := secret.Parse(config.DSNReference); err != nil {
		return fmt.Errorf("sql dsn reference is invalid")
	}
	if !config.Pool.Owns ||
		!validNormalizedIdentity(config.Pool.System, 0) ||
		!validNormalizedIdentity(config.Pool.Name, 0) ||
		config.Pool.MaxIdle < 0 ||
		config.Pool.MaxOpen < 0 ||
		config.Pool.MaxOpen > 0 && config.Pool.MaxIdle > config.Pool.MaxOpen {
		return fmt.Errorf("sql pool settings are invalid")
	}
	return nil
}

func validateOutboxComponentConfig(config outboxYAMLConfig, dialect string) error {
	if !validSQLTable(config.Table, dialect) || !validSQLIsolation(config.Isolation) {
		return fmt.Errorf("outbox table or isolation is invalid")
	}
	if config.PollInterval <= 0 ||
		config.ErrorDelay <= 0 ||
		config.LeaseTTL <= 0 ||
		config.PublishTimeout <= 0 ||
		config.PublishTimeout >= config.LeaseTTL ||
		config.BatchSize <= 0 || config.BatchSize > componentMaxRetentionBatch ||
		config.MaxAttempts <= 0 ||
		config.RetryBase <= 0 ||
		config.RetryMax < config.RetryBase {
		return fmt.Errorf("outbox dispatcher budgets are invalid")
	}
	return nil
}

func validateOutboxRetentionConfig(config outboxRetentionYAMLConfig) error {
	if config.PublishedRetention <= 0 ||
		config.TerminalRetention <= 0 ||
		config.BatchSize <= 0 || config.BatchSize > componentMaxRetentionBatch ||
		config.MaxBatches <= 0 || config.MaxBatches > componentMaxRetentionBatches ||
		config.RetryAfter <= 0 {
		return fmt.Errorf("outbox retention budgets are invalid")
	}
	return nil
}

func validateInboxRetentionConfig(config inboxRetentionYAMLConfig) error {
	if config.ProcessedRetention <= 0 ||
		config.BatchSize <= 0 || config.BatchSize > componentMaxRetentionBatch ||
		config.MaxBatches <= 0 || config.MaxBatches > componentMaxRetentionBatches ||
		config.RetryAfter <= 0 {
		return fmt.Errorf("inbox retention budgets are invalid")
	}
	return nil
}

func validateSagaComponentConfig(config sagaYAMLConfig, dialect string) error {
	if !validSQLTable(config.Table, dialect) ||
		!validCoordinationPrefix(config.CoordinationPrefix) ||
		config.LeaseTTL < 100*time.Millisecond ||
		config.LeaseTTL > 10*time.Minute ||
		config.StepTimeout < 10*time.Millisecond ||
		config.StepTimeout > 10*time.Minute ||
		config.MaxCompensationAttempts < 1 ||
		config.MaxCompensationAttempts > 1_000 {
		return fmt.Errorf("saga configuration is invalid")
	}
	return nil
}

func validateInboxComponentConfig(config inboxYAMLConfig, dialect string) error {
	if !validSQLTable(config.Table, dialect) ||
		!validSQLIsolation(config.Isolation) ||
		!validNormalizedIdentity(config.Consumer, 256) ||
		config.RetryAfter <= 0 {
		return fmt.Errorf("inbox configuration is invalid")
	}
	return nil
}

func validateKafkaProducerComponentConfig(config kafkaProducerYAMLConfig) error {
	if len(config.Brokers) == 0 || len(config.Brokers) > 64 {
		return fmt.Errorf("broker count is outside 1..64")
	}
	seen := make(map[string]struct{}, len(config.Brokers))
	for _, broker := range config.Brokers {
		if len(broker) > 512 || !validComponentText(broker, 512) ||
			!validHostPort(broker) {
			return fmt.Errorf("broker %q must be host:port", broker)
		}
		if _, duplicate := seen[broker]; duplicate {
			return fmt.Errorf("broker %q is duplicated", broker)
		}
		seen[broker] = struct{}{}
	}
	if config.ClientID != "" && !validNormalizedIdentity(config.ClientID, 256) {
		return fmt.Errorf("client ID is invalid")
	}
	if config.MaxHeaders != 0 &&
		(config.MaxHeaders < 1 || config.MaxHeaders > componentMaxKafkaHeaders) {
		return fmt.Errorf("propagation header count is outside supported bounds")
	}
	if config.MaxBytes != 0 &&
		(config.MaxBytes < 256 || config.MaxBytes > componentMaxKafkaBytes) {
		return fmt.Errorf("propagation byte budget is outside supported bounds")
	}
	return validateKafkaSecurityComponentConfig(config.Security)
}

func validateKafkaConsumerComponentConfig(config kafkaConsumerYAMLConfig) error {
	if err := validateKafkaProducerComponentConfig(kafkaProducerYAMLConfig{
		Brokers:          config.Brokers,
		ClientID:         config.ClientID,
		TracePropagation: config.TracePropagation,
		MaxHeaders:       config.MaxHeaders,
		MaxBytes:         config.MaxBytes,
		Security:         config.Security,
	}); err != nil {
		return err
	}
	if !validNormalizedIdentity(config.Group, 256) {
		return fmt.Errorf("consumer group is invalid")
	}
	if len(config.Topics) == 0 || len(config.Topics) > 128 {
		return fmt.Errorf("topic count is outside 1..128")
	}
	seen := make(map[string]struct{}, len(config.Topics))
	for _, topic := range config.Topics {
		if !validKafkaTopic(topic) {
			return fmt.Errorf("topic is invalid")
		}
		if _, duplicate := seen[topic]; duplicate {
			return fmt.Errorf("topic %q is duplicated", topic)
		}
		seen[topic] = struct{}{}
	}
	if config.DeadLetterTopic != "" && !validKafkaTopic(config.DeadLetterTopic) {
		return fmt.Errorf("dead-letter topic is invalid")
	}
	if _, duplicate := seen[config.DeadLetterTopic]; config.DeadLetterTopic != "" && duplicate {
		return fmt.Errorf("dead-letter topic must not be a source topic")
	}
	return nil
}

func validateKafkaSecurityComponentConfig(config kafkaSecurityYAMLConfig) error {
	if config.TLS.Enabled == config.AllowInsecure {
		return fmt.Errorf("exactly one of TLS or allow-insecure must be selected")
	}
	if config.TLS.ServerName != "" &&
		!validNormalizedIdentity(config.TLS.ServerName, 253) {
		return fmt.Errorf("tls server name is invalid")
	}
	if config.TLS.BundleReference != "" {
		if !config.TLS.Enabled {
			return fmt.Errorf("tls bundle requires tls")
		}
		if _, err := secret.Parse(config.TLS.BundleReference); err != nil {
			return fmt.Errorf("tls bundle reference is invalid")
		}
	}
	if config.TLS.MutualTLS && config.TLS.BundleReference == "" {
		return fmt.Errorf("mTLS requires an atomic TLS bundle reference")
	}
	if !config.TLS.Enabled &&
		(config.TLS.BundleReference != "" || config.TLS.ServerName != "" ||
			config.TLS.MutualTLS) {
		return fmt.Errorf("disabled TLS contains active settings")
	}
	if config.SASL == nil {
		return nil
	}
	switch config.SASL.Mechanism {
	case "":
		if config.SASL.CredentialsReference != "" {
			return fmt.Errorf("sasl credentials require a mechanism")
		}
	case "plain", "scram-sha-256", "scram-sha-512":
		if !config.TLS.Enabled {
			return fmt.Errorf("sasl requires tls")
		}
		if _, err := secret.Parse(config.SASL.CredentialsReference); err != nil {
			return fmt.Errorf("sasl credentials reference is invalid")
		}
	default:
		return fmt.Errorf("unsupported SASL mechanism %q", config.SASL.Mechanism)
	}
	return nil
}

func validateCronJobComponentConfig(config cronJobYAMLConfig) error {
	if !validNormalizedIdentity(config.Name, 0) || config.Spec == "" ||
		strings.TrimSpace(config.Spec) != config.Spec ||
		strings.TrimSpace(config.Timezone) != config.Timezone ||
		config.Overlap != "forbid" && config.Overlap != "allow" ||
		config.Misfire != "skip" ||
		config.MaxRetries < 0 || config.MaxRetries > 10 {
		return fmt.Errorf("cron job configuration is invalid")
	}
	if _, err := time.LoadLocation(config.Timezone); err != nil {
		return fmt.Errorf("cron job timezone is invalid")
	}
	parserOptions := robfig.Minute | robfig.Hour | robfig.Dom |
		robfig.Month | robfig.Dow | robfig.Descriptor
	if config.Seconds {
		parserOptions |= robfig.Second
	}
	if _, err := robfig.NewParser(parserOptions).Parse(config.Spec); err != nil {
		return fmt.Errorf("cron job schedule is invalid")
	}
	return nil
}

func validateRedisJobOwnershipConfig(config jobOwnershipYAMLConfig) error {
	return validateJobOwnershipBase(config, func() bool {
		return validCoordinationPrefix(config.Prefix)
	})
}

func validateKubernetesJobOwnershipConfig(config jobOwnershipYAMLConfig) error {
	return validateJobOwnershipBase(config, func() bool {
		return validDNS1123Label(config.Namespace) &&
			validDNS1123Subdomain(config.LeaseName)
	})
}

func validateJobOwnershipBase(
	config jobOwnershipYAMLConfig,
	providerValid func() bool,
) error {
	if !validNormalizedIdentity(config.Key, 512) ||
		config.TTL < 3*time.Second || config.TTL > 24*time.Hour ||
		!providerValid() {
		return fmt.Errorf("job ownership configuration is invalid")
	}
	return nil
}

func validSQLTable(value string, dialect string) bool {
	maxBytes := 0
	switch dialect {
	case "mysql":
		maxBytes = 64
	case "postgres":
		maxBytes = 63
	default:
		return false
	}
	parts := strings.Split(value, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || len(part) > maxBytes || !utf8.ValidString(part) {
			return false
		}
		for index, r := range part {
			if r == '_' || unicode.IsLetter(r) ||
				index > 0 && unicode.IsDigit(r) {
				continue
			}
			return false
		}
	}
	return true
}

func validSQLIsolation(value string) bool {
	switch value {
	case "", "default", "read-committed", "repeatable-read", "serializable":
		return true
	default:
		return false
	}
}

func validCoordinationPrefix(value string) bool {
	return value != "" && len(value) <= 256 &&
		strings.TrimSpace(value) == value && utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\r\n\x00{}")
}

func validKafkaTopic(value string) bool {
	if value == "." || value == ".." || len(value) == 0 || len(value) > 249 {
		return false
	}
	for index := 0; index < len(value); index++ {
		r := value[index]
		if r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func validHostPort(value string) bool {
	host, portText, err := net.SplitHostPort(value)
	port, portErr := strconv.Atoi(portText)
	return err == nil && host != "" &&
		!strings.ContainsFunc(host, func(r rune) bool {
			return unicode.IsSpace(r) || unicode.IsControl(r)
		}) && portErr == nil && port >= 1 && port <= 65_535
}

func validComponentText(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validNormalizedIdentity(value string, maxBytes int) bool {
	return value != "" && (maxBytes == 0 || len(value) <= maxBytes) &&
		utf8.ValidString(value) && strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validDNS1123Label(value string) bool {
	return len(value) <= 63 && dns1123LabelPattern.MatchString(value)
}

func validDNS1123Subdomain(value string) bool {
	return len(value) <= 253 && dns1123SubdomainPattern.MatchString(value)
}
