package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

// ProducerRuntimeConfig is the strict generated-project construction schema.
// Authentication and private tls material are represented only by secret
// references rather than opaque Kafka options or inline credentials.
type ProducerRuntimeConfig struct {
	Brokers          []string             `config:"brokers"`
	ClientID         string               `config:"clientid"`
	TracePropagation bool                 `config:"tracePropagation"`
	MaxHeaders       int                  `config:"maxHeaders"`
	MaxBytes         int                  `config:"maxBytes"`
	Security         ClientSecurityConfig `config:"security"`
}

// ProducerConfig constructs an owned producer with optional bounded context
// propagation.
type ProducerConfig struct {
	Brokers      []string
	ClientID     string
	KafkaOptions []kgo.Opt
	Propagation  PropagationConfig
}

// ProducerDescription is a broker-, topic-, payload-, and error-free snapshot.
type ProducerDescription struct {
	Started                    bool
	Closed                     bool
	HealthChecks               uint64
	HealthFailures             uint64
	PublishAttempts            uint64
	Published                  uint64
	PublishFailures            uint64
	ConsecutivePublishFailures uint64
}

// Producer publishes records synchronously and owns its franz-go client.
type Producer struct {
	client      *kgo.Client
	propagation propagationSettings
	security    *clientSecurity

	lifecycleMu    sync.Mutex
	started        bool
	closed         bool
	healthChecks   uint64
	healthFailures uint64

	publishAttempts            atomic.Uint64
	published                  atomic.Uint64
	publishFailures            atomic.Uint64
	consecutivePublishFailures atomic.Uint64

	closeOnce sync.Once
	closeErr  error
}

// NewProducer creates an owned producer.
func NewProducer(
	brokers []string,
	clientid string,
	options ...kgo.Opt,
) (*Producer, error) {
	return NewProducerWithConfig(ProducerConfig{
		Brokers:      brokers,
		ClientID:     clientid,
		KafkaOptions: options,
	})
}

// NewProducerWithConfig creates an owned producer with explicit propagation.
func NewProducerWithConfig(config ProducerConfig) (*Producer, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("%w: brokers are required", ErrInvalidOption)
	}
	for _, broker := range config.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("%w: broker is empty", ErrInvalidOption)
		}
	}
	propagationSettings, err := normalizePropagation(config.Propagation)
	if err != nil {
		return nil, err
	}
	settings := []kgo.Opt{kgo.SeedBrokers(config.Brokers...)}
	if normalized := strings.TrimSpace(config.ClientID); normalized != "" {
		settings = append(settings, kgo.ClientID(normalized))
	}
	for index, option := range config.KafkaOptions {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: Kafka option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		settings = append(settings, option)
	}
	client, err := kgo.NewClient(settings...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}
	return &Producer{
		client:      client,
		propagation: propagationSettings,
	}, nil
}

// NewConfiguredProducer creates an owned producer from strict runtime config
// and the App's instance-scoped propagator.
func NewConfiguredProducer(
	config ProducerRuntimeConfig,
	propagator propagation.TextMapPropagator,
	secrets SecretManager,
) (*Producer, error) {
	if err := ValidateProducerRuntimeConfig(config); err != nil {
		return nil, err
	}
	propagationConfig := PropagationConfig{
		MaxHeaders: config.MaxHeaders,
		MaxBytes:   config.MaxBytes,
	}
	if config.TracePropagation {
		if propagator == nil {
			return nil, fmt.Errorf(
				"%w: trace propagation requires a propagator",
				ErrInvalidOption,
			)
		}
		propagationConfig.Propagator = propagator
	}
	security, err := newClientSecurity(config.Security, secrets)
	if err != nil {
		return nil, err
	}
	producer, err := NewProducerWithConfig(ProducerConfig{
		Brokers:      append([]string(nil), config.Brokers...),
		ClientID:     config.ClientID,
		KafkaOptions: security.options,
		Propagation:  propagationConfig,
	})
	if err != nil {
		return nil, err
	}
	producer.security = security
	return producer, nil
}

// ValidateProducerRuntimeConfig validates broker topology, stable client
// identity, and propagation budgets.
func ValidateProducerRuntimeConfig(config ProducerRuntimeConfig) error {
	if len(config.Brokers) == 0 || len(config.Brokers) > 64 {
		return fmt.Errorf(
			"%w: broker count is outside 1..64",
			ErrInvalidOption,
		)
	}
	if err := ValidateClientSecurityConfig(config.Security); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(config.Brokers))
	for _, broker := range config.Brokers {
		if strings.TrimSpace(broker) != broker ||
			len(broker) > 512 ||
			!utf8.ValidString(broker) {
			return fmt.Errorf("%w: broker is invalid", ErrInvalidOption)
		}
		host, port, err := net.SplitHostPort(broker)
		number, numberErr := strconv.Atoi(port)
		if err != nil ||
			host == "" ||
			strings.ContainsFunc(host, func(character rune) bool {
				return unicode.IsSpace(character) ||
					unicode.IsControl(character)
			}) ||
			numberErr != nil ||
			number < 1 ||
			number > 65_535 {
			return fmt.Errorf(
				"%w: broker %q must be host:port",
				ErrInvalidOption,
				broker,
			)
		}
		if _, duplicate := seen[broker]; duplicate {
			return fmt.Errorf(
				"%w: broker %q is duplicated",
				ErrInvalidOption,
				broker,
			)
		}
		seen[broker] = struct{}{}
	}
	if config.ClientID != "" {
		if len(config.ClientID) > 256 ||
			strings.TrimSpace(config.ClientID) != config.ClientID ||
			!utf8.ValidString(config.ClientID) ||
			strings.ContainsFunc(config.ClientID, unicode.IsControl) {
			return fmt.Errorf("%w: client id is invalid", ErrInvalidOption)
		}
	}
	if _, err := normalizePropagation(PropagationConfig{
		MaxHeaders: config.MaxHeaders,
		MaxBytes:   config.MaxBytes,
	}); err != nil {
		return err
	}
	return nil
}

// Start verifies broker connectivity.
func (producer *Producer) Start(ctx context.Context) error {
	if producer == nil || producer.client == nil {
		return fmt.Errorf("%w: producer is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	producer.lifecycleMu.Lock()
	producer.healthChecks++
	producer.lifecycleMu.Unlock()
	if err := producer.security.Start(ctx); err != nil {
		producer.lifecycleMu.Lock()
		producer.healthFailures++
		producer.lifecycleMu.Unlock()
		return fmt.Errorf("kafka: producer security: %w", err)
	}
	if err := producer.client.Ping(ctx); err != nil {
		producer.lifecycleMu.Lock()
		producer.healthFailures++
		producer.lifecycleMu.Unlock()
		cleanupErr := producer.security.Shutdown(context.WithoutCancel(ctx))
		return errors.Join(
			fmt.Errorf("kafka: producer ping: %w", err),
			cleanupErr,
		)
	}
	producer.lifecycleMu.Lock()
	if producer.closed {
		producer.lifecycleMu.Unlock()
		return fmt.Errorf("%w: producer is closed", ErrInvalidOption)
	}
	producer.started = true
	producer.lifecycleMu.Unlock()
	return nil
}

// Shutdown flushes buffered records and closes exactly once.
func (producer *Producer) Shutdown(ctx context.Context) error {
	if producer == nil || producer.client == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	producer.closeOnce.Do(func() {
		if err := producer.client.Flush(ctx); err != nil {
			producer.closeErr = errors.Join(
				producer.closeErr,
				fmt.Errorf("kafka: flush: %w", err),
			)
		}
		producer.client.Close()
		producer.closeErr = errors.Join(
			producer.closeErr,
			producer.security.Shutdown(ctx),
		)
		producer.lifecycleMu.Lock()
		producer.started = false
		producer.closed = true
		producer.lifecycleMu.Unlock()
	})
	return producer.closeErr
}

// Publish writes one record and waits for broker acknowledgement.
func (producer *Producer) Publish(
	ctx context.Context,
	topic string,
	key []byte,
	value []byte,
	headers ...Header,
) error {
	if producer == nil || producer.client == nil {
		return fmt.Errorf("%w: producer is nil", ErrInvalidOption)
	}
	normalizedTopic := strings.TrimSpace(topic)
	if normalizedTopic == "" {
		return fmt.Errorf("%w: topic is empty", ErrInvalidOption)
	}
	preparedHeaders, err := prepareMessageHeaders(
		ctx,
		headers,
		producer.propagation,
	)
	if err != nil {
		return err
	}
	recordHeaders := make([]kgo.RecordHeader, len(preparedHeaders))
	for index, header := range preparedHeaders {
		recordHeaders[index] = kgo.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}
	producer.publishAttempts.Add(1)
	result := producer.client.ProduceSync(ctx, &kgo.Record{
		Topic:   normalizedTopic,
		Key:     append([]byte(nil), key...),
		Value:   append([]byte(nil), value...),
		Headers: recordHeaders,
	})
	if err := result.FirstErr(); err != nil {
		producer.recordPublishResult(err)
		return fmt.Errorf("kafka: produce: %w", err)
	}
	producer.recordPublishResult(nil)
	return nil
}

func (producer *Producer) recordPublishResult(err error) {
	if err != nil {
		producer.publishFailures.Add(1)
		producer.consecutivePublishFailures.Add(1)
		return
	}
	producer.published.Add(1)
	producer.consecutivePublishFailures.Store(0)
}

// Description returns aggregate lifecycle and publish counters.
func (producer *Producer) Description() ProducerDescription {
	if producer == nil {
		return ProducerDescription{Closed: true}
	}
	producer.lifecycleMu.Lock()
	description := ProducerDescription{
		Started:        producer.started,
		Closed:         producer.closed,
		HealthChecks:   producer.healthChecks,
		HealthFailures: producer.healthFailures,
	}
	producer.lifecycleMu.Unlock()
	description.PublishAttempts = producer.publishAttempts.Load()
	description.Published = producer.published.Load()
	description.PublishFailures = producer.publishFailures.Load()
	description.ConsecutivePublishFailures =
		producer.consecutivePublishFailures.Load()
	return description
}
