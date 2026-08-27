// Package kafka provides a franz-go backed Keelith Worker adapter.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/worker"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

var (
	// ErrInvalidOption reports invalid Kafka client or consumer configuration.
	ErrInvalidOption = errors.New("kafka: invalid option")
	// ErrNotRunning reports a lifecycle call before Subscribe.
	ErrNotRunning = errors.New("kafka: consumer is not running")
	// ErrNoDeadLetterTopic reports DeadLetter without an explicit destination.
	ErrNoDeadLetterTopic = errors.New("kafka: dead-letter topic is not configured")
)

// Header is one Kafka record header.
type Header struct {
	Key   string
	Value []byte
}

// Record is an infrastructure-neutral Kafka delivery used by Backend.
type Record struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Timestamp   time.Time
	Key         []byte
	Value       []byte
	Headers     []Header
}

// Backend isolates the Consumer state machine from franz-go.
type Backend interface {
	Ping(context.Context) error
	Poll(context.Context) (Record, error)
	Commit(context.Context, Record) error
	Retry(Record, time.Duration)
	DeadLetter(context.Context, string, Record, error) error
	AllowRebalance()
	Close()
}

// ConsumerConfig creates an owned franz-go consumer.
type ConsumerConfig struct {
	Brokers         []string `config:"brokers"`
	Group           string   `config:"group"`
	Topics          []string `config:"topics"`
	ClientID        string   `config:"clientid"`
	DeadLetterTopic string   `config:"deadLetterTopic"`
	ResetAtStart    bool     `config:"resetAtStart"`
}

// ConsumerRuntimeConfig is the strict generated-project construction schema.
// Broker identity, subscription identity, propagation, and transport security
// are restart-bound. Authentication and private tls material use only secret
// references.
type ConsumerRuntimeConfig struct {
	Brokers          []string             `config:"brokers"`
	Group            string               `config:"group"`
	Topics           []string             `config:"topics"`
	ClientID         string               `config:"clientid"`
	DeadLetterTopic  string               `config:"deadLetterTopic"`
	ResetAtStart     bool                 `config:"resetAtStart"`
	TracePropagation bool                 `config:"tracePropagation"`
	MaxHeaders       int                  `config:"maxHeaders"`
	MaxBytes         int                  `config:"maxBytes"`
	Security         ClientSecurityConfig `config:"security"`
}

// ConsumerDescription is a value-free Kafka delivery lifecycle snapshot.
type ConsumerDescription struct {
	Running           bool
	StopRequested     bool
	InFlight          bool
	Finished          bool
	Failed            bool
	Delivered         uint64
	Acknowledged      uint64
	Retried           uint64
	DeadLettered      uint64
	NegativelyAcked   uint64
	RebalanceReleases uint64
}

// Consumer implements worker.Consumer with explicit commit semantics.
type Consumer struct {
	backend         Backend
	deadLetterTopic string
	owns            bool
	propagation     propagationSettings
	security        *clientSecurity

	mu             sync.Mutex
	handler        worker.ConsumerHandler
	pollCancel     context.CancelCauseFunc
	deliveryCancel context.CancelCauseFunc
	runErr         error
	started        bool
	stopRequested  bool
	inflight       bool
	finished       bool

	done      chan struct{}
	closeOnce sync.Once
	closeErr  error

	delivered         atomic.Uint64
	acknowledged      atomic.Uint64
	retried           atomic.Uint64
	deadLettered      atomic.Uint64
	negativelyAcked   atomic.Uint64
	rebalanceReleases atomic.Uint64
}

// ConsumerOption configures one Kafka Consumer adapter.
type ConsumerOption interface {
	applyConsumer(*consumerOptions) error
}

type consumerOptionFunc func(*consumerOptions) error

func (function consumerOptionFunc) applyConsumer(options *consumerOptions) error {
	return function(options)
}

type consumerOptions struct {
	propagation    PropagationConfig
	propagationSet bool
}

// WithConsumerPropagation configures bounded metadata and trace extraction.
func WithConsumerPropagation(config PropagationConfig) ConsumerOption {
	return consumerOptionFunc(func(options *consumerOptions) error {
		if options.propagationSet {
			return fmt.Errorf("%w: duplicate consumer propagation", ErrInvalidOption)
		}
		options.propagation = config
		options.propagationSet = true
		return nil
	})
}

// NewConsumer creates an owned franz-go client with auto-commit disabled.
func NewConsumer(
	config ConsumerConfig,
	optionList ...ConsumerOption,
) (*Consumer, error) {
	if err := ValidateConsumerConfig(config); err != nil {
		return nil, err
	}
	settings, err := resolveConsumerOptions(optionList)
	if err != nil {
		return nil, err
	}
	return newOwnedConsumer(config, settings, nil, nil)
}

// NewConfiguredConsumer creates an owned consumer from strict runtime config
// and the App's instance-scoped propagator.
func NewConfiguredConsumer(
	config ConsumerRuntimeConfig,
	propagator propagation.TextMapPropagator,
	secrets SecretManager,
) (*Consumer, error) {
	if err := ValidateConsumerRuntimeConfig(config); err != nil {
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
	settings, err := normalizePropagation(propagationConfig)
	if err != nil {
		return nil, err
	}
	security, err := newClientSecurity(config.Security, secrets)
	if err != nil {
		return nil, err
	}
	return newOwnedConsumer(
		ConsumerConfig{
			Brokers:         append([]string(nil), config.Brokers...),
			Group:           config.Group,
			Topics:          append([]string(nil), config.Topics...),
			ClientID:        config.ClientID,
			DeadLetterTopic: config.DeadLetterTopic,
			ResetAtStart:    config.ResetAtStart,
		},
		settings,
		security.options,
		security,
	)
}

func newOwnedConsumer(
	config ConsumerConfig,
	settings propagationSettings,
	additionalOptions []kgo.Opt,
	security *clientSecurity,
) (*Consumer, error) {
	options := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.Group),
		kgo.ConsumeTopics(config.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
	backend := &franzBackend{
		retries: make(map[retryPartition]retryTimer),
	}
	options = append(
		options,
		kgo.OnPartitionsRevoked(func(
			_ context.Context,
			client *kgo.Client,
			partitions map[string][]int32,
		) {
			backend.clearRetries(client, partitions)
		}),
		kgo.OnPartitionsLost(func(
			_ context.Context,
			client *kgo.Client,
			partitions map[string][]int32,
		) {
			backend.clearRetries(client, partitions)
		}),
	)
	options = append(options, additionalOptions...)
	if clientid := strings.TrimSpace(config.ClientID); clientid != "" {
		options = append(options, kgo.ClientID(clientid))
	}
	if config.ResetAtStart {
		options = append(options, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	backend.client = client
	result, err := wrapConsumer(
		backend,
		config.DeadLetterTopic,
		true,
		settings,
	)
	if err != nil {
		client.Close()
		return nil, err
	}
	result.security = security
	return result, nil
}

// ValidateConsumerRuntimeConfig validates subscription identity, propagation
// budgets, and explicit transport security.
func ValidateConsumerRuntimeConfig(config ConsumerRuntimeConfig) error {
	if err := ValidateProducerRuntimeConfig(ProducerRuntimeConfig{
		Brokers:          config.Brokers,
		ClientID:         config.ClientID,
		TracePropagation: config.TracePropagation,
		MaxHeaders:       config.MaxHeaders,
		MaxBytes:         config.MaxBytes,
		Security:         config.Security,
	}); err != nil {
		return err
	}
	if !validConsumerIdentity(config.Group, 256) {
		return fmt.Errorf("%w: consumer group is invalid", ErrInvalidOption)
	}
	if len(config.Topics) == 0 || len(config.Topics) > 128 {
		return fmt.Errorf(
			"%w: topic count is outside 1..128",
			ErrInvalidOption,
		)
	}
	seenTopics := make(map[string]struct{}, len(config.Topics))
	for _, topic := range config.Topics {
		if !validKafkaTopic(topic) {
			return fmt.Errorf("%w: topic is invalid", ErrInvalidOption)
		}
		if _, duplicate := seenTopics[topic]; duplicate {
			return fmt.Errorf(
				"%w: topic %q is duplicated",
				ErrInvalidOption,
				topic,
			)
		}
		seenTopics[topic] = struct{}{}
	}
	if config.DeadLetterTopic != "" &&
		!validKafkaTopic(config.DeadLetterTopic) {
		return fmt.Errorf(
			"%w: dead-letter topic is invalid",
			ErrInvalidOption,
		)
	}
	if _, consumesDeadLetter := seenTopics[config.DeadLetterTopic]; config.DeadLetterTopic != "" && consumesDeadLetter {
		return fmt.Errorf(
			"%w: dead-letter topic must not be a source topic",
			ErrInvalidOption,
		)
	}
	return nil
}

// ValidateConsumerConfig validates stable Kafka subscription identity.
func ValidateConsumerConfig(config ConsumerConfig) error {
	if len(config.Brokers) == 0 ||
		strings.TrimSpace(config.Group) == "" ||
		len(config.Topics) == 0 {
		return fmt.Errorf(
			"%w: brokers, group, and topics are required",
			ErrInvalidOption,
		)
	}
	for _, broker := range config.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("%w: broker is empty", ErrInvalidOption)
		}
	}
	for _, topic := range config.Topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("%w: topic is empty", ErrInvalidOption)
		}
	}
	return nil
}

func validConsumerIdentity(value string, maxBytes int) bool {
	return value != "" &&
		len(value) <= maxBytes &&
		utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validKafkaTopic(value string) bool {
	if value == "." || value == ".." || len(value) == 0 || len(value) > 249 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '.' ||
			character == '_' ||
			character == '-' {
			continue
		}
		return false
	}
	return true
}

// WrapConsumer adapts a custom Backend.
func WrapConsumer(
	backend Backend,
	deadLetterTopic string,
	owns bool,
	optionList ...ConsumerOption,
) (*Consumer, error) {
	settings, err := resolveConsumerOptions(optionList)
	if err != nil {
		return nil, err
	}
	return wrapConsumer(backend, deadLetterTopic, owns, settings)
}

func wrapConsumer(
	backend Backend,
	deadLetterTopic string,
	owns bool,
	settings propagationSettings,
) (*Consumer, error) {
	if isNil(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	return &Consumer{
		backend:         backend,
		deadLetterTopic: strings.TrimSpace(deadLetterTopic),
		owns:            owns,
		propagation:     settings,
		done:            make(chan struct{}),
	}, nil
}

func resolveConsumerOptions(
	optionList []ConsumerOption,
) (propagationSettings, error) {
	options := consumerOptions{}
	for index, option := range optionList {
		if option == nil {
			return propagationSettings{}, fmt.Errorf(
				"%w: consumer option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.applyConsumer(&options); err != nil {
			return propagationSettings{}, fmt.Errorf(
				"%w: consumer option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	return normalizePropagation(options.propagation)
}

// Subscribe verifies the broker connection and starts the poll loop.
func (consumer *Consumer) Subscribe(
	ctx context.Context,
	handler worker.ConsumerHandler,
) error {
	if consumer == nil || isNil(consumer.backend) || handler == nil {
		return fmt.Errorf("%w: consumer or handler is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	consumer.mu.Lock()
	if consumer.started {
		consumer.mu.Unlock()
		return fmt.Errorf("%w: already subscribed", ErrInvalidOption)
	}
	consumer.mu.Unlock()
	if err := consumer.security.Start(ctx); err != nil {
		return fmt.Errorf("kafka: consumer security: %w", err)
	}
	if err := consumer.backend.Ping(ctx); err != nil {
		cleanupErr := consumer.security.Shutdown(context.WithoutCancel(ctx))
		return errors.Join(
			fmt.Errorf("kafka: ping: %w", err),
			cleanupErr,
		)
	}
	pollCtx, pollCancel := context.WithCancelCause(context.Background())
	deliveryCtx, deliveryCancel := context.WithCancelCause(context.Background())
	consumer.mu.Lock()
	consumer.handler = handler
	consumer.pollCancel = pollCancel
	consumer.deliveryCancel = deliveryCancel
	consumer.started = true
	consumer.mu.Unlock()
	go consumer.consume(pollCtx, deliveryCtx)
	return nil
}

// StopPulling interrupts Poll without closing the client.
func (consumer *Consumer) StopPulling(ctx context.Context) error {
	if consumer == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	consumer.mu.Lock()
	cancel := consumer.pollCancel
	started := consumer.started
	consumer.stopRequested = started
	consumer.mu.Unlock()
	if !started {
		return ErrNotRunning
	}
	if cancel != nil {
		cancel(context.Canceled)
	}
	return nil
}

// Drain waits for record handling and commit/DLQ completion.
func (consumer *Consumer) Drain(ctx context.Context) error {
	if consumer == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	select {
	case <-consumer.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Close cancels any remaining delivery and releases an owned franz-go client.
func (consumer *Consumer) Close(ctx context.Context) error {
	if consumer == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	consumer.mu.Lock()
	pollCancel := consumer.pollCancel
	deliveryCancel := consumer.deliveryCancel
	consumer.mu.Unlock()
	if pollCancel != nil {
		pollCancel(context.Canceled)
	}
	if deliveryCancel != nil {
		deliveryCancel(context.Canceled)
	}
	if !consumer.owns {
		return nil
	}
	consumer.closeOnce.Do(func() {
		consumer.backend.Close()
		consumer.closeErr = consumer.security.Shutdown(ctx)
	})
	return consumer.closeErr
}

// Wait reports poll, commit, DLQ, or handler disposition failure.
func (consumer *Consumer) Wait() error {
	if consumer == nil {
		return nil
	}
	consumer.mu.Lock()
	started := consumer.started
	consumer.mu.Unlock()
	if !started {
		return ErrNotRunning
	}
	<-consumer.done
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return consumer.runErr
}

// Description returns bounded lifecycle and disposition counters.
func (consumer *Consumer) Description() ConsumerDescription {
	if consumer == nil {
		return ConsumerDescription{}
	}
	consumer.mu.Lock()
	description := ConsumerDescription{
		Running:       consumer.started && !consumer.finished,
		StopRequested: consumer.stopRequested,
		InFlight:      consumer.inflight,
		Finished:      consumer.finished,
		Failed:        consumer.runErr != nil,
	}
	consumer.mu.Unlock()
	description.Delivered = consumer.delivered.Load()
	description.Acknowledged = consumer.acknowledged.Load()
	description.Retried = consumer.retried.Load()
	description.DeadLettered = consumer.deadLettered.Load()
	description.NegativelyAcked = consumer.negativelyAcked.Load()
	description.RebalanceReleases = consumer.rebalanceReleases.Load()
	return description
}

func (consumer *Consumer) consume(
	pollCtx context.Context,
	deliveryCtx context.Context,
) {
	defer func() {
		consumer.mu.Lock()
		consumer.inflight = false
		consumer.finished = true
		consumer.mu.Unlock()
		close(consumer.done)
	}()
	for {
		record, err := consumer.backend.Poll(pollCtx)
		if err != nil {
			if context.Cause(pollCtx) != nil {
				consumer.allowRebalance()
				return
			}
			consumer.setRunError(fmt.Errorf("kafka: poll: %w", err))
			return
		}
		consumer.mu.Lock()
		consumer.inflight = true
		consumer.mu.Unlock()
		consumer.delivered.Add(1)
		err = consumer.handle(deliveryCtx, record)
		consumer.mu.Lock()
		consumer.inflight = false
		consumer.mu.Unlock()
		consumer.allowRebalance()
		if err != nil {
			consumer.setRunError(err)
			return
		}
	}
}

func (consumer *Consumer) handle(
	ctx context.Context,
	record Record,
) error {
	ctx, inbound, err := extractMessageContext(
		ctx,
		record.Headers,
		consumer.propagation,
	)
	if err != nil {
		return err
	}
	message := worker.NewMessage(
		recordid(record),
		record.Value,
		inbound,
	)
	consumer.mu.Lock()
	handler := consumer.handler
	consumer.mu.Unlock()
	result := handler(ctx, message)
	switch result.Action() {
	case worker.ActionAck:
		if err := consumer.backend.Commit(ctx, record); err != nil {
			return err
		}
		consumer.acknowledged.Add(1)
		return nil
	case worker.ActionRetry:
		consumer.retried.Add(1)
		consumer.backend.Retry(record, result.RetryAfter())
		return nil
	case worker.ActionDeadLetter:
		if consumer.deadLetterTopic == "" {
			return errors.Join(ErrNoDeadLetterTopic, result.Cause())
		}
		if err := consumer.backend.DeadLetter(
			ctx,
			consumer.deadLetterTopic,
			record,
			result.Cause(),
		); err != nil {
			return fmt.Errorf("kafka: dead letter: %w", err)
		}
		if err := consumer.backend.Commit(ctx, record); err != nil {
			return err
		}
		consumer.deadLettered.Add(1)
		return nil
	case worker.ActionNack:
		consumer.negativelyAcked.Add(1)
		return result.Cause()
	default:
		consumer.negativelyAcked.Add(1)
		return worker.ErrInvalidResult
	}
}

func (consumer *Consumer) allowRebalance() {
	consumer.backend.AllowRebalance()
	consumer.rebalanceReleases.Add(1)
}

func (consumer *Consumer) setRunError(err error) {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	consumer.runErr = err
}

func recordid(record Record) string {
	return record.Topic + "/" +
		strconv.FormatInt(int64(record.Partition), 10) + "/" +
		strconv.FormatInt(record.Offset, 10)
}

type franzBackend struct {
	client *kgo.Client

	retryMu         sync.Mutex
	retryGeneration uint64
	retries         map[retryPartition]retryTimer
	closed          bool
}

type retryPartition struct {
	topic     string
	partition int32
}

type retryTimer struct {
	generation uint64
	timer      *time.Timer
}

func (backend *franzBackend) Ping(ctx context.Context) error {
	return backend.client.Ping(ctx)
}

func (backend *franzBackend) Poll(ctx context.Context) (Record, error) {
	fetches := backend.client.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		return Record{}, err
	}
	records := fetches.Records()
	if len(records) == 0 {
		return Record{}, errors.New("kafka: poll returned no records")
	}
	record := records[0]
	headers := make([]Header, len(record.Headers))
	for index, header := range record.Headers {
		headers[index] = Header{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}
	return Record{
		Topic:       record.Topic,
		Partition:   record.Partition,
		Offset:      record.Offset,
		LeaderEpoch: record.LeaderEpoch,
		Timestamp:   record.Timestamp,
		Key:         append([]byte(nil), record.Key...),
		Value:       append([]byte(nil), record.Value...),
		Headers:     headers,
	}, nil
}

func (backend *franzBackend) Commit(
	ctx context.Context,
	record Record,
) error {
	return backend.client.CommitRecords(ctx, toKgoRecord(record))
}

func (backend *franzBackend) Retry(record Record, delay time.Duration) {
	backend.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		record.Topic: {
			record.Partition: {
				Epoch:  record.LeaderEpoch,
				Offset: record.Offset,
			},
		},
	})
	partition := retryPartition{
		topic:     record.Topic,
		partition: record.Partition,
	}
	topicPartitions := map[string][]int32{
		record.Topic: {record.Partition},
	}
	backend.retryMu.Lock()
	if backend.closed {
		backend.retryMu.Unlock()
		return
	}
	if previous, exists := backend.retries[partition]; exists {
		previous.timer.Stop()
		delete(backend.retries, partition)
		backend.client.ResumeFetchPartitions(topicPartitions)
	}
	if delay <= 0 {
		backend.retryMu.Unlock()
		return
	}
	backend.client.PauseFetchPartitions(topicPartitions)
	backend.retryGeneration++
	generation := backend.retryGeneration
	timer := time.AfterFunc(delay, func() {
		backend.retryMu.Lock()
		defer backend.retryMu.Unlock()
		current, exists := backend.retries[partition]
		if backend.closed ||
			!exists ||
			current.generation != generation {
			return
		}
		delete(backend.retries, partition)
		backend.client.ResumeFetchPartitions(topicPartitions)
	})
	backend.retries[partition] = retryTimer{
		generation: generation,
		timer:      timer,
	}
	backend.retryMu.Unlock()
}

func (backend *franzBackend) clearRetries(
	client *kgo.Client,
	partitions map[string][]int32,
) {
	if client == nil || len(partitions) == 0 {
		return
	}
	resume := make(map[string][]int32)
	backend.retryMu.Lock()
	for topic, topicPartitions := range partitions {
		for _, partition := range topicPartitions {
			key := retryPartition{topic: topic, partition: partition}
			retry, exists := backend.retries[key]
			if !exists {
				continue
			}
			retry.timer.Stop()
			delete(backend.retries, key)
			resume[topic] = append(resume[topic], partition)
		}
	}
	backend.retryMu.Unlock()
	if len(resume) > 0 {
		client.ResumeFetchPartitions(resume)
	}
}

func (backend *franzBackend) DeadLetter(
	ctx context.Context,
	topic string,
	record Record,
	cause error,
) error {
	headers := []kgo.RecordHeader{
		{Key: "keelith-source-topic", Value: []byte(record.Topic)},
		{Key: "keelith-source-partition", Value: []byte(strconv.Itoa(int(record.Partition)))},
		{Key: "keelith-source-offset", Value: []byte(strconv.FormatInt(record.Offset, 10))},
	}
	if cause != nil {
		headers = append(headers, kgo.RecordHeader{
			Key:   "keelith-failure",
			Value: []byte(cause.Error()),
		})
	}
	result := backend.client.ProduceSync(ctx, &kgo.Record{
		Topic:   topic,
		Key:     append([]byte(nil), record.Key...),
		Value:   append([]byte(nil), record.Value...),
		Headers: headers,
	})
	return result.FirstErr()
}

func (backend *franzBackend) AllowRebalance() {
	backend.client.AllowRebalance()
}

func (backend *franzBackend) Close() {
	backend.retryMu.Lock()
	backend.closed = true
	for partition, retry := range backend.retries {
		retry.timer.Stop()
		delete(backend.retries, partition)
	}
	backend.retryMu.Unlock()
	backend.client.Close()
}

func toKgoRecord(record Record) *kgo.Record {
	return &kgo.Record{
		Topic:       record.Topic,
		Partition:   record.Partition,
		Offset:      record.Offset,
		LeaderEpoch: record.LeaderEpoch,
		Timestamp:   record.Timestamp,
		Key:         append([]byte(nil), record.Key...),
		Value:       append([]byte(nil), record.Value...),
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
