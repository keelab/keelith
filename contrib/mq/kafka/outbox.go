package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/keelab/keelith/outbox"
)

var _ outbox.Publisher = (*OutboxPublisher)(nil)

// OutboxPublisher adapts Producer to the broker-neutral outbox Publisher.
type OutboxPublisher struct {
	producer *Producer
}

// NewOutboxPublisher constructs a Kafka outbox publisher.
func NewOutboxPublisher(producer *Producer) (*OutboxPublisher, error) {
	if producer == nil || producer.client == nil {
		return nil, fmt.Errorf("%w: producer is nil", ErrInvalidOption)
	}
	return &OutboxPublisher{producer: producer}, nil
}

// Publish maps destination, key, payload, and bounded headers to Kafka.
func (publisher *OutboxPublisher) Publish(
	ctx context.Context,
	message outbox.Message,
) error {
	if publisher == nil || publisher.producer == nil {
		return fmt.Errorf("%w: outbox publisher is nil", ErrInvalidOption)
	}
	if err := message.Validate(); err != nil {
		return err
	}
	keys := make([]string, 0, len(message.Headers))
	for key := range message.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	headers := make([]Header, 0, len(keys)+1)
	headers = append(headers, Header{
		Key:   "keelith-outbox-id",
		Value: []byte(message.ID),
	})
	for _, key := range keys {
		headers = append(headers, Header{
			Key:   key,
			Value: append([]byte(nil), message.Headers[key]...),
		})
	}
	return publisher.producer.Publish(
		ctx,
		message.Destination,
		message.Key,
		message.Payload,
		headers...,
	)
}
