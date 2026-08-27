package kafka

import keelithconfig "github.com/keelab/keelith/config"

// NewConsumerConfigBinding creates a strict typed Kafka consumer binding.
//
// Consumer group, topics, brokers, reset position, and DLQ destination are
// lifecycle identity and therefore require a consumer restart when changed.
func NewConsumerConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[ConsumerConfig],
) (*keelithconfig.Component[ConsumerConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[ConsumerConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateConsumerConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[ConsumerConfig](name, path, all...)
}

// NewConsumerRuntimeConfigBinding creates a strict generated-project Kafka
// consumer binding. All fields are subscription or connection identity and
// therefore require a consumer restart when changed.
func NewConsumerRuntimeConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[ConsumerRuntimeConfig],
) (*keelithconfig.Component[ConsumerRuntimeConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[ConsumerRuntimeConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(
			ValidateConsumerRuntimeConfig,
		),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[ConsumerRuntimeConfig](name, path, all...)
}

// NewProducerConfigBinding creates a strict typed Kafka producer binding.
// Broker topology, client identity, and propagation settings require restart.
func NewProducerConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[ProducerRuntimeConfig],
) (*keelithconfig.Component[ProducerRuntimeConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[ProducerRuntimeConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateProducerRuntimeConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[ProducerRuntimeConfig](name, path, all...)
}
