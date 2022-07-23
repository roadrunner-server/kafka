package kafkajobs

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/roadrunner-server/errors"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Prefetch int      `mapstructure:"prefetch"`
	Priority int      `mapstructure:"priority"`
	Topics   []string `mapstructure:"topics"`

	// kafka local
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	// https://docs.confluent.io/kafka-clients/go/current/overview.html
	KafkaConsumerConfigMap *kafka.ConfigMap
	KafkaProducerConfigMap *kafka.ConfigMap
}

func (c *config) InitDefault() error {
	if c.Prefetch == 0 {
		c.Prefetch = 10
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if len(c.Topics) == 0 {
		return errors.Str("at least 1 topic should be set per pipeline")
	}

	if c.KafkaConsumerConfigMap == nil {
		c.KafkaConsumerConfigMap = &kafka.ConfigMap{}
		_ = c.KafkaConsumerConfigMap.SetKey("bootstrap.servers", "127.0.0.1:9092")
		_ = c.KafkaConsumerConfigMap.SetKey("group.id", "default")
		_ = c.KafkaConsumerConfigMap.SetKey("auto.offset.reset", "earliest")
	}

	// delete RR driver keys
	delete(*c.KafkaConsumerConfigMap, "priority")
	delete(*c.KafkaConsumerConfigMap, "prefetch")
	delete(*c.KafkaConsumerConfigMap, "topics")

	newMapConsumer := &kafka.ConfigMap{}

	for k, v := range *c.KafkaConsumerConfigMap {
		kk := strings.ReplaceAll(k, ":", ".")
		_ = newMapConsumer.SetKey(kk, v)
	}

	c.KafkaConsumerConfigMap = newMapConsumer

	if c.KafkaProducerConfigMap == nil {
		c.KafkaProducerConfigMap = &kafka.ConfigMap{}
		_ = c.KafkaProducerConfigMap.SetKey("bootstrap.servers", "127.0.0.1:9092")
	}

	// delete RR driver keys
	delete(*c.KafkaProducerConfigMap, "priority")
	delete(*c.KafkaProducerConfigMap, "prefetch")
	delete(*c.KafkaProducerConfigMap, "topics")

	newMapProducer := &kafka.ConfigMap{}

	for k, v := range *c.KafkaProducerConfigMap {
		kk := strings.ReplaceAll(k, ":", ".")
		_ = newMapProducer.SetKey(kk, v)
	}

	c.KafkaProducerConfigMap = newMapProducer
	return nil
}
