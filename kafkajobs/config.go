package kafkajobs

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// pipeline rabbitmq info
const ()

// config is used to parse pipeline configuration
type config struct {
	// global
	Addr     string `mapstructure:"addr"`
	Prefetch int    `mapstructure:"prefetch"`
	Priority int    `mapstructure:"priority"`

	// kafka local
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	KafkaConfigMap *kafka.ConfigMap
}

func (c *config) InitDefault() {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:9092"
	}

	if c.Prefetch == 0 {
		c.Prefetch = 10
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.KafkaConfigMap == nil {
		c.KafkaConfigMap = &kafka.ConfigMap{}
		_ = c.KafkaConfigMap.SetKey("bootstrap.servers", "127.0.0.1:9092")
		_ = c.KafkaConfigMap.SetKey("group.id", "default")
		_ = c.KafkaConfigMap.SetKey("auto.offset.reset", "earliest")
	} else {
		// set addr from the global config values
		_ = c.KafkaConfigMap.SetKey("bootstrap.servers", c.Addr)
	}
}
