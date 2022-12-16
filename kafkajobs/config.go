package kafkajobs

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/roadrunner-server/errors"
)

const (
	topics              string = "topics"
	numOfPartitions     string = "number_of_partitions"
	pri                 string = "priority"
	createTopicsOnStart string = "create_topics_on_start"
	topicsConfig        string = "topics_config"
	consumerConfig      string = "consumer_config"
	producerConfig      string = "producer_config"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Priority            int               `mapstructure:"priority"`
	Topics              []string          `mapstructure:"topics"`
	TopicsConfig        map[string]string `mapstructure:"topics_config"`
	CreateTopicsOnStart bool              `mapstructure:"create_topics_on_start"`
	NumberOfPartitions  int               `mapstructure:"number_of_partitions"`

	// kafka local
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	// https://docs.confluent.io/kafka-clients/go/current/overview.html
	KafkaConsumerConfigMap *kafka.ConfigMap
	KafkaProducerConfigMap *kafka.ConfigMap
}

func (c *config) InitDefault() error {
	if len(c.Topics) == 0 {
		return errors.Str("at least 1 topic should be set per pipeline")
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.CreateTopicsOnStart && c.NumberOfPartitions == 0 {
		c.NumberOfPartitions = 1
	}

	if c.KafkaConsumerConfigMap == nil {
		// init default config
		c.KafkaConsumerConfigMap = &kafka.ConfigMap{}
		_ = c.KafkaConsumerConfigMap.SetKey("bootstrap.servers", "127.0.0.1:9092")
		_ = c.KafkaConsumerConfigMap.SetKey("group.id", "default")
		_ = c.KafkaConsumerConfigMap.SetKey("auto.offset.reset", "earliest")
		_ = c.KafkaConsumerConfigMap.SetKey("allow.auto.create.topics", true)
	}

	// delete RR driver keys
	delete(*c.KafkaConsumerConfigMap, "priority")
	delete(*c.KafkaConsumerConfigMap, "prefetch")
	delete(*c.KafkaConsumerConfigMap, "topics")

	for k, v := range *c.KafkaConsumerConfigMap {
		kk := strings.ReplaceAll(k, ":", ".")
		delete(*c.KafkaConsumerConfigMap, k)
		_ = c.KafkaConsumerConfigMap.SetKey(kk, v)
	}

	if c.KafkaProducerConfigMap == nil {
		// init default config
		c.KafkaProducerConfigMap = &kafka.ConfigMap{}
		_ = c.KafkaProducerConfigMap.SetKey("bootstrap.servers", "127.0.0.1:9092")
	}

	// delete RR driver keys
	delete(*c.KafkaProducerConfigMap, "priority")
	delete(*c.KafkaProducerConfigMap, "prefetch")
	delete(*c.KafkaProducerConfigMap, "topics")

	for k, v := range *c.KafkaProducerConfigMap {
		kk := strings.ReplaceAll(k, ":", ".")
		delete(*c.KafkaProducerConfigMap, k)
		_ = c.KafkaProducerConfigMap.SetKey(kk, v)
	}

	// replace : for the topics config
	if len(c.TopicsConfig) != 0 {
		for k, v := range c.TopicsConfig {
			kk := strings.ReplaceAll(k, ":", ".")
			delete(c.TopicsConfig, k)
			c.TopicsConfig[kk] = v
		}
	}

	return nil
}
