package kafkajobs

import (
	"github.com/Shopify/sarama"
	"github.com/roadrunner-server/errors"
)

const (
	topics string = "topic"
	pri    string = "priority"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Addresses []string `mapstructure:"addrs"`

	// kafka local
	Priority           int                   `mapstructure:"priority"`
	Topic              string                `mapstructure:"topic"`
	PartitionsOffsets  map[int32]interface{} `mapstructure:"partitions_offsets"`
	ReplicationFactory int16                 `mapstructure:"replication_factory"`
	GroupID            string                `mapstructure:"group_id, omitempty"`

	MaxOpenRequests int       `mapstructure:"max_open_requests"`
	Producer        *Producer `mapstructure:"producer"`

	// private, combinations of partitions per-topic
	topicPartitions map[string][]int32
}

type Producer struct {
	MaxMessageBytes  int                 `mapstructure:"max_message_bytes"`
	RequiredAcks     sarama.RequiredAcks `mapstructure:"required_acks"`
	Timeout          int                 `mapstructure:"timeout"`
	CompressionCodec string              `mapstructure:"compression_codec"`
	CompressionLevel int                 `mapstructure:"compression_level"`
	Idempotent       bool                `mapstructure:"idempotent"`
}

func (c *config) InitDefault() error {
	if c.Topic == "" {
		return errors.Str("topic should not be empty")
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.ReplicationFactory == 0 {
		c.ReplicationFactory = 2
	}

	// default - 0, OffsetNewest
	if len(c.PartitionsOffsets) == 0 {
		c.PartitionsOffsets = make(map[int32]interface{})
		c.PartitionsOffsets[0] = int(sarama.OffsetNewest)
	}

	// merge a topic partitions
	c.topicPartitions = make(map[string][]int32, len(c.Topic))
	for k, _ := range c.PartitionsOffsets {
		c.topicPartitions[c.Topic] = append(c.topicPartitions[c.Topic], k)
	}

	return nil
}
