package kafkajobs

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/roadrunner-server/errors"
)

const (
	topicKey    string = "topic"
	priorityKey string = "priority"
)

type CompressionCodec string

const (
	none   CompressionCodec = "none"
	gzip   CompressionCodec = "gzip"
	snappy CompressionCodec = "snappy"
	lz4    CompressionCodec = "lz4"
	zstd   CompressionCodec = "zstd"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Addresses []string `mapstructure:"addrs"`

	// kafka local
	Priority          int                   `mapstructure:"priority"`
	Topic             string                `mapstructure:"topic"`
	CreateTopic       *CreateTopics         `mapstructure:"create_topics"`
	PartitionsOffsets map[int32]interface{} `mapstructure:"partitions_offsets"`
	GroupID           string                `mapstructure:"group_id, omitempty"`
	MaxOpenRequests   int                   `mapstructure:"max_open_requests"`

	ClientID string `mapstructure:"client_id"`
	// in the semantic versioning form (e.g 2.11.0)
	KafkaVersion string `mapstructure:"kafka_version"`

	// consumer, producer and topics options
	ProducerOpts  *ProducerOpts      `mapstructure:"producer_options"`
	ConsumerOpts  *ConsumerOpts      `mapstructure:"consumer_options"`
	TopicsOptions map[string]*string `mapstructure:"topic_options"`

	// private, combinations of partitions per-topic
	topicPartitions map[string][]int32
	// kafka config
	kafkaConfig *sarama.Config
}

type CreateTopics struct {
	ReplicationFactory int16              `mapstructure:"replication_factory"`
	ReplicaAssignment  map[int32][]int32  `mapstructure:"replica_assignment"`
	ConfigEntries      map[string]*string `mapstructure:"config_entries"`
}

type ProducerOpts struct {
	MaxMessageBytes  int              `mapstructure:"max_message_bytes"`
	RequiredAcks     *int             `mapstructure:"required_acks"`
	Timeout          int              `mapstructure:"timeout"`
	CompressionCodec CompressionCodec `mapstructure:"compression_codec"`
	CompressionLevel *int             `mapstructure:"compression_level"`
	Idempotent       bool             `mapstructure:"idempotent"`
}

type ConsumerOpts struct {
	SessionTimeout    int `mapstructure:"session_timeout"`
	HeartbeatInterval int `mapstructure:"heartbeat_interval"`
}

func (c *config) InitDefault() error {
	if c.Topic == "" {
		return errors.Str("topic should not be empty")
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.CreateTopic != nil {
		if c.CreateTopic.ReplicationFactory == 0 {
			c.CreateTopic.ReplicationFactory = 1
		}
	}

	// default - 0, OffsetNewest
	if len(c.PartitionsOffsets) == 0 {
		c.PartitionsOffsets = make(map[int32]interface{})
		c.PartitionsOffsets[0] = int(sarama.OffsetNewest)
	}

	// merge a topic partitions
	c.topicPartitions = make(map[string][]int32, len(c.Topic))
	for k := range c.PartitionsOffsets {
		c.topicPartitions[c.Topic] = append(c.topicPartitions[c.Topic], k)
	}

	c.kafkaConfig = sarama.NewConfig()

	if c.ProducerOpts != nil {
		c.kafkaConfig.Producer.Idempotent = c.ProducerOpts.Idempotent
		if c.ProducerOpts.RequiredAcks == nil {
			c.ProducerOpts.RequiredAcks = ptrTo(1)
		}
		c.kafkaConfig.Producer.RequiredAcks = sarama.RequiredAcks(*c.ProducerOpts.RequiredAcks)

		if c.ProducerOpts.MaxMessageBytes != 0 {
			c.kafkaConfig.Producer.MaxMessageBytes = c.ProducerOpts.MaxMessageBytes
		}

		if c.ProducerOpts.Timeout != 0 {
			c.kafkaConfig.Producer.Timeout = time.Second * time.Duration(c.ProducerOpts.Timeout)
		}

		if c.ProducerOpts.CompressionCodec != "" {
			switch c.ProducerOpts.CompressionCodec {
			case gzip:
				c.kafkaConfig.Producer.Compression = sarama.CompressionGZIP
			case lz4:
				c.kafkaConfig.Producer.Compression = sarama.CompressionLZ4
			case zstd:
				c.kafkaConfig.Producer.Compression = sarama.CompressionZSTD
			case snappy:
				c.kafkaConfig.Producer.Compression = sarama.CompressionSnappy
			case none:
				c.kafkaConfig.Producer.Compression = sarama.CompressionNone
			default:
				c.kafkaConfig.Producer.Compression = sarama.CompressionNone
			}
		}

		if c.ProducerOpts.CompressionLevel != nil {
			c.kafkaConfig.Producer.CompressionLevel = *c.ProducerOpts.CompressionLevel
		}
	}

	if c.ConsumerOpts != nil {
		if c.ConsumerOpts.HeartbeatInterval != 0 {
			c.kafkaConfig.Consumer.Group.Heartbeat.Interval = time.Second * time.Duration(c.ConsumerOpts.HeartbeatInterval)
		}

		if c.ConsumerOpts.SessionTimeout != 0 {
			c.kafkaConfig.Consumer.Group.Session.Timeout = time.Second * time.Duration(c.ConsumerOpts.SessionTimeout)
		}
	}

	if c.MaxOpenRequests != 0 {
		c.kafkaConfig.Net.MaxOpenRequests = c.MaxOpenRequests
	}

	if c.ClientID == "" {
		c.kafkaConfig.ClientID = "roadrunner"
	}

	c.kafkaConfig.Producer.Return.Successes = true
	c.kafkaConfig.Producer.Return.Errors = true

	return c.kafkaConfig.Validate()
}

func ptrTo[T any](val T) *T {
	return &val
}
