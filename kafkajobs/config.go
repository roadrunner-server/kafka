package kafkajobs

import (
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafka configuration options
// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

const (
	// general options
	topicKey             string = "topic"
	priorityKey          string = "priority"
	groupIDKey           string = "group_id"
	partitionsOffsetsKey string = "partitions_offsets"
	maxOpenRequestsKey   string = "max_open_requests"
	clientIDKey          string = "client_id"
	versionKey           string = "version"

	// consumer opts
	sessionTimeoutKey      string = "session_timeout"
	heartbeatIntervalKey   string = "heartbeat_interval"
	maxFetchMessageSizeKey string = "max_fetch_message_size"
	minFetchMessageSizeKey string = "min_fetch_message_size"

	// create topic opts
	replicationFactorKey string = "replication_factor"
	replicaAssignmentKey string = "replica_assignment"
	configEntriesKey     string = "config_entries"

	// producer opts
	maxMessageSizeKey   string = "max_message_bytes"
	requiredAcksKey     string = "required_acks"
	timeoutKey          string = "timeout"
	compressionCodecKey string = "compression_codec"
	compressionLevelKey string = "compression_level"
	idempotentKey       string = "idempotent"
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
	Brokers []string `mapstructure:"brokers"`

	// kafka local
	Priority int `mapstructure:"priority"`

	///// OLD -----------
	GroupID         string `mapstructure:"group_id"`
	MaxOpenRequests int    `mapstructure:"max_open_requests"`

	ClientID string `mapstructure:"client_id"`
	// in the semantic versioning form (e.g 2.11.0)
	KafkaVersion string `mapstructure:"kafka_version"`

	// consumer, producer and topics options
	CreateTopic  *CreateTopics `mapstructure:"create_topics"`
	ProducerOpts *ProducerOpts `mapstructure:"producer_options"`
	ConsumerOpts *ConsumerOpts `mapstructure:"consumer_options"`

	// private, combinations of partitions per-topic
	topicPartitions map[string][]int32
}

type CreateTopics struct {
	ReplicationFactor int16              `mapstructure:"replication_factor"`
	ReplicaAssignment map[int32][]int32  `mapstructure:"replica_assignment"`
	ConfigEntries     map[string]*string `mapstructure:"config_entries"`
}

type ProducerOpts struct {
	DisableIdempotent  bool             `mapstructure:"disable_idempotent"`
	RequiredAcks       string           `mapstructure:"required_acks"`
	MaxMessageBytes    int32            `mapstructure:"max_message_bytes"`
	RequestTimeout     time.Duration    `mapstructure:"request_timeout"`
	DeliveryTimeout    time.Duration    `mapstructure:"delivery_timeout"`
	TransactionTimeout time.Duration    `mapstructure:"transaction_timeout"`
	CompressionCodec   CompressionCodec `mapstructure:"compression_codec"`
}

type ConsumerOpts struct {
	// kafka
	Topics              []string                    `mapstructure:"topics"`
	ConsumeRegexp       bool                        `mapstructure:"consume_regexp"`
	ConsumePartitions   map[string]map[int32]string `mapstructure:"consume_partitions"`
	MaxFetchMessageSize int32                       `mapstructure:"max_fetch_message_size"`
	MinFetchMessageSize int32                       `mapstructure:"min_fetch_message_size"`
	ConsumeOffset       map[int32]int64             `mapstructure:"consume_offset"`
	SessionTimeout      int                         `mapstructure:"session_timeout"`
	HeartbeatInterval   int                         `mapstructure:"heartbeat_interval"`
}

func (c *config) InitDefault() ([]kgo.Opt, error) {
	opts := make([]kgo.Opt, 0, 1)

	//kgo.SeedBrokers(seeds...),
	//kgo.ConsumerGroup("my-group-identifier"),
	//kgo.ConsumeTopics("foo"),
	//kgo.ConsumerGroup(""),

	if c.Priority == 0 {
		c.Priority = 10
	}

	//if len(c.ConsumePartitions) > 0 {
	//	kp := make(map[string]map[int32]kgo.Offset)
	//	kgo.ConsumePartitions(kp)
	//}

	if c.ProducerOpts != nil {
		if c.ProducerOpts.DisableIdempotent {
			opts = append(opts, kgo.DisableIdempotentWrite())
		}

		if c.ProducerOpts.RequiredAcks != "" {
			opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
		}

		if c.ProducerOpts.MaxMessageBytes != 0 {
			opts = append(opts, kgo.ProducerBatchMaxBytes(c.ProducerOpts.MaxMessageBytes))
		}

		if c.ProducerOpts.RequestTimeout != 0 {
			opts = append(opts, kgo.ProduceRequestTimeout(c.ProducerOpts.RequestTimeout))
		}

		if c.ProducerOpts.DeliveryTimeout != 0 {
			opts = append(opts, kgo.RecordDeliveryTimeout(c.ProducerOpts.DeliveryTimeout))
		}

		if c.ProducerOpts.TransactionTimeout != 0 {
			opts = append(opts, kgo.TransactionTimeout(c.ProducerOpts.TransactionTimeout))
		}

		if c.ProducerOpts.CompressionCodec != "" {
			switch c.ProducerOpts.CompressionCodec {
			case gzip:
				opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
			case lz4:
				opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
			case zstd:
				opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
			case snappy:
				opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
			}
		}
	}

	if c.ConsumerOpts != nil {
		if len(c.ConsumerOpts.Topics) > 0 {
			opts = append(opts, kgo.ConsumeTopics(c.ConsumerOpts.Topics...))
		} else {
			return nil, errors.Str("topics should not be empty for the consumer")
		}

		if c.ConsumerOpts.ConsumeRegexp {
			opts = append(opts, kgo.ConsumeRegex())
		}

		// default - 0, OffsetNewest
		//if len(c.ConsumeOffset) != 0 {
		//	opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		//}
		//
		//if c.ConsumerOpts.HeartbeatInterval != 0 {
		//	c.kafkaConfig.Consumer.Group.Heartbeat.Interval = time.Second * time.Duration(c.ConsumerOpts.HeartbeatInterval)
		//}
		//
		//if c.ConsumerOpts.SessionTimeout != 0 {
		//	c.kafkaConfig.Consumer.Group.Session.Timeout = time.Second * time.Duration(c.ConsumerOpts.SessionTimeout)
		//}
		//
		//if c.ConsumerOpts.MaxFetchMessageSize != 0 {
		//	c.kafkaConfig.Consumer.Fetch.Default = c.ConsumerOpts.MaxFetchMessageSize
		//}
		//if c.ConsumerOpts.MinFetchMessageSize != 0 {
		//	c.kafkaConfig.Consumer.Fetch.Min = c.ConsumerOpts.MinFetchMessageSize
		//}
	}

	//if c.MaxOpenRequests != 0 {
	//	c.kafkaConfig.Net.MaxOpenRequests = c.MaxOpenRequests
	//}
	//
	//if c.ClientID == "" {
	//	c.kafkaConfig.ClientID = "roadrunner"
	//}
	//
	//c.kafkaConfig.Producer.Return.Successes = true
	//c.kafkaConfig.Producer.Return.Errors = true
	//c.kafkaConfig.Version = parseVersion(c.KafkaVersion)
	//// 1 minute in total
	//c.kafkaConfig.Metadata.Retry.Max = 10
	//c.kafkaConfig.Metadata.Retry.Backoff = time.Second * 6
	//
	//

	return opts, nil
}

/*
	groupIDKey           string = "group_id"
	partitionsOffsetsKey string = "partitions_offsets"
	maxOpenRequestsKey   string = "max_open_requests"

	// create topic opts
	replicationFactoryKey string = "replication_factory"
	replicaAssignmentKey  string = "replica_assignment"
	configEntriesKey      string = "config_entries"

	// producer opts
	maxMessageSizeKey   string = "max_message_bytes"
	requiredAcksKey     string = "required_acks"
	timeoutKey          string = "timeout"
	compressionCodecKey string = "compression_codec"
	compressionLevelKey string = "compression_level"
	idempotentKey       string = "idempotent"
*/

