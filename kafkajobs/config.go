package kafkajobs

import (
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v2/plugins/jobs/pipeline"
	"github.com/roadrunner-server/errors"
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
	Addresses []string `mapstructure:"addrs"`

	// kafka local
	Priority          int             `mapstructure:"priority"`
	Topic             string          `mapstructure:"topic"`
	PartitionsOffsets map[int32]int64 `mapstructure:"partitions_offsets"`
	GroupID           string          `mapstructure:"group_id"`
	MaxOpenRequests   int             `mapstructure:"max_open_requests"`

	ClientID string `mapstructure:"client_id"`
	// in the semantic versioning form (e.g 2.11.0)
	KafkaVersion string `mapstructure:"kafka_version"`

	// consumer, producer and topics options
	CreateTopic  *CreateTopics `mapstructure:"create_topics"`
	ProducerOpts *ProducerOpts `mapstructure:"producer_options"`
	ConsumerOpts *ConsumerOpts `mapstructure:"consumer_options"`

	// private, combinations of partitions per-topic
	topicPartitions map[string][]int32
	// kafka config
	kafkaConfig *sarama.Config
}

type CreateTopics struct {
	ReplicationFactor int16              `mapstructure:"replication_factor"`
	ReplicaAssignment map[int32][]int32  `mapstructure:"replica_assignment"`
	ConfigEntries     map[string]*string `mapstructure:"config_entries"`
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
	MaxFetchMessageSize int32 `mapstructure:"max_fetch_message_size"`
	MinFetchMessageSize int32 `mapstructure:"min_fetch_message_size"`
	SessionTimeout      int   `mapstructure:"session_timeout"`
	HeartbeatInterval   int   `mapstructure:"heartbeat_interval"`
}

func (c *config) InitDefault() error {
	if c.Topic == "" {
		return errors.Str("topic should not be empty")
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.CreateTopic != nil {
		if c.CreateTopic.ReplicationFactor == 0 {
			c.CreateTopic.ReplicationFactor = 1
		}

		for k, v := range c.CreateTopic.ConfigEntries {
			delete(c.CreateTopic.ConfigEntries, k)
			c.CreateTopic.ConfigEntries[strings.ReplaceAll(k, ":", ".")] = v
		}
	}

	// default - 0, OffsetNewest
	if len(c.PartitionsOffsets) == 0 {
		c.PartitionsOffsets = make(map[int32]int64)
		c.PartitionsOffsets[0] = sarama.OffsetNewest
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

		if c.ConsumerOpts.MaxFetchMessageSize != 0 {
			c.kafkaConfig.Consumer.Fetch.Default = c.ConsumerOpts.MaxFetchMessageSize
		}
		if c.ConsumerOpts.MinFetchMessageSize != 0 {
			c.kafkaConfig.Consumer.Fetch.Min = c.ConsumerOpts.MinFetchMessageSize
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
	c.kafkaConfig.Version = parseVersion(c.KafkaVersion)
	// 1 minute in total
	c.kafkaConfig.Metadata.Retry.Max = 10
	c.kafkaConfig.Metadata.Retry.Backoff = time.Second * 6

	return c.kafkaConfig.Validate()
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

func parseConfig(conf *config, pipe *pipeline.Pipeline) (*sarama.Config, error) { //nolint:gocognit,gocyclo
	// init
	sc := sarama.NewConfig()

	conf.Topic = pipe.String(topicKey, "")
	if conf.Topic == "" {
		return nil, errors.Str("topic should not be empty")
	}

	conf.Priority = pipe.Int(priorityKey, 10)

	// allocate
	conf.PartitionsOffsets = make(map[int32]int64)
	conf.topicPartitions = make(map[string][]int32, len(conf.Topic))

	// group id config
	conf.GroupID = pipe.String(groupIDKey, "")

	if val := pipe.Int(maxOpenRequestsKey, 0); val != 0 {
		sc.Net.MaxOpenRequests = val
	}

	// parse partitions offset

	if partOffs, ok := pipe.Get(partitionsOffsetsKey).(string); ok {
		po := make(map[string]any)
		err := json.Unmarshal([]byte(partOffs), &po)
		if err != nil {
			return nil, err
		}

		var partition int64
		var offset int64
		for k, v := range po {
			partition, err = strconv.ParseInt(k, 10, 32)
			if err != nil {
				return nil, err
			}

			if val, ok2 := v.(string); ok2 {
				offset, err = strconv.ParseInt(val, 10, 64)
				if err != nil {
					return nil, err
				}
			}

			conf.PartitionsOffsets[int32(partition)] = offset
		}
	} else {
		conf.PartitionsOffsets[0] = sarama.OffsetNewest
	}

	// merge a topic partitions
	for k := range conf.PartitionsOffsets {
		conf.topicPartitions[conf.Topic] = append(conf.topicPartitions[conf.Topic], k)
	}

	// create topic options
	conf.CreateTopic = &CreateTopics{
		ReplicationFactor: 0,
		ReplicaAssignment: nil,
		ConfigEntries:     nil,
	}
	if pipe.Get(replicationFactorKey) != nil {
		rf := pipe.String(replicationFactorKey, "1")
		rfi, err := strconv.ParseInt(rf, 10, 16)
		if err != nil {
			return nil, err
		}

		conf.CreateTopic.ReplicationFactor = int16(rfi)
	}

	if pipe.Get(replicaAssignmentKey) != nil {
		if rav, ok := pipe.Get(replicaAssignmentKey).(string); ok {
			var ra map[int32][]int32
			err := json.Unmarshal([]byte(rav), &ra)
			if err != nil {
				return nil, err
			}
			conf.CreateTopic.ReplicaAssignment = ra
		}
	}

	if pipe.Get(configEntriesKey) != nil {
		if cep, ok := pipe.Get(configEntriesKey).(string); ok {
			var ce map[string]*string
			err := json.Unmarshal([]byte(cep), &ce)
			if err != nil {
				return nil, err
			}

			conf.CreateTopic.ConfigEntries = ce
		}
	}

	// consumer ---
	if v := pipe.String(heartbeatIntervalKey, ""); v != "" {
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, err
		}

		sc.Consumer.Group.Heartbeat.Interval = time.Second * time.Duration(val)
	}

	if v := pipe.String(sessionTimeoutKey, ""); v != "" {
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, err
		}

		sc.Consumer.Group.Session.Timeout = time.Second * time.Duration(val)
	}

	if v := pipe.String(maxFetchMessageSizeKey, ""); v != "" {
		val, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, err
		}

		sc.Consumer.Fetch.Default = int32(val)
	}

	if v := pipe.String(minFetchMessageSizeKey, ""); v != "" {
		val, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, err
		}

		sc.Consumer.Fetch.Min = int32(val)
	}

	// producer options ----------------------
	if v := pipe.String(maxMessageSizeKey, ""); v != "" {
		val, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		sc.Producer.MaxMessageBytes = val
	}

	/*
		// NoResponse doesn't send any response, the TCP ACK is all you get.
		NoResponse RequiredAcks = 0
		// WaitForLocal waits for only the local commit to succeed before responding.
		WaitForLocal RequiredAcks = 1
		// WaitForAll waits for all in-sync replicas to commit before responding.
		// The minimum number of in-sync replicas is configured on the broker via
		// the `min.insync.replicas` configuration key.
		WaitForAll RequiredAcks = -1
	*/
	if v := pipe.String(requiredAcksKey, ""); v != "" {
		val, err := strconv.ParseInt(v, 10, 16)
		if err != nil {
			return nil, err
		}

		sc.Producer.RequiredAcks = sarama.RequiredAcks(val)
	}

	if v := pipe.String(timeoutKey, ""); v != "" {
		val, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}

		sc.Producer.Timeout = time.Second * time.Duration(val)
	}

	if v := pipe.String(compressionCodecKey, ""); v != "" {
		switch CompressionCodec(v) {
		case gzip:
			sc.Producer.Compression = sarama.CompressionGZIP
		case lz4:
			sc.Producer.Compression = sarama.CompressionLZ4
		case zstd:
			sc.Producer.Compression = sarama.CompressionZSTD
		case snappy:
			sc.Producer.Compression = sarama.CompressionSnappy
		case none:
			sc.Producer.Compression = sarama.CompressionNone
		default:
			sc.Producer.Compression = sarama.CompressionNone
		}
	}

	if v := pipe.String(compressionLevelKey, ""); v != "" {
		val, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		sc.Producer.CompressionLevel = val
	}

	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true
	sc.Version = parseVersion(pipe.String(versionKey, ""))

	sc.Producer.Idempotent = pipe.Bool(idempotentKey, false)
	sc.ClientID = pipe.String(clientIDKey, "roadrunner")

	// 1 minute in total
	sc.Metadata.Retry.Max = 10
	sc.Metadata.Retry.Backoff = time.Second * 6

	err := sc.Validate()
	if err != nil {
		return nil, err
	}

	// validated config
	conf.kafkaConfig = sc

	return sc, nil
}

func ptrTo[T any](val T) *T {
	return &val
}
