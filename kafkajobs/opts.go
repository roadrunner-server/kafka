package kafkajobs

import (
	"time"
)

// kafka configuration options
// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

const (
	// general options
	autoCreateTopicsEnableKey string = "auto_create_topics_enable"
	priorityKey               string = "priority"

	saslOptionsKey     string = "sasl_options"
	producerOptionsKey string = "producer_options"
	consumerOptionsKey string = "consumer_options"
	groupOptionsKey    string = "group_options"
)

type Offset struct {
	Type  OffsetTypes `mapstructure:"type"`
	Value int64       `mapstructure:"value"`
}

type Acks string

const (
	NoAck     Acks = "NoAck"
	LeaderAck Acks = "LeaderAck"
	AllISRAck Acks = "AllISRAck"
)

type CompressionCodec string

const (
	gzip   CompressionCodec = "gzip"
	snappy CompressionCodec = "snappy"
	lz4    CompressionCodec = "lz4"
	zstd   CompressionCodec = "zstd"
)

type SASLMechanism string

const (
	basic       SASLMechanism = "plain"
	scramSha256 SASLMechanism = "SCRAM-SHA-256"
	scramSha512 SASLMechanism = "SCRAM-SHA-512"
	awsMskIam   SASLMechanism = "aws_msk_iam"
)

type OffsetTypes string

const (
	At         OffsetTypes = "At"
	AfterMilli OffsetTypes = "AfterMilli"
	AtEnd      OffsetTypes = "AtEnd"
	AtStart    OffsetTypes = "AtStart"
	Relative   OffsetTypes = "Relative"
	WithEpoch  OffsetTypes = "WithEpoch"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Brokers []string `mapstructure:"brokers"`
	TLS     *TLS     `mapstructure:"tls"`
	SASL    *SASL    `mapstructure:"sasl"`
	Ping    *Ping    `mapstructure:"ping"`

	// pipeline
	Priority         int           `mapstructure:"priority"`
	AutoCreateTopics bool          `mapstructure:"auto_create_topics_enable"`
	ProducerOpts     *ProducerOpts `mapstructure:"producer_options"`
	ConsumerOpts     *ConsumerOpts `mapstructure:"consumer_options"`
	GroupOpts        *GroupOptions `mapstructure:"group_options"`
}

type SASL struct {
	Type SASLMechanism `mapstructure:"mechanism" json:"mechanism"`

	// plain + SHA
	Username string `mapstructure:"username" json:"username"`
	Password string `mapstructure:"password" json:"password"`
	Zid      string `mapstructure:"zid" json:"zid"`
	Nonce    []byte `mapstructure:"nonce" json:"nonce"`
	IsToken  bool   `mapstructure:"is_token" json:"is_token"`

	// aws_msk_iam
	AccessKey    string `mapstructure:"access_key" json:"access_key"`
	SecretKey    string `mapstructure:"secret_key" json:"secret_key"`
	SessionToken string `mapstructure:"session_token" json:"session_token"`
	UserAgent    string `mapstructure:"user_agent" json:"user_agent"`
}

type Ping struct {
	Timeout time.Duration `mapstructure:"timeout" json:"timeout"`
}

type GroupOptions struct {
	GroupID              string `mapstructure:"group_id" json:"group_id"`
	BlockRebalanceOnPoll bool   `mapstructure:"block_rebalance_on_poll" json:"block_rebalance_on_poll"`
}

type ProducerOpts struct {
	DisableIdempotent  bool             `mapstructure:"disable_idempotent" json:"disable_idempotent"`
	RequiredAcks       Acks             `mapstructure:"required_acks" json:"required_acks"`
	MaxMessageBytes    int32            `mapstructure:"max_message_bytes" json:"max_message_bytes"`
	RequestTimeout     time.Duration    `mapstructure:"request_timeout" json:"request_timeout"`
	DeliveryTimeout    time.Duration    `mapstructure:"delivery_timeout" json:"delivery_timeout"`
	TransactionTimeout time.Duration    `mapstructure:"transaction_timeout" json:"transaction_timeout"`
	CompressionCodec   CompressionCodec `mapstructure:"compression_codec" json:"compression_codec"`
}

type ConsumerOpts struct {
	Topics              []string                     `mapstructure:"topics" json:"topics"`
	ConsumeRegexp       bool                         `mapstructure:"consume_regexp" json:"consume_regexp"`
	MaxFetchMessageSize int32                        `mapstructure:"max_fetch_message_size" json:"max_fetch_message_size"`
	MinFetchMessageSize int32                        `mapstructure:"min_fetch_message_size" json:"min_fetch_message_size"`
	ConsumePartitions   map[string]map[int32]*Offset `mapstructure:"consume_partitions" json:"consume_partitions"`
	ConsumerOffset      *Offset                      `mapstructure:"consumer_offset" json:"consumer_offset"`
}

type ClientAuthType string

const (
	NoClientCert               ClientAuthType = "no_client_cert"
	RequestClientCert          ClientAuthType = "request_client_cert"
	RequireAnyClientCert       ClientAuthType = "require_any_client_cert"
	VerifyClientCertIfGiven    ClientAuthType = "verify_client_cert_if_given"
	RequireAndVerifyClientCert ClientAuthType = "require_and_verify_client_cert"
)

type TLS struct {
	Timeout  time.Duration  `mapstructure:"timeout" json:"timeout"`
	Key      string         `mapstructure:"key"`
	Cert     string         `mapstructure:"cert"`
	RootCA   string         `mapstructure:"root_ca"`
	AuthType ClientAuthType `mapstructure:"client_auth_type"`
}
