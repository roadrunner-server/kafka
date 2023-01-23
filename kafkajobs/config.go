package kafkajobs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/roadrunner-server/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func (c *config) InitDefault() ([]kgo.Opt, error) {
	opts := make([]kgo.Opt, 0, 1)

	if c.AutoCreateTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	opts = append(opts, kgo.MaxVersions(kversion.Stable()))

	if c.SASL != nil {
		switch c.SASL.Type {
		case basic:
			kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
				return plain.Auth{
					User: c.SASL.Username,
					Zid:  c.SASL.Zid,
					Pass: c.SASL.Password,
				}, nil
			}))
		case awsMskIam:
			sess, err := session.NewSession()
			if err != nil {
				return nil, errors.Errorf("unable to initialize aws session: %v", err)
			}

			kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
				val, err := sess.Config.Credentials.GetWithContext(ctx)
				if err == nil {
					return aws.Auth{
						AccessKey:    val.AccessKeyID,
						SecretKey:    val.SecretAccessKey,
						SessionToken: val.SessionToken,
						UserAgent:    c.SASL.UserAgent,
					}, nil
				}

				return aws.Auth{
					AccessKey:    c.SASL.AccessKey,
					SecretKey:    c.SASL.SecretKey,
					SessionToken: c.SASL.SessionToken,
					UserAgent:    c.SASL.UserAgent,
				}, nil
			}))
		default:
			return nil, errors.Errorf("unknown SASL authorization mechanism: %s", c.SASL.Type)
		}
	}

	if c.GroupOpts != nil {
		if c.GroupOpts.GroupID == "" {
			return nil, errors.Str("no group for the group options")
		}

		opts = append(opts, kgo.ConsumerGroup(c.GroupOpts.GroupID))

		if c.GroupOpts.BlockRebalanceOnPoll {
			opts = append(opts, kgo.BlockRebalanceOnPoll())
		}

		opts = append(opts, kgo.AutoCommitMarks())
	}

	if c.ProducerOpts != nil {
		if c.ProducerOpts.DisableIdempotent {
			opts = append(opts, kgo.DisableIdempotentWrite())
		}

		if c.ProducerOpts.RequiredAcks != "" {
			switch c.ProducerOpts.RequiredAcks {
			case NoAck:
				opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
			case LeaderAck:
				opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
			case AllISRAck:
				opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
			default:
				return nil, errors.Errorf("unknown ACK option provided: %s", c.ProducerOpts.RequiredAcks)
			}
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
		switch {
		case len(c.ConsumerOpts.Topics) > 0:
			opts = append(opts, kgo.ConsumeTopics(c.ConsumerOpts.Topics...))
		case len(c.ConsumerOpts.ConsumePartitions) > 0:
		default:
			return nil, errors.Str("topics should not be empty for the consumer")
		}

		if c.ConsumerOpts.ConsumerOffset != nil {
			var offset kgo.Offset
			val := c.ConsumerOpts.ConsumerOffset.Value
			tp := c.ConsumerOpts.ConsumerOffset.Type

			switch tp {
			case At:
				offset = kgo.NewOffset().At(val)
			case AfterMilli:
				offset = kgo.NewOffset().AfterMilli(val)
			case AtEnd:
				offset = kgo.NewOffset().AtEnd()
			case AtStart:
				offset = kgo.NewOffset().AtStart()
			case Relative:
				offset = kgo.NewOffset().Relative(val)
			case WithEpoch:
				offset = kgo.NewOffset().WithEpoch(int32(val))
			default:
				offset = kgo.NewOffset().AtStart()
			}

			opts = append(opts, kgo.ConsumeResetOffset(offset))
		}

		if c.ConsumerOpts.ConsumeRegexp {
			opts = append(opts, kgo.ConsumeRegex())
		}

		if c.ConsumerOpts.MaxFetchMessageSize != 0 {
			opts = append(opts, kgo.FetchMaxBytes(c.ConsumerOpts.MaxFetchMessageSize))
		}

		if c.ConsumerOpts.MinFetchMessageSize != 0 {
			opts = append(opts, kgo.FetchMinBytes(c.ConsumerOpts.MinFetchMessageSize))
		}

		if len(c.ConsumerOpts.ConsumePartitions) > 0 {
			partitions := make(map[string]map[int32]kgo.Offset, len(c.ConsumerOpts.ConsumePartitions))

			for k, v := range c.ConsumerOpts.ConsumePartitions {
				if len(v) > 0 {
					kgoOff := make(map[int32]kgo.Offset, len(v))
					for kk, vv := range v {
						switch vv.Type {
						case At:
							kgoOff[kk] = kgo.NewOffset().At(vv.Value)
						case AfterMilli:
							kgoOff[kk] = kgo.NewOffset().AfterMilli(vv.Value)
						case AtEnd:
							kgoOff[kk] = kgo.NewOffset().AtEnd()
						case AtStart:
							kgoOff[kk] = kgo.NewOffset().AtStart()
						case Relative:
							kgoOff[kk] = kgo.NewOffset().Relative(vv.Value)
						case WithEpoch:
							kgoOff[kk] = kgo.NewOffset().WithEpoch(int32(vv.Value))
						default:
							return nil, errors.Errorf("unknown type: %s", vv.Type)
						}
					}

					partitions[k] = kgoOff
				}

				kgo.ConsumePartitions(partitions)
			}
		}
	}

	return opts, nil
}
