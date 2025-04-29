package kafkajobs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/roadrunner-server/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.uber.org/zap"
)

const defaultPingTimeout = time.Second * 10
const defaultTLSTimeout = time.Second * 10

func (c *config) InitDefault(l *zap.Logger) ([]kgo.Opt, error) {
	const op = errors.Op("config.InitDefault")
	opts := make([]kgo.Opt, 0, 1)

	if c.AutoCreateTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	opts = append(opts, kgo.WithLogger(newLogger(l.Named("kgo"))))
	opts = append(opts, kgo.SeedBrokers(c.Brokers...))
	opts = append(opts, kgo.MaxVersions(kversion.Stable()))
	opts = append(opts, kgo.RetryTimeout(time.Minute*5))

	if c.enableTLS() {
		netDialer := &net.Dialer{Timeout: defaultTLSTimeout}

		if c.TLS.Timeout != 0 {
			netDialer.Timeout = c.TLS.Timeout
		}

		// check for the key and cert files
		if c.TLS.Key != "" {
			if _, err := os.Stat(c.TLS.Key); err != nil {
				if os.IsNotExist(err) {
					return nil, errors.E(op, errors.Errorf("private key file '%s' does not exist", c.TLS.Key))
				}

				return nil, errors.E(op, err)
			}
		}

		if c.TLS.Cert != "" {
			if _, err := os.Stat(c.TLS.Cert); err != nil {
				if os.IsNotExist(err) {
					return nil, errors.E(op, errors.Errorf("public certificate file '%s' does not exist", c.TLS.Cert))
				}

				return nil, errors.E(op, err)
			}
		}

		// if rootCA is provided - check it
		if c.TLS.RootCA != "" {
			if _, err := os.Stat(c.TLS.RootCA); err != nil {
				if os.IsNotExist(err) {
					return nil, errors.Errorf("root CA file '%s' does not exist", c.TLS.RootCA)
				}

				return nil, err
			}
		}

		tlsDialerConfig, err := c.tlsConfig()
		if err != nil {
			return nil, err
		}

		tlsDialer := &tls.Dialer{
			NetDialer: netDialer,
			Config:    tlsDialerConfig,
		}

		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	if c.SASL != nil {
		switch c.SASL.Type {
		case scramSha256:
			opts = append(opts, kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
				return scram.Auth{
					Zid:     c.SASL.Zid,
					User:    c.SASL.Username,
					Pass:    c.SASL.Password,
					Nonce:   c.SASL.Nonce,
					IsToken: c.SASL.IsToken,
				}, nil
			})))
		case scramSha512:
			opts = append(opts, kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
				return scram.Auth{
					Zid:     c.SASL.Zid,
					User:    c.SASL.Username,
					Pass:    c.SASL.Password,
					Nonce:   c.SASL.Nonce,
					IsToken: c.SASL.IsToken,
				}, nil
			})))
		case basic:
			opts = append(opts, kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
				return plain.Auth{
					User: c.SASL.Username,
					Zid:  c.SASL.Zid,
					Pass: c.SASL.Password,
				}, nil
			})))
		case awsMskIam:
			sess, err := session.NewSession()
			if err != nil {
				return nil, errors.Errorf("unable to initialize AWS session: %v", err)
			}

			opts = append(opts, kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
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
			})))
		default:
			return nil, errors.Errorf("unknown SASL authorization mechanism: %s", c.SASL.Type)
		}
	}

	if c.GroupOpts != nil {
		if c.GroupOpts.GroupID == "" {
			return nil, errors.Str("no group ID defined for group options")
		}

		if c.GroupOpts.InstanceID != "" {
			opts = append(opts, kgo.InstanceID(c.GroupOpts.InstanceID))
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
			return nil, errors.Str("topics and consume partitions should not be empty for the consumer")
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
				offset = kgo.NewOffset().WithEpoch(int32(val)) //nolint:gosec
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
							kgoOff[kk] = kgo.NewOffset().WithEpoch(int32(vv.Value)) //nolint:gosec
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

	if c.Ping == nil {
		c.Ping = &Ping{
			Timeout: defaultPingTimeout,
		}
	}

	return opts, nil
}

func (c *config) tlsConfig() (*tls.Config, error) {
	tlsDialerConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if c.TLS.Key != "" && c.TLS.Cert != "" {
		cert, err := tls.LoadX509KeyPair(c.TLS.Cert, c.TLS.Key)
		if err != nil {
			return nil, err
		}

		tlsDialerConfig.Certificates = []tls.Certificate{cert}
	}

	// RootCA is optional, but if provided - check it
	if c.TLS.RootCA != "" {
		// auth type used only for the CA
		switch c.TLS.AuthType {
		case NoClientCert:
			tlsDialerConfig.ClientAuth = tls.NoClientCert
		case RequestClientCert:
			tlsDialerConfig.ClientAuth = tls.RequestClientCert
		case RequireAnyClientCert:
			tlsDialerConfig.ClientAuth = tls.RequireAnyClientCert
		case VerifyClientCertIfGiven:
			tlsDialerConfig.ClientAuth = tls.VerifyClientCertIfGiven
		case RequireAndVerifyClientCert:
			tlsDialerConfig.ClientAuth = tls.RequireAndVerifyClientCert
		default:
			tlsDialerConfig.ClientAuth = tls.NoClientCert
		}

		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		if certPool == nil {
			certPool = x509.NewCertPool()
		}

		rca, err := os.ReadFile(c.TLS.RootCA)
		if err != nil {
			return nil, err
		}

		if ok := certPool.AppendCertsFromPEM(rca); !ok {
			return nil, errors.Errorf("could not append certificates from Root CA file '%s'", c.TLS.RootCA)
		}

		tlsDialerConfig.RootCAs = certPool
	}

	return tlsDialerConfig, nil
}

func (c *config) enableTLS() bool {
	if c.TLS != nil {
		return (c.TLS.Key != "" && c.TLS.Cert != "") || c.TLS.RootCA != ""
	}
	return false
}
