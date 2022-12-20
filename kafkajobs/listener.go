package kafkajobs

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"go.uber.org/zap"
)

// blocking function
func (c *Consumer) initConsumer(topics []string) error {
	var err error
	c.kafkaConsumer, err = kafka.NewConsumer(c.cfg.KafkaConsumerConfigMap)
	if err != nil {
		return err
	}

	err = c.kafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) listen() {
	go func() {
		for {
			select {
			case <-c.stopCh:
				return
			default:
				// pipeline was stopped
				if atomic.LoadUint32(&c.stopped) == 1 {
					return
				}

				ev := c.kafkaConsumer.Poll(-1)
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					c.log.Info("partition assigned", zap.String("partitions", e.String()))
					continue
				case kafka.RevokedPartitions:
					c.log.Info("partition revoked", zap.String("partitions", e.String()))
					continue
				case *kafka.Message:
					item := c.fromConsumer(e)

					if item.Options.AutoAck {
						_, err := c.kafkaConsumer.CommitMessage(e)
						if err != nil {
							c.log.Error("failed to commit message", zap.Error(err))
							continue
						}
					}

					c.pq.Insert(item)
				case kafka.PartitionEOF:
					c.log.Info("partition EOF", zap.String("topic", *e.Topic), zap.Int32("partition", e.Partition), zap.Error(e.Error))
					continue
					// redial or other type of error. We can continue our for loop
				case kafka.Error:
					c.log.Error("kafka consumer", zap.String("error", e.String()), zap.Bool("fatal", e.IsFatal()), zap.String("code", e.Code().String()), zap.Bool("retriable", e.IsRetriable()))
					if e.IsFatal() {
						err := c.initConsumer(c.cfg.Topics)
						if err != nil {
							panic(err)
						}
						return
					}
					continue
				default:
					if e != nil {
						c.log.Error("unrecognized", zap.String("error", e.String()))
					}
				}
			}
		}
	}()
}

func (c *Consumer) fromConsumer(msg *kafka.Message) *Item {
	/*
		RRJob      string = "rr_job"
		RRHeaders  string = "rr_headers"
		RRPipeline string = "rr_pipeline"
		RRDelay    string = "rr_delay"
		RRPriority string = "rr_priority"
		RRAutoAck  string = "rr_auto_ack"
	*/

	var rrjob string
	var rrpipeline string
	var rrpriority int64
	var rrautoack bool
	headers := make(map[string][]string)

	for i := 0; i < len(msg.Headers); i++ {
		switch msg.Headers[i].Key {
		case jobs.RRJob:
			rrjob = string(msg.Headers[i].Value)
		case jobs.RRPipeline:
			rrpipeline = string(msg.Headers[i].Value)
		case jobs.RRPriority:
			rrpriority = int64(binary.LittleEndian.Uint64(msg.Headers[i].Value))
		case jobs.RRAutoAck:
			rrautoack = true
		default:
			headers[msg.Headers[i].Key] = []string{string(msg.Headers[i].Value)}
		}
	}

	if rrjob == "" {
		rrjob = auto
	}

	if rrpipeline == "" {
		rrpipeline = auto
	}

	if rrpriority == 0 {
		rrpriority = 10
	}

	item := &Item{
		Job:     rrjob,
		Ident:   string(msg.Key),
		Payload: string(msg.Value),
		Headers: headers,
		Options: &Options{
			Priority: rrpriority,
			Pipeline: rrpipeline,
			AutoAck:  rrautoack,

			// private
			partition: msg.TopicPartition.Partition,
			topic:     *msg.TopicPartition.Topic,
			offset:    msg.TopicPartition.Offset,
			consumer:  c.kafkaConsumer,
			producer:  c.kafkaProducer,
			log:       c.log,
		},
	}
	return item
}
