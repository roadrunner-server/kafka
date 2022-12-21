package kafkajobs

import (
	"context"
	"encoding/binary"

	"github.com/Shopify/sarama"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func (c *Consumer) listen(pConsumers []sarama.PartitionConsumer) {
	go func() {
		for {
			select {
			case <-c.stopCh:
				for i := 0; i < len(pConsumers); i++ {
					pConsumers[i].AsyncClose()
				}
				return
			default:
				iter := c.kafkaClient.PollFetches(context.Background()).RecordIter()

				for !iter.Done() {
					c.pq.Insert(fromConsumer(iter.Next(), c.log))
				}
			}
		}
	}()
}

func fromConsumer(msg *kgo.Record, log *zap.Logger) *Item {
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
	headers := make(map[string][]string)

	for i := 0; i < len(msg.Headers); i++ {
		switch msg.Headers[i].Key {
		case jobs.RRJob:
			rrjob = string(msg.Headers[i].Value)
		case jobs.RRPipeline:
			rrpipeline = string(msg.Headers[i].Value)
		case jobs.RRPriority:
			rrpriority = int64(binary.LittleEndian.Uint64(msg.Headers[i].Value))
		default:
			headers[string(msg.Headers[i].Key)] = []string{string(msg.Headers[i].Value)}
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

			// private
			partition: msg.Partition,
			topic:     msg.Topic,
			offset:    msg.Offset,
			log:       log,
		},
	}
	return item
}
