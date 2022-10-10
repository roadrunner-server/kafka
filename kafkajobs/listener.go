package kafkajobs

import (
	"encoding/binary"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/utils"
	"go.uber.org/zap"
)

// blocking function
func (c *Consumer) initConsumer() ([]sarama.PartitionConsumer, error) {
	var err error
	c.kafkaConsumer, err = sarama.NewConsumerFromClient(c.kafkaClient)
	if err != nil {
		return nil, err
	}

	pConsumers := make([]sarama.PartitionConsumer, 0, 1)

	// we have only 1 topic (rustatian)
	for k, v := range c.cfg.topicPartitions {
		for i := 0; i < len(v); i++ {
			pc, errK := c.kafkaConsumer.ConsumePartition(k, v[i], c.cfg.PartitionsOffsets[v[i]])
			if errK != nil {
				return nil, errK
			}

			pConsumers = append(pConsumers, pc)
		}
	}

	if len(pConsumers) == 0 {
		/*
			%v	the value in a default format
				when printing structs, the plus flag (%+v) adds field names
		*/
		return nil, errors.Errorf("no consume configuration provided, please, check the `partitions_offsets` key: %+v", c.cfg.PartitionsOffsets)
	}

	return pConsumers, nil
}

func (c *Consumer) listen(pConsumers []sarama.PartitionConsumer) {
	messagesCh := fanInConsumers(pConsumers)
	errorsCh := fanInConsumersErrors(pConsumers)

	go func() {
		for {
			select {
			case <-c.stopCh:
				for i := 0; i < len(pConsumers); i++ {
					pConsumers[i].AsyncClose()
				}
				return
			case msg := <-messagesCh:
				if msg == nil {
					c.log.Debug("nil message")
					continue
				}

				c.log.Debug("message pushed to the priority queue",
					zap.String("topic", msg.Topic),
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.Any("headers", msg.Headers),
				)

				c.pq.Insert(fromConsumer(msg, c.kafkaProducer, c.log))
			case e := <-errorsCh:
				if e != nil {
					c.log.Error("consume error", zap.Error(e.Err))
				}
			}
		}
	}()
}

func fromConsumer(msg *sarama.ConsumerMessage, kp sarama.AsyncProducer, log *zap.Logger) *Item {
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
		switch utils.AsString(msg.Headers[i].Key) {
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
			producer:  kp,
			log:       log,
		},
	}
	return item
}

func fanInConsumers(cons []sarama.PartitionConsumer) chan *sarama.ConsumerMessage { //nolint:dupl
	out := make(chan *sarama.ConsumerMessage)
	wg := sync.WaitGroup{}

	output := func(c <-chan *sarama.ConsumerMessage) {
		for msg := range c {
			out <- msg
		}
		wg.Done()
	}

	wg.Add(len(cons))
	for i := 0; i < len(cons); i++ {
		ii := i
		go output(cons[ii].Messages())
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func fanInConsumersErrors(cons []sarama.PartitionConsumer) chan *sarama.ConsumerError { //nolint:dupl
	out := make(chan *sarama.ConsumerError)
	wg := sync.WaitGroup{}

	output := func(c <-chan *sarama.ConsumerError) {
		for msg := range c {
			out <- msg
		}
		wg.Done()
	}

	wg.Add(len(cons))
	for i := 0; i < len(cons); i++ {
		ii := i
		go output(cons[ii].Errors())
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
