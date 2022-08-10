package kafkajobs

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	priorityqueue "github.com/roadrunner-server/api/v2/pq"
	"go.uber.org/zap"
)

type groupListener struct {
	log      *zap.Logger
	kp       sarama.AsyncProducer
	pq       priorityqueue.Queue
	metadata string
	started  chan struct{}
}

func (gl *groupListener) Setup(sarama.ConsumerGroupSession) error {
	gl.log.Debug("group setup")
	gl.started <- struct{}{}
	return nil
}

func (gl *groupListener) Cleanup(sarama.ConsumerGroupSession) error {
	gl.log.Debug("group cleanup")
	return nil
}

func (gl *groupListener) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				gl.log.Debug("nil message")
				continue
			}

			gl.log.Debug("message pushed to the priority queue",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Any("headers", msg.Headers),
			)

			gl.pq.Insert(fromConsumer(msg, gl.kp, gl.log))
			session.MarkMessage(msg, gl.metadata)
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) initCG() error {
	var err error
	c.kafkaCG, err = sarama.NewConsumerGroup(c.cfg.Addresses, c.cfg.GroupID, c.cfg.kafkaConfig)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) listenCG(log *zap.Logger, kp sarama.AsyncProducer, pq priorityqueue.Queue, md string, started chan struct{}) {
	go func() {
		for {
			select {
			case <-c.stopCh:
				return
			default:
				var ctx context.Context
				ctx, c.kafkaCGCancel = context.WithCancel(context.Background())
				err := c.kafkaCG.Consume(ctx, []string{c.cfg.Topic}, &groupListener{
					log:      log,
					kp:       kp,
					pq:       pq,
					metadata: md,
					started:  started,
				})
				if err != nil {
					c.log.Error("consume group", zap.Error(err))
					// super intelligent backoff
					time.Sleep(time.Second)
					continue
				}
			}
		}
	}()
}
