package amqpjobs

import (
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func (c *Consumer) listener(deliv <-chan amqp.Delivery) {
	go func() {
		for msg := range deliv {
			d, err := c.fromDelivery(msg)
			if err != nil {
				c.log.Error("delivery convert", zap.Error(err))
				/*
					Acknowledge failed job to prevent endless loo;
				*/
				err = msg.Ack(false)
				if err != nil {
					c.log.Error("nack failed", zap.Error(err))
				}

				if d != nil {
					d.Headers = nil
					d.Options = nil
				}
				continue
			}

			if d.Options.AutoAck {
				// we don't care about error here, since the job is not important
				_ = msg.Ack(false)
			}

			// insert job into the main priority queue
			c.pq.Insert(d)
		}

		c.log.Debug("delivery channel was closed, leaving the rabbit listener")
		// reduce number of listeners
		if atomic.LoadUint32(&c.listeners) == 0 {
			c.log.Debug("number of listeners", zap.Uint32("listeners", atomic.LoadUint32(&c.listeners)))
			return
		}

		atomic.AddUint32(&c.listeners, ^uint32(0))
		c.log.Debug("number of listeners", zap.Uint32("listeners", atomic.LoadUint32(&c.listeners)))
	}()
}
