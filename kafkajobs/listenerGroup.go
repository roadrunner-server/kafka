//nolint:unused
package kafkajobs

import (
	"context"

	"github.com/Shopify/sarama"
)

type groupListener struct{}

func (gl *groupListener) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (gl *groupListener) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (gl *groupListener) ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error {
	return nil
}

func (c *Consumer) initGroupConsumer() error {
	var gl groupListener
	var err error
	c.kafkaGroupConsumer, err = sarama.NewConsumerGroup(c.cfg.Addresses, c.cfg.GroupID, c.cfg.kafkaConfig)
	if err != nil {
		return err
	}

	err = c.kafkaGroupConsumer.Consume(context.Background(), []string{c.cfg.Topic}, &gl)
	if err != nil {
		return err
	}

	return nil
}
