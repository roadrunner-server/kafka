package kafkajobs

import (
	"context"

	"github.com/Shopify/sarama"
)

type groupListener struct {
}

func (gl *groupListener) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (gl *groupListener) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (gl *groupListener) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return nil
}

func (c *Consumer) initGroupConsumer() error {
	var gl groupListener
	var err error
	c.kafkaGroupConsumer, err = sarama.NewConsumerGroupFromClient(c.cfg.GroupID, c.kafkaClient)
	if err != nil {
		return err
	}

	err = c.kafkaGroupConsumer.Consume(context.Background(), []string{c.cfg.Topic}, &gl)
	if err != nil {
		return err
	}

	return nil
}
