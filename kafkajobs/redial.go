package amqpjobs

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// redialer used to redial to the rabbitmq in case of the connection interrupts
func (c *Consumer) redialer() { //nolint:gocognit,gocyclo
}

func (c *Consumer) reset() {
}

func (c *Consumer) redialMergeCh() {
}

func (c *Consumer) redial(amqpErr *amqp.Error) {
}
