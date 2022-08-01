package kafkajobs

import (
	"github.com/roadrunner-server/errors"
)

const (
	topics string = "topics"
	pri    string = "priority"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Addresses []string `mapstructure:"addrs"`

	// kafka local
	Priority   int      `mapstructure:"priority"`
	Topics     []string `mapstructure:"topics"`
	Partitions []int32  `mapstructure:"partitions"`
	GroupID    string   `mapstructure:"group_id, omitempty"`

	// private, combinations of partitions per-topic
	topicPartitions map[string][]int32
}

func (c *config) InitDefault() error {
	if len(c.Topics) == 0 {
		return errors.Str("at least 1 topic should be set per pipeline")
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if len(c.Topics) == 0 {
		c.Topics = append(c.Topics, "default")
	}

	if len(c.Partitions) == 0 {
		c.Partitions = append(c.Partitions, 0)
	}

	// merge a topic partitions
	c.topicPartitions = make(map[string][]int32, len(c.Topics))
	for i := 0; i < len(c.Topics); i++ {
		for j := 0; j < len(c.Partitions); j++ {
			c.topicPartitions[c.Topics[i]] = append(c.topicPartitions[c.Topics[i]], c.Partitions[j])
		}
	}

	return nil
}
