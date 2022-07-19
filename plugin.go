package kafka

import (
	"github.com/roadrunner-server/api/v2/plugins/config"
	"github.com/roadrunner-server/api/v2/plugins/jobs"
	"github.com/roadrunner-server/api/v2/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/api/v2/pq"
	"github.com/roadrunner-server/kafka/v2/kafkajobs"
	"go.uber.org/zap"
)

const (
	pluginName string = "kafka"
)

// https://hub.docker.com/u/confluentinc
// https://docs.confluent.io/platform/current/clients/index.html

type Plugin struct {
	log *zap.Logger
	cfg config.Configurer
}

func (p *Plugin) Init(log *zap.Logger, cfg config.Configurer) error {
	p.log = new(zap.Logger)
	*p.log = *log
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

// ConsumerFromConfig constructs kafka driver from the .rr.yaml configuration
func (p *Plugin) ConsumerFromConfig(configKey string, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return kafkajobs.NewKafkaConsumer(configKey, p.log, p.cfg, pq)
}

// ConsumerFromPipeline constructs kafka driver from pipeline
func (p *Plugin) ConsumerFromPipeline(pipe *pipeline.Pipeline, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return kafkajobs.FromPipeline(pipe, p.log, p.cfg, pq)
}
