//go:build (linux || darwin || freebsd) && amd64 && cgo

package kafka

import (
	"github.com/roadrunner-server/kafka/v3/kafkajobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/sdk/v3/priority_queue"
	"go.uber.org/zap"
)

const (
	pluginName string = "kafka"
)

// https://hub.docker.com/u/confluentinc
// https://docs.confluent.io/platform/current/clients/index.html

type Plugin struct {
	log *zap.Logger
	cfg Configurer
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error

	// Has checks if config section exists.
	Has(name string) bool
}

func (p *Plugin) Init(log *zap.Logger, cfg Configurer) error {
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
