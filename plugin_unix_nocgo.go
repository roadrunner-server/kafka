//go:build (linux || darwin || freebsd) && amd64 && !cgo

package kafka

import (
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/sdk/v3/priority_queue"
)

const (
	pluginName string = "kafka"
)

// https://hub.docker.com/u/confluentinc
// https://docs.confluent.io/platform/current/clients/index.html

type Plugin struct{}

func (p *Plugin) Init() error {
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

// ConsumerFromConfig constructs kafka driver from the .rr.yaml configuration
func (p *Plugin) ConsumerFromConfig(_ string, _ priorityqueue.Queue) (jobs.Consumer, error) {
	return nil, errors.Str("CGO_ENABLED=1 required to build Kafka with the `librdkafka` C++ library")
}

// ConsumerFromPipeline constructs kafka driver from pipeline
func (p *Plugin) ConsumerFromPipeline(_ *pipeline.Pipeline, _ priorityqueue.Queue) (jobs.Consumer, error) {
	return nil, errors.Str("CGO_ENABLED=1 required to build Kafka with the `librdkafka` C++ library")
}
