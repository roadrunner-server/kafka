package kafka

import (
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/kafka/v4/kafkajobs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

const (
	pluginName string = "kafka"
)

// https://hub.docker.com/u/confluentinc
// https://docs.confluent.io/platform/current/clients/index.html

type Plugin struct {
	log    *zap.Logger
	cfg    Configurer
	tracer *sdktrace.TracerProvider
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error

	// Has checks if a config section exists.
	Has(name string) bool
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(pluginName) {
		return errors.E(errors.Disabled)
	}

	p.log = log.NamedLogger(pluginName)
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) Weight() uint {
	return 10
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

// DriverFromConfig constructs kafka driver from the .rr.yaml configuration
func (p *Plugin) DriverFromConfig(configKey string, pq jobs.Queue, pipeline jobs.Pipeline) (jobs.Driver, error) {
	return kafkajobs.FromConfig(p.tracer, configKey, p.log, p.cfg, pipeline, pq)
}

// DriverFromPipeline constructs kafka driver from pipeline
func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, pq jobs.Queue) (jobs.Driver, error) {
	return kafkajobs.FromPipeline(p.tracer, pipe, p.log, p.cfg, pq)
}
