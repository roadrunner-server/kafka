package kafkajobs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	cfgPlugin "github.com/roadrunner-server/api/v2/plugins/config"
	"github.com/roadrunner-server/api/v2/plugins/jobs"
	"github.com/roadrunner-server/api/v2/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/api/v2/pq"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/utils"
	"go.uber.org/zap"
)

const (
	pluginName string = "kafka"
)

type Consumer struct {
	sync.Mutex
	log      *zap.Logger
	pq       priorityqueue.Queue
	pipeline atomic.Value

	// kafka config
	kafkaProducer *kafka.Producer
	kafkaConsumer *kafka.Consumer

	listeners uint32
	delayed   *int64
	stopCh    chan struct{}
	stopped   uint32
}

// NewKafkaConsumer initializes kafka rabbitmq pipeline
func NewKafkaConsumer(configKey string, log *zap.Logger, cfg cfgPlugin.Configurer, pq priorityqueue.Queue) (*Consumer, error) {
	const op = errors.Op("new_kafka_consumer")
	// we need to obtain two parts of the amqp information here.
	// firs part - address to connect, it is located in the global section under the amqp pluginName
	// second part - queues and other pipeline information
	// if no such key - error
	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	// if no global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global kafka configuration, global configuration should contain kafka "))
	}

	// PARSE CONFIGURATION START -------
	var conf config
	err := cfg.UnmarshalKey(configKey, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(pluginName, &conf.KafkaConfigMap)
	if err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefault()
	// PARSE CONFIGURATION END -------

	jb := &Consumer{
		log:     log,
		pq:      pq,
		stopCh:  make(chan struct{}, 1),
		delayed: utils.Int64(0),
	}

	jb.kafkaProducer, err = kafka.NewProducer(conf.KafkaConfigMap)
	if err != nil {
		return nil, err
	}

	return jb, nil
}

func FromPipeline(pipeline *pipeline.Pipeline, log *zap.Logger, cfg cfgPlugin.Configurer, pq priorityqueue.Queue) (*Consumer, error) {
	const op = errors.Op("new_amqp_consumer_from_pipeline")
	// we need to obtain two parts of the amqp information here.
	// firs part - address to connect, it is located in the global section under the amqp pluginName
	// second part - queues and other pipeline information

	// only global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global amqp configuration, global configuration should contain amqp addrs"))
	}

	// PARSE CONFIGURATION -------
	var conf config
	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}
	conf.InitDefault()
	// PARSE CONFIGURATION -------

	jb := &Consumer{
		log:     log,
		pq:      pq,
		stopCh:  make(chan struct{}, 1),
		delayed: utils.Int64(0),
	}

	return jb, nil
}

func (c *Consumer) Push(ctx context.Context, job *jobs.Job) error {
	const op = errors.Op("rabbitmq_push")
	// check if the pipeline registered

	// load atomic value
	pipe := c.pipeline.Load().(*pipeline.Pipeline)
	if pipe.Name() != job.Options.Pipeline {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", job.Options.Pipeline, pipe.Name()))
	}

	err := c.handleItem(ctx, fromJob(job))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (c *Consumer) Register(_ context.Context, p *pipeline.Pipeline) error {
	c.pipeline.Store(p)
	return nil
}

func (c *Consumer) Run(_ context.Context, p *pipeline.Pipeline) error {
	start := time.Now()
	const op = errors.Op("rabbit_run")

	pipe := c.pipeline.Load().(*pipeline.Pipeline)
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	// protect connection (redial)
	c.Lock()
	defer c.Unlock()

	atomic.StoreUint32(&c.listeners, 1)
	c.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (c *Consumer) State(ctx context.Context) (*jobs.State, error) {
	return nil, nil
}

func (c *Consumer) Pause(_ context.Context, p string) {
	start := time.Now()
	pipe := c.pipeline.Load().(*pipeline.Pipeline)
	if pipe.Name() != p {
		c.log.Error("no such pipeline", zap.String("requested", p))
	}

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 0 {
		c.log.Warn("no active listeners, nothing to pause")
		return
	}

	c.kafkaConsumer.Pause([]kafka.TopicPartition{})

	atomic.AddUint32(&c.listeners, ^uint32(0))

	// protect connection (redial)
	c.Lock()
	defer c.Unlock()

	c.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
}

func (c *Consumer) Resume(_ context.Context, p string) {
	start := time.Now()
	pipe := c.pipeline.Load().(*pipeline.Pipeline)
	if pipe.Name() != p {
		c.log.Error("no such pipeline", zap.String("requested", p))
	}

	// protect connection (redial)
	c.Lock()
	defer c.Unlock()

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 1 {
		c.log.Warn("amqp listener is already in the active state")
		return
	}

	c.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
}

func (c *Consumer) Stop(context.Context) error {
	start := time.Now()
	atomic.StoreUint32(&c.stopped, 1)
	c.stopCh <- struct{}{}

	pipe := c.pipeline.Load().(*pipeline.Pipeline)

	// close all
	if c.kafkaConsumer != nil {
		_ = c.kafkaConsumer.Close()
	}

	if c.kafkaProducer != nil {
		c.kafkaProducer.Close()
	}

	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

// handleItem
func (c *Consumer) handleItem(ctx context.Context, msg *Item) error {
	const op = errors.Op("kafka_handle_item")

	delivCh := make(chan kafka.Event, 1)

	kh := make([]kafka.Header, len(msg.Headers))

	for k, v := range msg.Headers {
		for i := 0; i < len(v); i++ {
			kh = append(kh, kafka.Header{
				Key:   k,
				Value: utils.AsBytes(v[i]),
			})
		}
	}

	err := c.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     nil,
			Partition: 0,
			Offset:    0,
			Metadata:  nil,
		},
		Value:         msg.Body(),
		Key:           utils.AsBytes(msg.ID()),
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
		Headers:       kh,
	}, delivCh)
	if err != nil {
		return err
	}

	for {
		select {
		case e := <-delivCh:
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					return errors.Errorf("delivery failed: %v\n", ev.TopicPartition)
				} else {
					c.log.Debug("message delivered", zap.String("topic", *ev.TopicPartition.Topic))
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func ptrTo[T any](val T) *T {
	return &val
}
