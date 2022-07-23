package kafkajobs

import (
	"context"
	"encoding/binary"
	"fmt"
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
	mu       sync.Mutex
	log      *zap.Logger
	pq       priorityqueue.Queue
	pipeline atomic.Value
	cfg      *config

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

	// PARSE CONFIGURATION START -------
	var conf config
	err := cfg.UnmarshalKey(configKey, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(fmt.Sprintf("%s.consumer_config", configKey), &conf.KafkaConsumerConfigMap)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(fmt.Sprintf("%s.producer_config", configKey), &conf.KafkaProducerConfigMap)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = conf.InitDefault()
	if err != nil {
		return nil, err
	}
	// PARSE CONFIGURATION END -------

	jb := &Consumer{
		log:     log,
		pq:      pq,
		stopCh:  make(chan struct{}, 1),
		delayed: utils.Int64(0),
		cfg:     &conf,
	}

	// start producer to push the jobs
	jb.kafkaProducer, err = kafka.NewProducer(conf.KafkaProducerConfigMap)
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
	err = conf.InitDefault()
	if err != nil {
		return nil, err
	}
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

	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.initConsumer(c.cfg.Topics)
	if err != nil {
		return err
	}

	// start a listener
	go c.listen()

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

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.kafkaConsumer != nil {
		ktp := make(kafka.TopicPartitions, len(c.cfg.Topics))

		for i := 0; i < len(c.cfg.Topics); i++ {
			ktp = append(ktp, kafka.TopicPartition{
				Topic: ptrTo(c.cfg.Topics[i]),
			})
		}

		err := c.kafkaConsumer.Pause(ktp)
		if err != nil {
			c.log.Error("failed to pause kafka listener", zap.Error(err))
			return
		}
	}

	// remove active listener
	atomic.AddUint32(&c.listeners, ^uint32(0))

	c.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
}

func (c *Consumer) Resume(_ context.Context, p string) {
	start := time.Now()
	pipe := c.pipeline.Load().(*pipeline.Pipeline)
	if pipe.Name() != p {
		c.log.Error("no such pipeline", zap.String("requested", p))
	}

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 1 {
		c.log.Warn("amqp listener is already in the active state")
		return
	}
	// protect connection (redial)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.kafkaConsumer != nil {
		ktp := make(kafka.TopicPartitions, len(c.cfg.Topics))

		for i := 0; i < len(c.cfg.Topics); i++ {
			ktp = append(ktp, kafka.TopicPartition{
				Topic: ptrTo(c.cfg.Topics[i]),
			})
		}

		err := c.kafkaConsumer.Resume(ktp)
		if err != nil {
			c.log.Error("failed to resume kafka listener", zap.Error(err))
			return
		}
	}

	atomic.StoreUint32(&c.listeners, 1)
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
		c.kafkaProducer.Flush(15 * 1000)
		c.kafkaProducer.Close()
	}

	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

// handleItem
func (c *Consumer) handleItem(ctx context.Context, msg *Item) error {
	const op = errors.Op("kafka_handle_item")

	// confirm channel
	eventCh := make(chan kafka.Event, 1)

	kh := make([]kafka.Header, 0, len(msg.Headers))

	// only 1 header per key is supported
	// RR_HEADERS
	for k, v := range msg.Headers {
		if len(v) > 0 {
			kh = append(kh, kafka.Header{
				Key:   k,
				Value: utils.AsBytes(v[0]),
			})
		}
	}

	/*
		RRJob      string = "rr_job"
		RRHeaders  string = "rr_headers"
		RRPipeline string = "rr_pipeline"
		RRDelay    string = "rr_delay"
		RRPriority string = "rr_priority"
		RRAutoAck  string = "rr_auto_ack"
	*/

	// RRJob
	kh = append(kh, kafka.Header{
		Key:   jobs.RRJob,
		Value: []byte(msg.Job),
	})
	// RRPipeline
	kh = append(kh, kafka.Header{
		Key:   jobs.RRPipeline,
		Value: []byte(msg.Options.Pipeline),
	})
	// RRPriority
	pri := make([]byte, 8)
	binary.LittleEndian.PutUint64(pri, uint64(msg.Priority()))
	kh = append(kh, kafka.Header{
		Key:   jobs.RRPriority,
		Value: pri,
	})

	// put auto_ack only if exists
	if msg.Options.AutoAck {
		ack := make([]byte, 1)
		ack[0] = 1
		kh = append(kh, kafka.Header{
			Key:   jobs.RRAutoAck,
			Value: ack,
		})
	}

	err := c.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     ptrTo(msg.Options.topic),
			Partition: msg.Options.partition,
			Offset:    msg.Options.offset,
			Metadata:  ptrTo(msg.Options.metadata),
		},
		Value: msg.Body(),
		// Job ID
		// RRID       string = "rr_id"
		Key:           utils.AsBytes(msg.ID()),
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
		Headers:       kh,
	}, eventCh)
	if err != nil {
		return errors.E(op, err)
	}

	select {
	case e := <-eventCh:
		switch ev := e.(type) { //nolint:gocritic
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				return errors.Errorf("delivery failed: %v", ev.TopicPartition.Error)
			}

			c.log.Debug("message delivered", zap.String("topic", *ev.TopicPartition.Topic))
		}
	case <-ctx.Done():
		return errors.E(errors.TimeOut, ctx.Err())
	}

	return nil
}

func ptrTo[T any](val T) *T {
	return &val
}
