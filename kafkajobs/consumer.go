package kafkajobs

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/sdk/v3/priority_queue"
	"github.com/roadrunner-server/sdk/v3/utils"
	"go.uber.org/zap"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error

	// Has checks if config section exists.
	Has(name string) bool
}

type Consumer struct {
	mu       sync.Mutex
	log      *zap.Logger
	pq       priorityqueue.Queue
	pipeline atomic.Pointer[pipeline.Pipeline]
	cfg      *config

	// kafka config
	kafkaProducer *kafka.Producer
	kafkaConsumer *kafka.Consumer

	listeners uint32
	delayed   *int64
	stopCh    chan struct{}
	stopped   uint32
}

// NewKafkaConsumer initializes kafka pipeline from the configuration
func NewKafkaConsumer(configKey string, log *zap.Logger, cfg Configurer, pq priorityqueue.Queue) (*Consumer, error) {
	const op = errors.Op("new_kafka_consumer")

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
	if conf.CreateTopicsOnStart {
		err = jb.createTopics(&conf, jb.kafkaProducer)
		if err != nil {
			return nil, err
		}
	}

	return jb, nil
}

// FromPipeline initializes pipeline on-the-fly
func FromPipeline(pipeline *pipeline.Pipeline, log *zap.Logger, _ Configurer, pq priorityqueue.Queue) (*Consumer, error) {
	const op = errors.Op("new_kafka_consumer")

	tp := pipeline.String(topics, "default")
	tp = strings.ReplaceAll(tp, " ", "")
	tps := strings.Split(tp, ",")

	var numOfPart int
	var err error
	nop := pipeline.String(numOfPartitions, "1")
	numOfPart, err = strconv.Atoi(nop)
	if err != nil {
		// just use default value
		numOfPart = 1
	}

	conf := &config{
		Priority:            pipeline.Int(pri, 10),
		Topics:              tps,
		CreateTopicsOnStart: pipeline.Bool(createTopicsOnStart, false),
		NumberOfPartitions:  numOfPart,
	}

	// PARSE CONFIGURATION START -------
	topicsConf := pipeline.String(topicsConfig, "")
	err = json.Unmarshal([]byte(topicsConf), &conf.TopicsConfig)
	if err != nil {
		return nil, errors.E(op, err)
	}

	consumerConf := pipeline.String(consumerConfig, "")
	conf.KafkaConsumerConfigMap = &kafka.ConfigMap{}
	err = json.Unmarshal([]byte(consumerConf), conf.KafkaConsumerConfigMap)
	if err != nil {
		return nil, errors.E(op, err)
	}

	producerConf := pipeline.String(producerConfig, "")
	conf.KafkaProducerConfigMap = &kafka.ConfigMap{}
	err = json.Unmarshal([]byte(producerConf), conf.KafkaProducerConfigMap)
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb := &Consumer{
		log:     log,
		pq:      pq,
		stopCh:  make(chan struct{}, 1),
		delayed: utils.Int64(0),
		cfg:     conf,
	}

	// start producer to push the jobs
	jb.kafkaProducer, err = kafka.NewProducer(conf.KafkaProducerConfigMap)
	if err != nil {
		return nil, err
	}
	if conf.CreateTopicsOnStart {
		err = jb.createTopics(conf, jb.kafkaProducer)
		if err != nil {
			return nil, errors.E(op, err)
		}
	}

	return jb, nil
}

func (c *Consumer) Push(ctx context.Context, job *jobs.Job) error {
	const op = errors.Op("kafka_push")
	// check if the pipeline registered

	// load atomic value
	pipe := c.pipeline.Load()
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
	const op = errors.Op("kafka_run")

	pipe := c.pipeline.Load()
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

func (c *Consumer) State(context.Context) (*jobs.State, error) {
	pipe := c.pipeline.Load()

	return &jobs.State{
		Priority: uint64(pipe.Priority()),
		Pipeline: pipe.Name(),
		Driver:   pipe.Driver(),
		Queue:    c.cfg.Topics[0],
		Ready:    atomic.LoadUint32(&c.listeners) > 0,
	}, nil
}

func (c *Consumer) Pause(_ context.Context, p string) {
	start := time.Now()
	pipe := c.pipeline.Load()
	if pipe.Name() != p {
		c.log.Error("no such pipeline", zap.String("requested", p))
	}

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 0 {
		c.log.Warn("no active listeners, nothing to pause")
		return
	}

	// how is that possible, that listener is registered, but consumer is nil???
	if c.kafkaConsumer == nil {
		c.log.Error("consumer is nil, unable to resume")
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	ktp := make(kafka.TopicPartitions, 0, len(c.cfg.Topics))

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

	// remove active listener
	atomic.AddUint32(&c.listeners, ^uint32(0))

	c.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
}

func (c *Consumer) Resume(_ context.Context, p string) {
	start := time.Now()
	pipe := c.pipeline.Load()
	if pipe.Name() != p {
		c.log.Error("no such pipeline", zap.String("requested", p))
	}

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 1 {
		c.log.Warn("amqp listener is already in the active state")
		return
	}

	// protect connection
	c.mu.Lock()
	defer c.mu.Unlock()

	// user called Resume, pipeline was registered from the PHP code
	if c.kafkaConsumer == nil {
		err := c.initConsumer(c.cfg.Topics)
		if err != nil {
			c.log.Error("unable to init consumer", zap.Error(err))
			return
		}

		// start a listener
		go c.listen()
	} else {
		// kafka consumer already initialized
		ktp := make(kafka.TopicPartitions, 0, len(c.cfg.Topics))

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

	// increase number of listeners
	atomic.StoreUint32(&c.listeners, 1)

	c.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
}

func (c *Consumer) Stop(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	start := time.Now()
	atomic.StoreUint32(&c.stopped, 1)
	c.stopCh <- struct{}{}

	pipe := c.pipeline.Load()

	// close all
	if c.kafkaConsumer != nil {
		err := c.kafkaConsumer.Unsubscribe()
		if err != nil {
			c.log.Error("consumer unsubscribe", zap.Error(err))
		}
		err = c.kafkaConsumer.Close()
		if err != nil {
			c.log.Error("consumer close", zap.Error(err))
		}
	}

	if c.kafkaProducer != nil {
		_ = c.kafkaProducer.Flush(1 * 1000)
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

func (c *Consumer) createTopics(conf *config, kp *kafka.Producer) error {
	ac, err := kafka.NewAdminClientFromProducer(kp)
	if err != nil {
		return err
	}

	tspec := make([]kafka.TopicSpecification, 0, len(conf.Topics))

	for i := 0; i < len(conf.Topics); i++ {
		tspec = append(tspec, kafka.TopicSpecification{
			Topic:             conf.Topics[i],
			NumPartitions:     conf.NumberOfPartitions,
			ReplicationFactor: 1,
			Config:            conf.TopicsConfig,
		})
	}

	ktopres, err := ac.CreateTopics(context.Background(), tspec)
	if err != nil {
		return err
	}

	for i := 0; i < len(ktopres); i++ {
		if ktopres[i].Error.Code() != kafka.ErrNoError {
			if ktopres[i].Error.Code() == kafka.ErrTopicAlreadyExists {
				// don't fail if the topic exists
				continue
			}
			return ktopres[i].Error
		}
	}

	return nil
}
