package kafkajobs

import (
	"context"
	"encoding/binary"
	stderr "errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	cfgPlugin "github.com/roadrunner-server/api/v2/plugins/config"
	"github.com/roadrunner-server/api/v2/plugins/jobs"
	"github.com/roadrunner-server/api/v2/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/api/v2/pq"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/utils"
	"go.uber.org/zap"
)

const pluginName string = "kafka"

type Consumer struct {
	mu       sync.Mutex
	log      *zap.Logger
	pq       priorityqueue.Queue
	pipeline atomic.Value
	cfg      *config

	// kafka config
	kafkaClient        sarama.Client
	kafkaProducer      sarama.AsyncProducer
	kafkaConsumer      sarama.Consumer
	kafkaGroupConsumer sarama.ConsumerGroup //nolint:unused

	listeners uint32
	delayed   *int64
	stopCh    chan struct{}
	stopped   uint32
}

// NewKafkaConsumer initializes kafka pipeline from the configuration
func NewKafkaConsumer(configKey string, log *zap.Logger, cfg cfgPlugin.Configurer, pq priorityqueue.Queue) (*Consumer, error) {
	const op = errors.Op("new_kafka_consumer")

	// no global config
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global configuration found, docs: https://roadrunner.dev/docs/plugins-jobs/2.x/en"))
	}

	// no local config
	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	// PARSE CONFIGURATION START -------
	var conf config
	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(configKey, &conf)
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
	jb.kafkaClient, err = sarama.NewClient(conf.Addresses, conf.kafkaConfig)
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb.kafkaProducer, err = sarama.NewAsyncProducerFromClient(jb.kafkaClient)
	if err != nil {
		return nil, errors.E(op, err)
	}

	if conf.CreateTopic != nil {
		err = createTopic(&conf, jb.kafkaClient)
		if err != nil {
			return nil, errors.E(op, err)
		}
	}

	return jb, nil
}

// FromPipeline initializes pipeline on-the-fly
func FromPipeline(pipeline *pipeline.Pipeline, log *zap.Logger, cfg cfgPlugin.Configurer, pq priorityqueue.Queue) (*Consumer, error) {
	const op = errors.Op("new_kafka_consumer")

	// no global config
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global configuration found, docs: https://roadrunner.dev/docs/plugins-jobs/2.x/en"))
	}

	conf := &config{
		Priority: pipeline.Int(priorityKey, 10),
		Topic:    pipeline.String(topicKey, "default"),
	}

	// PARSE CONFIGURATION START -------
	err := cfg.UnmarshalKey(pluginName, &conf)
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
	sconfig := sarama.NewConfig()
	sconfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	sconfig.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	sconfig.Producer.Return.Successes = false

	// start producer to push the jobs
	jb.kafkaProducer, err = sarama.NewAsyncProducer(conf.Addresses, sconfig)
	if err != nil {
		return nil, err
	}

	return jb, nil
}

func (c *Consumer) Push(ctx context.Context, job *jobs.Job) error {
	const op = errors.Op("kafka_push")
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
	const op = errors.Op("kafka_run")

	pipe := c.pipeline.Load().(*pipeline.Pipeline)
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	pConsumer, err := c.initConsumer()
	if err != nil {
		return errors.E(op, err)
	}

	// start a listener
	go c.listen(pConsumer)

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

	// how is that possible, that listener is registered, but consumer is nil???
	if c.kafkaConsumer == nil {
		c.log.Error("consumer is nil, unable to resume")
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.kafkaConsumer.Pause(c.cfg.topicPartitions)

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

	// protect connection
	c.mu.Lock()
	defer c.mu.Unlock()

	// user called Resume, pipeline was registered from the PHP code
	if c.kafkaConsumer == nil {
		pConsumer, err := c.initConsumer()
		if err != nil {
			c.log.Error("unable to init consumer", zap.Error(err))
			return
		}

		// start a listener
		go c.listen(pConsumer)
	} else {
		// kafka consumer already initialized
		c.kafkaConsumer.Resume(c.cfg.topicPartitions)
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

	pipe := c.pipeline.Load().(*pipeline.Pipeline)

	// close all
	if c.kafkaConsumer != nil {
		err := c.kafkaConsumer.Close()
		if err != nil {
			c.log.Error("consumer close", zap.Error(err))
		}
	}

	if c.kafkaProducer != nil {
		err := c.kafkaProducer.Close()
		if err != nil {
			c.log.Error("producer close", zap.Error(err))
		}
	}

	err := c.kafkaClient.Close()
	if err != nil {
		c.log.Error("producer close", zap.Error(err))
	}

	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

// handleItem
func (c *Consumer) handleItem(_ context.Context, msg *Item) error {
	const op = errors.Op("kafka_handle_item")

	kh := make([]sarama.RecordHeader, 0, len(msg.Headers))

	// only 1 header per key is supported
	// RR_HEADERS
	for k, v := range msg.Headers {
		if len(v) > 0 {
			kh = append(kh, sarama.RecordHeader{
				Key:   utils.AsBytes(k),
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
	kh = append(kh, sarama.RecordHeader{
		Key:   utils.AsBytes(jobs.RRJob),
		Value: utils.AsBytes(msg.Job),
	})
	// RRPipeline
	kh = append(kh, sarama.RecordHeader{
		Key:   utils.AsBytes(jobs.RRPipeline),
		Value: utils.AsBytes(msg.Options.Pipeline),
	})
	// RRPriority
	rrpri := make([]byte, 8)
	binary.LittleEndian.PutUint64(rrpri, uint64(msg.Priority()))
	kh = append(kh, sarama.RecordHeader{
		Key:   utils.AsBytes(jobs.RRPriority),
		Value: rrpri,
	})

	// put auto_ack only if exists
	if msg.Options.AutoAck {
		ack := make([]byte, 1)
		ack[0] = 1
		kh = append(kh, sarama.RecordHeader{
			Key:   utils.AsBytes(jobs.RRAutoAck),
			Value: ack,
		})
	}

	id := []byte(msg.ID())
	c.kafkaProducer.Input() <- &sarama.ProducerMessage{
		Topic:     msg.Options.topic,
		Key:       JobKVEncoder{value: id},
		Value:     JobKVEncoder{value: msg.Body()},
		Headers:   kh,
		Metadata:  msg.Options.metadata,
		Offset:    msg.Options.offset,
		Partition: msg.Options.partition,
		Timestamp: time.Time{},
	}

	select {
	case s := <-c.kafkaProducer.Successes():
		c.log.Debug("message sent", zap.Int32("partition", s.Partition), zap.Int64("offset", s.Offset))
	case e := <-c.kafkaProducer.Errors():
		return errors.E(op, e.Err)
	}

	return nil
}

func createTopic(conf *config, client sarama.Client) error {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return err
	}

	err = admin.CreateTopic(conf.Topic, &sarama.TopicDetail{
		NumPartitions:     int32(len(conf.PartitionsOffsets)),
		ReplicationFactor: conf.CreateTopic.ReplicationFactory,
		ReplicaAssignment: conf.CreateTopic.ReplicaAssignment,
		ConfigEntries:     conf.CreateTopic.ConfigEntries,
	}, false)
	if err != nil {
		if stderr.Is(err, sarama.ErrTopicAlreadyExists) {
			return nil
		}
		return err
	}

	return nil
}
