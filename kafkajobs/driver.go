package kafkajobs

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/utils"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

const pluginName string = "kafka"

type Driver struct {
	mu       sync.Mutex
	log      *zap.Logger
	pq       pq.Queue
	pipeline atomic.Pointer[jobs.Pipeline]
	cfg      *config

	// kafka config
	kafkaClient    *kgo.Client
	kafkaCancelCtx context.CancelFunc
	recordsCh      chan *kgo.Record
	requeueCh      chan *Item

	listeners  uint32
	delayed    *int64
	stopCh     chan struct{}
	commandsCh chan<- jobs.Commander
	stopped    uint32
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

// FromConfig initializes kafka pipeline from the configuration
func FromConfig(configKey string, log *zap.Logger, cfg Configurer, pipeline jobs.Pipeline, pq pq.Queue, cmder chan<- jobs.Commander) (*Driver, error) {
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

	opts, err := conf.InitDefault()
	if err != nil {
		return nil, errors.E(op, err)
	}
	// PARSE CONFIGURATION END -------

	jb := &Driver{
		log:        log,
		pq:         pq,
		stopCh:     make(chan struct{}, 1),
		recordsCh:  make(chan *kgo.Record, 100),
		requeueCh:  make(chan *Item, 10),
		commandsCh: cmder,
		delayed:    utils.Int64(0),
		cfg:        &conf,
	}

	jb.kafkaClient, err = kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb.pipeline.Store(&pipeline)

	go jb.recordsHandler()
	go jb.requeueHandler()

	return jb, nil
}

// FromPipeline initializes pipeline on-the-fly
func FromPipeline(pipeline jobs.Pipeline, log *zap.Logger, cfg Configurer, pq pq.Queue, cmder chan<- jobs.Commander) (*Driver, error) {
	const op = errors.Op("new_kafka_consumer")

	// no global config
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global configuration found, docs: https://roadrunner.dev/docs/plugins-jobs/2.x/en"))
	}

	var conf config
	// PARSE CONFIGURATION START -------
	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	var pOpt *ProducerOpts
	producerOpts := pipeline.String(producerOptionsKey, "")
	if producerOpts != "" {
		err = json.Unmarshal([]byte(producerOpts), &pOpt)
		if err != nil {
			return nil, errors.E(op, err)
		}

		conf.ProducerOpts = pOpt
	}

	var cOpt *ConsumerOpts
	consumerOpts := pipeline.String(consumerOptionsKey, "")
	if consumerOpts != "" {
		err = json.Unmarshal([]byte(consumerOpts), &cOpt)
		if err != nil {
			return nil, errors.E(op, err)
		}

		conf.ConsumerOpts = cOpt
	}

	var gOpt *GroupOptions
	groupsOpts := pipeline.String(groupOptionsKey, "")
	if groupsOpts != "" {
		err = json.Unmarshal([]byte(groupsOpts), &gOpt)
		if err != nil {
			return nil, errors.E(op, err)
		}

		conf.GroupOpts = gOpt
	}

	var sOpt *SASL
	saslOpts := pipeline.String(saslOptionsKey, "")
	if saslOpts != "" {
		err = json.Unmarshal([]byte(saslOpts), &sOpt)
		if err != nil {
			return nil, errors.E(op, err)
		}

		conf.SASL = sOpt
	}

	opts, err := conf.InitDefault()
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb := &Driver{
		log:        log,
		pq:         pq,
		stopCh:     make(chan struct{}, 1),
		recordsCh:  make(chan *kgo.Record, 100),
		requeueCh:  make(chan *Item, 10),
		commandsCh: cmder,
		delayed:    utils.Int64(0),
		cfg:        &conf,
	}

	conf.AutoCreateTopics = pipeline.Bool(autoCreateTopicsEnableKey, false)
	conf.Priority = pipeline.Int(priorityKey, 10)

	jb.kafkaClient, err = kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb.pipeline.Store(&pipeline)

	go jb.recordsHandler()
	go jb.requeueHandler()

	return jb, nil
}

func (d *Driver) Run(_ context.Context, p jobs.Pipeline) error {
	start := time.Now()
	const op = errors.Op("kafka_run")

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	go func() {
		err := d.listen()
		if err != nil {
			d.log.Error("listener error", zap.Error(err))
		}
	}()

	atomic.StoreUint32(&d.listeners, 1)
	d.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (d *Driver) Push(_ context.Context, job jobs.Job) error {
	const op = errors.Op("kafka_push")
	// check if the pipeline registered

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != job.Pipeline() {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", job.Pipeline(), pipe.Name()))
	}

	err := d.handleItem(fromJob(job))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (d *Driver) State(context.Context) (*jobs.State, error) {
	pipe := *d.pipeline.Load()

	return &jobs.State{
		Priority: uint64(pipe.Priority()),
		Pipeline: pipe.Name(),
		Driver:   pipe.Driver(),
		Queue:    topics(d.cfg),
		Ready:    atomic.LoadUint32(&d.listeners) > 0,
	}, nil
}

func (d *Driver) Pause(_ context.Context, p string) error {
	start := time.Now()
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline", zap.String("requested", p))
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	if d.cfg.ConsumerOpts != nil {
		d.kafkaClient.PauseFetchTopics(d.cfg.ConsumerOpts.Topics...)
	}

	// remove active listener
	atomic.AddUint32(&d.listeners, ^uint32(0))

	d.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Resume(_ context.Context, p string) error {
	start := time.Now()
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline", zap.String("requested", p))
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 1 {
		//c.log.Warn("amqp listener is already in the active state")
		return errors.Str("amqp listener is already in the active state")
	}

	if d.cfg.ConsumerOpts != nil {
		d.kafkaClient.ResumeFetchTopics(d.cfg.ConsumerOpts.Topics...)
	}

	// increase number of listeners
	atomic.StoreUint32(&d.listeners, 1)

	d.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Stop(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	start := time.Now()
	atomic.StoreUint32(&d.stopped, 1)
	d.stopCh <- struct{}{}

	// cancel the consumer
	d.kafkaCancelCtx()

	close(d.requeueCh)
	close(d.recordsCh)

	pipe := *d.pipeline.Load()
	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

// handleItem
func (d *Driver) handleItem(msg *Item) error {
	const op = errors.Op("kafka_handle_item")

	kh := make([]kgo.RecordHeader, 0, len(msg.Hdrs))

	// only 1 header per key is supported
	// RR_HEADERS
	for k, v := range msg.Hdrs {
		if len(v) > 0 {
			kh = append(kh, kgo.RecordHeader{
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
	kh = append(kh, kgo.RecordHeader{
		Key:   jobs.RRJob,
		Value: utils.AsBytes(msg.Job),
	})
	// RRPipeline
	kh = append(kh, kgo.RecordHeader{
		Key:   jobs.RRPipeline,
		Value: utils.AsBytes(msg.Options.Pipeline),
	})
	// RRPriority
	rrpri := make([]byte, 8)
	binary.LittleEndian.PutUint64(rrpri, uint64(msg.Priority()))
	kh = append(kh, kgo.RecordHeader{
		Key:   jobs.RRPriority,
		Value: rrpri,
	})

	pr := d.kafkaClient.ProduceSync(context.Background(), &kgo.Record{
		Key:       []byte(msg.ID()),
		Value:     msg.Body(),
		Headers:   kh,
		Timestamp: time.Now(),
		Topic:     msg.Topic(),
		Partition: msg.Partition(),
		Offset:    msg.Offset(),
	})

	err := pr.FirstErr()
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func topics(cfg *config) string {
	if cfg.ConsumerOpts != nil {
		return fmt.Sprintf("%s", cfg.ConsumerOpts.Topics)
	}

	return "NO TOPICS"
}

func (d *Driver) recordsHandler() {
	for rec := range d.recordsCh {
		if d.cfg.GroupOpts != nil {
			d.kafkaClient.MarkCommitRecords(rec)
			continue
		}
	}
}

func (d *Driver) requeueHandler() {
	for item := range d.requeueCh {
		err := d.handleItem(item)
		if err != nil {
			d.log.Error("failed to requeue the job", zap.Error(err))
		}
	}
}
