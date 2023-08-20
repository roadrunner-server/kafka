package kafkajobs

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v4/plugins/v2/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/utils"
	"github.com/twmb/franz-go/pkg/kgo"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	pluginName string = "kafka"
	tracerName string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Driver struct {
	mu       sync.Mutex
	log      *zap.Logger
	pq       jobs.Queue
	pipeline atomic.Pointer[jobs.Pipeline]
	cfg      *config
	tracer   *sdktrace.TracerProvider
	prop     propagation.TextMapPropagator

	// kafka config
	kafkaClient    *kgo.Client
	kafkaCancelCtx context.CancelFunc
	recordsCh      chan *kgo.Record
	requeueCh      chan *Item

	listeners  uint32
	delayed    *int64
	commandsCh chan<- jobs.Commander
	stopped    uint64

	once sync.Once
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

// FromConfig initializes kafka pipeline from the configuration
func FromConfig(tracer *sdktrace.TracerProvider, configKey string, log *zap.Logger, cfg Configurer, pipeline jobs.Pipeline, pq jobs.Queue, cmder chan<- jobs.Commander) (*Driver, error) {
	const op = errors.Op("new_kafka_consumer")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	// no global config
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global configuration found, docs: https://roadrunner.dev/docs/queues-kafka/2023.x/en"))
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
		tracer:     tracer,
		prop:       prop,
		log:        log,
		pq:         pq,
		stopped:    0,
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

	err = ping(jb.kafkaClient, log, conf.Ping.Timeout, pipeline)
	if err != nil {
		return nil, err
	}

	jb.pipeline.Store(&pipeline)

	go jb.recordsHandler()
	go jb.requeueHandler()

	return jb, nil
}

// FromPipeline initializes pipeline on-the-fly
func FromPipeline(tracer *sdktrace.TracerProvider, pipeline jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue, cmder chan<- jobs.Commander) (*Driver, error) {
	const op = errors.Op("new_kafka_consumer")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	// no global config
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global configuration found, docs: https://roadrunner.dev/docs/queues-kafka/2023.x/en"))
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

	conf.AutoCreateTopics = pipeline.Bool(autoCreateTopicsEnableKey, false)
	conf.Priority = pipeline.Int(priorityKey, 10)

	opts, err := conf.InitDefault()
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb := &Driver{
		tracer:     tracer,
		prop:       prop,
		log:        log,
		pq:         pq,
		stopped:    0,
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

	err = ping(jb.kafkaClient, log, conf.Ping.Timeout, pipeline)
	if err != nil {
		return nil, err
	}

	jb.pipeline.Store(&pipeline)

	go jb.recordsHandler()
	go jb.requeueHandler()

	return jb, nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	start := time.Now().UTC()
	const op = errors.Op("kafka_run")

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "kafka_run")
	defer span.End()

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

func (d *Driver) Push(ctx context.Context, job jobs.Message) error {
	const op = errors.Op("kafka_push")

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "kafka_push")
	defer span.End()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != job.GroupID() {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", job.GroupID(), pipe.Name()))
	}

	err := d.handleItem(ctx, fromJob(job))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	pipe := *d.pipeline.Load()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "kafka_state")
	defer span.End()

	return &jobs.State{
		Priority: uint64(pipe.Priority()),
		Pipeline: pipe.Name(),
		Driver:   pipe.Driver(),
		Queue:    topics(d.cfg),
		Ready:    atomic.LoadUint32(&d.listeners) > 0,
	}, nil
}

func (d *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "kafka_pause")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	d.mu.Lock()
	if d.cfg.ConsumerOpts != nil {
		d.kafkaClient.PauseFetchTopics(d.cfg.ConsumerOpts.Topics...)
	}
	d.mu.Unlock()

	// remove active listener
	atomic.AddUint32(&d.listeners, ^uint32(0))

	d.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Resume(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "kafka_resume")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 1 {
		return errors.Str("kafka listener is already in the active state")
	}

	d.once.Do(func() {
		go func() {
			err := d.listen()
			if err != nil {
				d.log.Error("listener error", zap.Error(err))
			}
		}()
	})

	d.mu.Lock()
	if d.cfg.ConsumerOpts != nil {
		d.kafkaClient.ResumeFetchTopics(d.cfg.ConsumerOpts.Topics...)
	}
	d.mu.Unlock()

	// increase number of listeners
	atomic.StoreUint32(&d.listeners, 1)

	d.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "kafka_stop")

	d.mu.Lock()
	// set the stopped state
	atomic.StoreUint64(&d.stopped, 1)

	defer func() {
		close(d.requeueCh)
		close(d.recordsCh)
		d.mu.Unlock()
		span.End()
	}()

	start := time.Now().UTC()

	if d.kafkaCancelCtx != nil {
		// cancel the consumer
		d.kafkaCancelCtx()
	}

	d.kafkaClient.CloseAllowingRebalance()

	// properly check for the listeners
	pipe := *d.pipeline.Load()

	// remove all pending JOBS associated with the pipeline
	_ = d.pq.Remove(pipe.Name())

	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

// handleItem
func (d *Driver) handleItem(ctx context.Context, msg *Item) error {
	const op = errors.Op("kafka_handle_item")

	kh := make([]kgo.RecordHeader, 0, len(msg.headers))
	d.prop.Inject(ctx, propagation.HeaderCarrier(msg.headers))

	// only 1 header per key is supported
	// RR_HEADERS
	for k, v := range msg.headers {
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

	pr := d.kafkaClient.ProduceSync(ctx, &kgo.Record{
		Key:       []byte(msg.ID()),
		Value:     msg.Body(),
		Headers:   kh,
		Timestamp: time.Now().UTC(),
		Topic:     msg.Options.Queue,
		Partition: msg.Options.Partition,
		Offset:    msg.Options.Offset,
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err := d.handleItem(ctx, item)
		cancel()
		if err != nil {
			d.log.Error("failed to requeue the job", zap.Error(err))
		}
	}
}

func ping(client *kgo.Client, log *zap.Logger, timeout time.Duration, pipe jobs.Pipeline) error {
	const op = errors.Op("kafka_ping")

	pingCtx, pingCancel := context.WithTimeout(context.Background(), timeout)
	defer pingCancel()

	err := client.Ping(pingCtx)
	if err != nil {
		return errors.E(op, errors.Errorf("ping kafka was failed: %s", err))
	}

	log.Debug("ping kafka: ok", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()))

	return nil
}
