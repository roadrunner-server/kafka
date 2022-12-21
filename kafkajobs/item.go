package kafkajobs

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/utils"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

var _ jobs.Acknowledger = (*Item)(nil)

const (
	auto string = "deduced_by_rr"
)

type Item struct {
	// Job contains pluginName of job broker (usually PHP class).
	Job string `json:"job"`

	// Ident is unique identifier of the job, should be provided from outside
	Ident string `json:"id"`

	// Payload is string data (usually JSON) passed to Job broker.
	Payload string `json:"payload"`

	// Headers with key-values pairs
	Headers map[string][]string `json:"headers"`

	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as priority not set
	Priority int64 `json:"priority"`

	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`

	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int64 `json:"delay,omitempty"`

	// AutoAck option
	AutoAck bool `json:"auto_ack"`

	// kafka related fields
	// private (used to commit messages)
	log       *zap.Logger
	topic     string
	metadata  string
	partition int32
	offset    int64
	cl        *kgo.Client
}

// DelayDuration returns delay duration in a form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return utils.AsBytes(i.Payload)
}

// Context packs job context (job, id) into binary payload.
// Not used in the amqp, amqp.Table used instead
func (i *Item) Context() ([]byte, error) {
	ctx, err := json.Marshal(
		struct {
			ID        string              `json:"id"`
			Job       string              `json:"job"`
			Headers   map[string][]string `json:"headers"`
			Pipeline  string              `json:"pipeline"`
			Topic     string              `json:"topic"`
			Partition int32               `json:"partition"`
			Offset    int64               `json:"offset"`
		}{ID: i.Ident, Job: i.Job, Headers: i.Headers,
			Pipeline: i.Options.Pipeline,
			Topic:    i.Options.topic, Partition: i.Options.partition, Offset: i.Options.offset},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Ack() error {
	i.Options.cl.
	return nil
}

func (i *Item) Nack() error {
	return nil
}

func (i *Item) Copy() *Item {
	item := new(Item)
	*item = *i

	item.Options = &Options{
		Priority:  i.Options.Priority,
		Pipeline:  i.Options.Pipeline,
		Delay:     i.Options.Delay,
		AutoAck:   i.Options.AutoAck,
		topic:     i.Options.topic,
		partition: i.Options.partition,
		metadata:  i.Options.metadata,
		offset:    i.Options.offset,
	}

	return item
}

// Requeue with the provided delay, handled by the Nack
func (i *Item) Requeue(headers map[string][]string, _ int64) error {
	const op = errors.Op("item_requeue")

	msg := i.Copy()
	msg.Headers = headers

	kh := make([]kgo.RecordHeader, 0, len(msg.Headers))

	// only 1 header per key is supported
	// RR_HEADERS
	for k, v := range msg.Headers {
		if len(v) > 0 {
			kh = append(kh, kgo.RecordHeader{
				Key:   k,
				Value: utils.AsBytes(v[0]),
			})
		}
	}

	/*
		RRID       string = "rr_id"
		RRJob      string = "rr_job"
		RRHeaders  string = "rr_headers"
		RRPipeline string = "rr_pipeline"
		RRDelay    string = "rr_delay"
		RRPriority string = "rr_priority"
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
	pri := make([]byte, 8)
	binary.LittleEndian.PutUint64(pri, uint64(msg.Priority()))
	kh = append(kh, kgo.RecordHeader{
		Key:   jobs.RRPriority,
		Value: pri,
	})

	if i.Options.cl == nil {
		return errors.E(op, errors.Str("can't requeue the message, client is not active"))
	}

	id := []byte(msg.ID())

	pr := i.Options.cl.ProduceSync(context.Background(), &kgo.Record{
		Key:       id,
		Value:     msg.Body(),
		Headers:   kh,
		Timestamp: time.Time{},
		Topic:     msg.Options.topic,
		Partition: msg.Options.partition,
		Attrs:     kgo.RecordAttrs{},
		Offset:    msg.Options.offset,
	})

	err := pr.FirstErr()
	if err != nil {
		return err
	}
	return nil
}

// Respond is not used and presented to satisfy the Job interface
func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job *jobs.Job) *Item {
	return &Item{
		Job:     job.Job,
		Ident:   job.Ident,
		Payload: job.Payload,
		Headers: job.Headers,
		Options: &Options{
			Priority:  job.Options.Priority,
			Pipeline:  job.Options.Pipeline,
			Delay:     job.Options.Delay,
			AutoAck:   job.Options.AutoAck,
			topic:     job.Options.Topic,
			metadata:  job.Options.Metadata,
			partition: job.Options.Partition,
			offset:    job.Options.Offset,
		},
	}
}
