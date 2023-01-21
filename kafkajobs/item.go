package kafkajobs

import (
	"time"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	"github.com/roadrunner-server/sdk/v4/utils"
	"github.com/twmb/franz-go/pkg/kgo"
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
	Pld string `json:"payload"`
	// Headers with key-values pairs
	Hdrs map[string][]string `json:"headers"`
	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`

	// kafka related fields
	// private (used to commit messages)
	commitsCh chan *kgo.Record
	requeueCh chan *Item
	record    *kgo.Record
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

	Topic     string
	Metadata  string
	Partition int32
	Offset    int64
}

// DelayDuration returns delay duration in a form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) Headers() map[string][]string {
	return i.Hdrs
}

func (i *Item) Name() string {
	return i.Job
}

func (i *Item) Metadata() string {
	if i.Options == nil {
		return ""
	}
	return i.Options.Metadata
}
func (i *Item) Partition() int32 {
	if i.Options == nil {
		return 0
	}
	return i.Options.Partition
}
func (i *Item) Offset() int64 {
	if i.Options == nil {
		return 0
	}
	return i.Options.Offset
}

func (i *Item) Topic() string {
	if i.Options == nil {
		return ""
	}
	return i.Options.Topic
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return utils.AsBytes(i.Pld)
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
		}{ID: i.ID(), Job: i.Name(), Headers: i.Headers(),
			Pipeline: i.Options.Pipeline,
			Topic:    i.Topic(), Partition: i.Partition(), Offset: i.Offset()},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Ack() error {
	select {
	case i.commitsCh <- i.record:
		return nil
	default:
		return nil
	}
}

func (i *Item) Nack() error {
	return nil
}

func (i *Item) Copy() *Item {
	item := new(Item)
	*item = *i

	*item.Options = Options{
		Priority:  i.Options.Priority,
		Pipeline:  i.Options.Pipeline,
		Delay:     i.Options.Delay,
		AutoAck:   i.Options.AutoAck,
		Topic:     i.Topic(),
		Partition: i.Partition(),
		Metadata:  i.Metadata(),
		Offset:    i.Offset(),
	}

	return item
}

// Requeue with the provided delay, handled by the Nack
func (i *Item) Requeue(headers map[string][]string, _ int64) error {
	msg := i.Copy()
	msg.Hdrs = headers

	select {
	case i.requeueCh <- msg:
		return nil
	default:
		return nil
	}
}

// Respond is not used and presented to satisfy the Job interface
func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Job) *Item {
	return &Item{
		Job:   job.Name(),
		Ident: job.ID(),
		Pld:   job.Payload(),
		Hdrs:  job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.Pipeline(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),

			Topic:     job.Topic(),
			Metadata:  job.Metadata(),
			Partition: job.Partition(),
			Offset:    job.Offset(),
		},
	}
}
