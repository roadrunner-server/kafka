package kafkajobs

import (
	"sync/atomic"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v4/plugins/v2/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/utils"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ jobs.Job = (*Item)(nil)

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
	headers map[string][]string
	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`

	// kafka related fields
	// private (used to commit messages)
	stopped   *uint64
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

	Queue     string
	Metadata  string
	Partition int32
	Offset    int64
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

func (i *Item) PipelineID() string {
	return i.Options.Pipeline
}

func (i *Item) Headers() map[string][]string {
	return i.headers
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
			Driver    string              `json:"driver"`
			Headers   map[string][]string `json:"headers"`
			Pipeline  string              `json:"pipeline"`
			Queue     string              `json:"queue"`
			Topic     string              `json:"topic"`
			Partition int32               `json:"partition"`
			Offset    int64               `json:"offset"`
		}{
			ID:        i.ID(),
			Job:       i.Job,
			Driver:    pluginName,
			Headers:   i.headers,
			Pipeline:  i.Options.Pipeline,
			Queue:     i.Options.Queue,
			Topic:     i.Options.Queue,
			Partition: i.Options.Partition,
			Offset:    i.Options.Offset,
		},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Ack() error {
	// check if we have jobs in worker, but the consumer was already stopped
	// TODO: should not be needed after logic update
	if atomic.LoadUint64(i.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}
	select {
	case i.commitsCh <- i.record:
		return nil
	default:
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
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
		Queue:     i.Options.Queue,
		Partition: i.Options.Partition,
		Metadata:  i.Options.Metadata,
		Offset:    i.Options.Offset,
	}

	return item
}

// Requeue with the provided delay, handled by the Nack
func (i *Item) Requeue(headers map[string][]string, _ int64) error {
	// check if we have jobs in worker, but the consumer was already stopped
	// TODO: should not be needed after logic update
	if atomic.LoadUint64(i.stopped) == 1 {
		return errors.Str("failed to requeue the JOB, the pipeline is probably stopped")
	}

	msg := i.Copy()
	msg.headers = headers

	select {
	case i.requeueCh <- msg:
		return nil
	default:
		return errors.Str("failed to requeue the JOB, the pipeline is probably stopped")
	}
}

// Respond is not used and presented to satisfy the Job interface
func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		headers: job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.PipelineID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),

			Queue:     job.Topic(),
			Metadata:  job.Metadata(),
			Partition: job.Partition(),
			Offset:    job.Offset(),
		},
	}
}
