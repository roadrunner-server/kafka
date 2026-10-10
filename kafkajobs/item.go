package kafkajobs

import (
	"encoding/json"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ jobs.Job = (*Item)(nil)

const (
	auto string = "deduced_by_rr"
)

type Item struct {
	// Job contains the pluginName of job broker (usually PHP class).
	Job string `json:"job"`
	// Ident is a unique identifier of the job, should be provided from outside
	Ident string `json:"id"`
	// Payload is string data (usually JSON) passed to Job broker.
	Payload []byte `json:"payload"`
	// Headers with key-values pairs
	headers map[string][]string
	// Options contain a set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`

	// kafka related fields
	// private (used to commit messages)
	stopped   *atomic.Uint64
	commitsCh chan *kgo.Record
	pq        jobs.Queue
	record    *kgo.Record

	// done is closed when a worker reply settles the record. The Serial
	// listener waits on it before it inserts the next record of the partition.
	// It is nil in FanOut mode.
	done     chan struct{}
	doneOnce sync.Once
}

// Options carry information about how to handle a given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as a priority not set
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

func (i *Item) GroupID() string {
	return i.Options.Pipeline
}

func (i *Item) Headers() map[string][]string {
	return i.headers
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return i.Payload
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
	if i.stopped.Load() == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}
	select {
	case i.commitsCh <- i.record:
		i.release()
		return nil
	default:
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}
}

// Nack skips the record: the next commit on the partition moves past its
// offset. The only work is to open the serial gate.
func (i *Item) Nack() error {
	i.release()
	return nil
}

func (i *Item) NackWithOptions(requeue bool, delay int) error {
	if i.stopped.Load() == 1 {
		return errors.Str("failed to NackWithOptions the JOB, the pipeline is probably stopped")
	}

	if requeue {
		return i.Requeue(nil, delay)
	}

	i.release()

	return nil
}

// Requeue puts the job back into the pipeline priority queue after the delay.
// The retry is kept only in memory: a record produced to the topic reaches
// every consumer group of that topic. The serial gate stays closed, so the
// retry is the next record of its partition.
func (i *Item) Requeue(headers map[string][]string, delay int) error {
	// check if we have jobs in worker, but the consumer was already stopped
	// TODO: should not be needed after logic update
	if i.stopped.Load() == 1 {
		return errors.Str("failed to requeue the JOB, the pipeline is probably stopped")
	}

	maps.Copy(i.headers, headers)

	// Insert blocks while the queue is full, so it must not run on the jobs poller goroutine
	time.AfterFunc(time.Duration(delay)*time.Second, func() {
		if i.stopped.Load() == 1 {
			return
		}

		i.pq.Insert(i)
	})

	return nil
}

// Respond is not used and presented to satisfy the Job interface
func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

// release opens the serial gate. The first settling reply closes the channel.
// A later reply on a requeued record does nothing.
func (i *Item) release() {
	if i.done == nil {
		return
	}

	i.doneOnce.Do(func() { close(i.done) })
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		headers: job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),

			Queue:     job.Topic(),
			Metadata:  job.Metadata(),
			Partition: job.Partition(),
			Offset:    job.Offset(),
		},
	}
}
