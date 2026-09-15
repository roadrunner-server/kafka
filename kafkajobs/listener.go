package kafkajobs

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/events"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	restartStr string = "restart"
	// defaultRestartDelay bounds the restart rate when the error persists
	// across restarts.
	defaultRestartDelay = time.Second * 10
	// defaultRestartResend is the interval between repeated restart commands.
	defaultRestartResend = time.Second * 10
)

func (d *Driver) listen() error {
	var ctx context.Context
	d.mu.Lock()
	// protect context against context update
	ctx, d.kafkaCancelCtx = context.WithCancel(context.Background())
	d.mu.Unlock()

	defer func() {
		d.listening.Store(false)
		if d.cfg.GroupOpts != nil {
			d.kafkaClient.AllowRebalance()
		}
		d.log.Debug("kafka listener stopped")
	}()

	for {
		fetches := d.kafkaClient.PollRecords(ctx, 100)
		if fetches.IsClientClosed() {
			d.listeners.Store(0)

			return errors.New("client is closed, stopping the pipeline")
		}

		// Errors return all errors in a fetch with the topic and partition that
		// errored.
		//
		// There are five classes of errors possible:
		//
		//  1. a normal kerr.Error; these are usually the non-retrievable kerr.Errors,
		//     but theoretically a non-retrievable error can be fixed at runtime (auth
		//     error? fix auth). It is worth restarting the client for these errors if
		//     you do not intend to fix this problem at runtime.
		//
		//  2. an injected *ErrDataLoss; these are informational, the client
		//     automatically resets consuming to where it should and resumes. This
		//     error is worth logging and investigating but not worth restarting the
		//     client for.
		//
		//  3. an untyped batch parse failure; these are usually unrecoverable by
		//     restarts, and it may be best to just let the client continue. However,
		//     restarting is an option, but you may need to manually repair your
		//     partition.
		//
		//  4. an injected ErrClientClosed; this is a fatal informational error that
		//     is returned from every Poll call if the client has been closed.
		//     A corresponding helper function IsClientClosed can be used to detect
		//     this error.
		//
		//  5. an injected *ErrGroupSession; the client lost its group session and
		//     rejoins the group on its own. The error unwraps to the kerr.Error
		//     that caused the loss.
		//     https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#ErrGroupSession

		var edl *kgo.ErrDataLoss
		var regErr *kerr.Error

		errs := fetches.Errors()
		for i := range errs {
			switch {
			case errors.As(errs[i].Err, &edl):
				d.log.Warn("restarting consumer",
					"topic", errs[i].Topic,
					"partition", errs[i].Partition,
					"error", errs[i].Err)
				continue

			case groupRejoins(errs[i].Err):
				d.log.Warn("group session was lost, the consumer rejoins the group",
					"topic", errs[i].Topic,
					"partition", errs[i].Partition,
					"error", errs[i].Err)
				continue

			case errors.As(errs[i].Err, &regErr):
				// https://kafka.apache.org/protocol.html#protocol_error_codes
				switch regErr.Retriable {
				case true:
					d.log.Warn("retrievable consumer error, restarting consumer",
						"topic", errs[i].Topic,
						"partition", errs[i].Partition,
						"code", regErr.Code,
						"description", regErr.Description,
						"message", regErr.Message)

					// unknown_topic_id (100); more codes will be added as needed
					if regErr.Code == 100 {
						d.mu.Lock()
						d.kafkaClient.PurgeTopicsFromClient(errs[i].Topic)
						d.kafkaClient.AddConsumeTopics(errs[i].Topic)
						d.kafkaClient.ForceMetadataRefresh()
						d.mu.Unlock()
					}
					continue
				case false:
					d.log.Error("non-recoverable consumer error",
						"topic", errs[i].Topic,
						"partition", errs[i].Partition,
						"code", regErr.Code,
						"description", regErr.Description,
						"message", regErr.Message)

					d.listeners.Store(0)
					d.restarting.Store(true)

					return errs[i].Err
				}

			case errors.Is(errs[i].Err, context.Canceled):
				d.log.Info("consumer context canceled, stopping the listener",
					"error", errs[i].Err,
					"topic", errs[i].Topic,
					"partition", errs[i].Partition)

				d.listeners.Store(0)

				return nil

			default:
				d.log.Warn("retriable consumer error",
					"error", errs[i].Err,
					"topic", errs[i].Topic,
					"partition", errs[i].Partition)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			item := fromConsumer(r, d.requeueCh, d.recordsCh, &d.stopped)

			ctxT, span := d.tracer.Tracer(tracerName).Start(otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(item.headers)), "kafka_listener")
			d.prop.Inject(ctxT, propagation.HeaderCarrier(item.headers))

			d.pq.Insert(item)

			span.End()
		})

		if d.cfg.GroupOpts != nil {
			d.kafkaClient.AllowRebalance()
		}
	}
}

// groupRejoins reports whether err is a lost group session that the kafka
// client restores by rejoining the group.
func groupRejoins(err error) bool {
	session, ok := errors.AsType[*kgo.ErrGroupSession](err)
	if !ok {
		return false
	}

	return errors.Is(session.Err, kerr.UnknownMemberID) || errors.Is(session.Err, kerr.IllegalGeneration)
}

// requestRestart asks the JOBS plugin to recreate the pipeline through the
// global events bus. It returns when the JOBS plugin stops this driver. The
// first command waits for the restart delay. This bounds the restart rate when
// the error persists across restarts. The bus drops a command when the JOBS
// plugin is busy. The command is sent again until the driver is stopped. The
// stopped check is best-effort. A Destroy that runs at the same time lets one
// command through, and the JOBS plugin rejects it with a warning.
func (d *Driver) requestRestart() {
	// The JOBS plugin writes to the pipeline when it recreates the pipeline.
	// The name is read once, before the driver is stopped.
	name := (*d.pipeline.Load()).Name()
	delay := d.restartDelay

	for {
		select {
		case <-d.done:
		case <-time.After(delay):
		}

		if d.stopped.Load() == 1 {
			d.log.Debug("driver is stopped, the pipeline restart command was not sent", "pipeline", name)
			return
		}

		d.eventBus.Send(events.NewEvent(events.EventJOBSDriverCommand, name, restartStr))
		d.log.Info("pipeline restart command was sent", "pipeline", name)
		delay = d.restartResend
	}
}

func fromConsumer(msg *kgo.Record, reqCh chan *Item, commCh chan *kgo.Record, stopped *atomic.Uint64) *Item {
	/*
		RRJob      string = "rr_job"
		RRHeaders  string = "rr_headers"
		RRPipeline string = "rr_pipeline"
		RRDelay    string = "rr_delay"
		RRPriority string = "rr_priority"
		RRAutoAck  string = "rr_auto_ack"
	*/

	var rrjob string
	var rrpipeline string
	var rrpriority int64
	headers := make(map[string][]string)

	for i := range msg.Headers {
		switch msg.Headers[i].Key {
		case jobs.RRJob:
			rrjob = string(msg.Headers[i].Value)
		case jobs.RRPipeline:
			rrpipeline = string(msg.Headers[i].Value)
		case jobs.RRPriority:
			rrpriority = int64(binary.LittleEndian.Uint64(msg.Headers[i].Value)) //nolint:gosec
		default:
			headers[msg.Headers[i].Key] = []string{string(msg.Headers[i].Value)}
		}
	}

	if rrjob == "" {
		rrjob = auto
	}

	if rrpipeline == "" {
		rrpipeline = auto
	}

	if rrpriority == 0 {
		rrpriority = 10
	}

	item := &Item{
		Job:     rrjob,
		Ident:   string(msg.Key),
		Payload: msg.Value,
		headers: headers,

		stopped:   stopped,
		requeueCh: reqCh,
		commitsCh: commCh,
		record:    msg,

		Options: &Options{
			Priority: rrpriority,
			Pipeline: rrpipeline,

			// private
			Partition: msg.Partition,
			Queue:     msg.Topic,
			Offset:    msg.Offset,
		},
	}

	return item
}
