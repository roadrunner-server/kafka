package kafkajobs

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/roadrunner-server/api/v4/plugins/v3/jobs"
	"github.com/roadrunner-server/events"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

const (
	restartStr string = "restart"
)

func (d *Driver) listen() error {
	var ctx context.Context
	d.mu.Lock()
	// protect context against context update
	ctx, d.kafkaCancelCtx = context.WithCancel(context.Background())
	d.mu.Unlock()

	defer func() {
		if d.cfg.GroupOpts != nil {
			d.kafkaClient.AllowRebalance()
		}
		d.log.Debug("kafka listener stopped")
	}()

	for {
		fetches := d.kafkaClient.PollRecords(ctx, 100)
		if fetches.IsClientClosed() {
			// recreate pipeline on fail
			d.eventsCh <- events.NewEvent(events.EventJOBSDriverCommand, (*d.pipeline.Load()).Name(), restartStr)
			d.log.Debug("kafka client closed, sending pipeline restart command")

			// remove all listeners
			atomic.StoreUint32(&d.listeners, 0)

			return errors.New("client is closed, stopping the pipeline")
		}

		// Errors return all errors in a fetch with the topic and partition that
		// errored.
		//
		// There are four classes of errors possible:
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

		var edl *kgo.ErrDataLoss
		var regErr *kerr.Error

		errs := fetches.Errors()
		for i := range errs {
			switch {
			case errors.As(errs[i].Err, &edl):
				d.log.Warn("restarting consumer",
					zap.String("topic", errs[i].Topic),
					zap.Int32("partition", errs[i].Partition),
					zap.Error(errs[i].Err))
				continue

			case errors.As(errs[i].Err, &regErr):
				var errP *kerr.Error
				errors.As(errs[i].Err, &errP)
				// https://kafka.apache.org/protocol.html#protocol_error_codes
				switch errP.Retriable {
				case true:
					d.log.Warn("retrievable consumer error, restarting consumer",
						zap.String("topic", errs[i].Topic),
						zap.Int32("partition", errs[i].Partition),
						zap.Int16("code", errP.Code),
						zap.String("description", errP.Description),
						zap.String("message", errP.Message))

					// more codes will be added
					switch errP.Code { //nolint:gocritic
					// unknown_topic_id
					case 100:
						d.mu.Lock()
						d.kafkaClient.PurgeTopicsFromClient(errs[i].Topic)
						d.kafkaClient.AddConsumeTopics(errs[i].Topic)
						d.kafkaClient.ForceMetadataRefresh()
						d.mu.Unlock()
					}
					continue
				case false:
					d.log.Error("non-recoverable consumer error",
						zap.String("topic", errs[i].Topic),
						zap.Int32("partition", errs[i].Partition),
						zap.Int16("code", errP.Code),
						zap.String("description", errP.Description),
						zap.String("message", errP.Message))

					// error is unrecoverable, recreate a pipeline
					d.eventsCh <- events.NewEvent(events.EventJOBSDriverCommand, (*d.pipeline.Load()).Name(), restartStr)

					// remove all listeners
					atomic.StoreUint32(&d.listeners, 0)

					return errs[i].Err
				}

			case errors.Is(errs[i].Err, context.Canceled):
				d.log.Info("consumer context canceled, stopping the listener",
					zap.Error(errs[i].Err),
					zap.String("topic", errs[i].Topic),
					zap.Int32("partition", errs[i].Partition))
				return nil

			default:
				d.log.Warn("retriable consumer error",
					zap.Error(errs[i].Err),
					zap.String("topic", errs[i].Topic),
					zap.Int32("partition", errs[i].Partition))
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

func fromConsumer(msg *kgo.Record, reqCh chan *Item, commCh chan *kgo.Record, stopped *uint64) *Item {
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
