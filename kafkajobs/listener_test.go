package kafkajobs

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/events"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const listenerTestTimeout = time.Second * 10

// recordingHandler collects the log messages. The listener goroutine writes
// the messages and the test goroutine reads them, so the mutex is required.
type recordingHandler struct {
	mu       sync.Mutex
	messages []string
}

func (*recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.messages = append(h.messages, r.Message)
	h.mu.Unlock()

	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) count(message string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	n := 0
	for _, m := range h.messages {
		if m == message {
			n++
		}
	}

	return n
}

// testPipeline is the pipeline the JOBS plugin associates with the driver.
type testPipeline struct {
	name string
}

func (p *testPipeline) Name() string                      { return p.name }
func (*testPipeline) Driver() string                      { return pluginName }
func (*testPipeline) With(string, any)                    {}
func (*testPipeline) Has(string) bool                     { return false }
func (*testPipeline) String(_ string, d string) string    { return d }
func (*testPipeline) Int(_ string, d int) int             { return d }
func (*testPipeline) Bool(_ string, d bool) bool          { return d }
func (*testPipeline) Map(string, map[string]string) error { return nil }
func (*testPipeline) Priority() int64                     { return 10 }
func (*testPipeline) Get(string) any                      { return nil }

var _ jobs.Pipeline = (*testPipeline)(nil)

// testQueue is the priority queue the driver inserts consumed jobs into.
type testQueue struct{}

func (*testQueue) Remove(string) []jobs.Job { return nil }
func (*testQueue) Insert(jobs.Job)          {}
func (*testQueue) ExtractMin() jobs.Job     { return nil }
func (*testQueue) Len() uint64              { return 0 }

var _ jobs.Queue = (*testQueue)(nil)

// newTestDriver builds a driver with a kafka client that has no broker and no
// consume configuration. A poll parks until the client is closed.
func newTestDriver(t *testing.T, pipeName string) (*Driver, *recordingHandler, *kgo.Client) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	handler := &recordingHandler{}
	eventBus, _ := events.NewEventBus()

	d := &Driver{
		log:         slog.New(handler),
		pq:          &testQueue{},
		cfg:         &config{},
		tracer:      sdktrace.NewTracerProvider(),
		prop:        propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}),
		eventBus:    eventBus,
		kafkaClient: client,
		recordsCh:   make(chan *kgo.Record, 1),
		requeueCh:   make(chan *Item, 1),
	}

	var pipe jobs.Pipeline = &testPipeline{name: pipeName}
	d.pipeline.Store(&pipe)
	d.listeners.Store(1)

	return d, handler, client
}

// subscribeDriverCommands subscribes a channel to the driver commands on the
// global events bus, the same way the JOBS plugin does. The bus drops an event
// when the channel is full, so the channel has a buffer.
func subscribeDriverCommands(t *testing.T) (*events.Bus, chan events.Event) {
	t.Helper()

	bus, id := events.NewEventBus()
	ch := make(chan events.Event, 10)
	require.NoError(t, bus.SubscribeP(id, fmt.Sprintf("*.%s", events.EventJOBSDriverCommand), ch))
	t.Cleanup(func() { bus.Unsubscribe(id) })

	return bus, ch
}

func startListener(d *Driver) chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- d.listen()
	}()

	return errCh
}

func waitListener(t *testing.T, errCh chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(listenerTestTimeout):
		require.FailNow(t, "the listener did not stop")
		return nil
	}
}

// requireCommand waits for the driver command of the given pipeline. The global
// bus delivers the commands of the other pipelines to the same channel.
func requireCommand(t *testing.T, commands chan events.Event, pipeName string) events.Event {
	t.Helper()

	timeout := time.After(listenerTestTimeout)
	for {
		select {
		case ev := <-commands:
			if ev.Plugin() == pipeName {
				return ev
			}
		case <-timeout:
			require.FailNow(t, "the driver sent no command for the pipeline", pipeName)
			return nil
		}
	}
}

// requireNoCommand drains the channel until the sentinel pipeline arrives and
// fails when a command for the pipeline is seen. The bus delivers the events in
// order, so a sentinel sent after the listener stopped proves the absence.
func requireNoCommand(t *testing.T, commands chan events.Event, pipeName string, sentinel string) {
	t.Helper()

	timeout := time.After(listenerTestTimeout)
	for {
		select {
		case ev := <-commands:
			require.NotEqual(t, pipeName, ev.Plugin(), "the stopped driver sent a command")
			if ev.Plugin() == sentinel {
				return
			}
		case <-timeout:
			require.FailNow(t, "the sentinel command did not arrive")
			return
		}
	}
}

// waitCancelFunc waits until the listener stores the poll cancel function.
func waitCancelFunc(t *testing.T, d *Driver) context.CancelFunc {
	t.Helper()

	for range 1000 {
		d.mu.Lock()
		cancel := d.kafkaCancelCtx
		d.mu.Unlock()

		if cancel != nil {
			return cancel
		}

		time.Sleep(time.Millisecond)
	}

	require.FailNow(t, "the listener stored no cancel function")
	return nil
}

// TestListenerRestartsPipelineOnClientClose asserts that the listener asks the
// JOBS plugin to recreate the pipeline when the kafka client is closed while
// the driver is running.
func TestListenerRestartsPipelineOnClientClose(t *testing.T) {
	const pipeName = "test-restart-on-client-close"

	_, commands := subscribeDriverCommands(t)
	d, _, client := newTestDriver(t, pipeName)

	errCh := startListener(d)
	client.Close()

	require.Error(t, waitListener(t, errCh))

	ev := requireCommand(t, commands, pipeName)
	require.Equal(t, restartStr, ev.Message())
	require.Equal(t, uint32(0), d.listeners.Load())
}

// TestListenerDoesNotRestartStoppedPipeline asserts that a stopped driver sends
// no restart command. The JOBS plugin stops the driver before it destroys or
// restarts the pipeline.
func TestListenerDoesNotRestartStoppedPipeline(t *testing.T) {
	const pipeName = "test-no-restart-when-stopped"
	const sentinel = "test-no-restart-when-stopped-sentinel"

	bus, commands := subscribeDriverCommands(t)
	d, logs, client := newTestDriver(t, pipeName)

	d.stopped.Store(1)

	errCh := startListener(d)
	client.Close()

	require.Error(t, waitListener(t, errCh))

	bus.Send(events.NewEvent(events.EventJOBSDriverCommand, sentinel, restartStr))
	requireNoCommand(t, commands, pipeName, sentinel)

	require.Equal(t, 1, logs.count("driver is stopped, the pipeline restart command was not sent"))
	require.Equal(t, uint32(0), d.listeners.Load())
}

// TestListenerResetsListenersOnContextCancel asserts that the driver reports no
// active listener after the Stop call cancels the poll context.
func TestListenerResetsListenersOnContextCancel(t *testing.T) {
	const pipeName = "test-reset-listeners-on-cancel"

	d, logs, _ := newTestDriver(t, pipeName)

	errCh := startListener(d)
	waitCancelFunc(t, d)()

	require.NoError(t, waitListener(t, errCh))
	require.Equal(t, uint32(0), d.listeners.Load())
	require.Equal(t, 1, logs.count("consumer context canceled, stopping the listener"))
}

// TestListenerStartsOnceAcrossRunPauseResume asserts that Run, Pause and Resume
// share one listener goroutine. Two goroutines that poll the same client and
// both allow rebalances break the block_rebalance_on_poll guarantee.
func TestListenerStartsOnceAcrossRunPauseResume(t *testing.T) {
	const pipeName = "test-single-listener"

	d, logs, client := newTestDriver(t, pipeName)
	ctx := t.Context()
	pipe := *d.pipeline.Load()

	require.NoError(t, d.Run(ctx, pipe))
	require.NoError(t, d.Pause(ctx, pipeName))
	require.NoError(t, d.Resume(ctx, pipeName))

	d.stopped.Store(1)
	client.Close()

	require.Eventually(t, func() bool {
		return logs.count("kafka listener stopped") >= 1
	}, listenerTestTimeout, time.Millisecond*10)
	require.Never(t, func() bool {
		return logs.count("kafka listener stopped") > 1
	}, time.Millisecond*500, time.Millisecond*10)
}
