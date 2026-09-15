package kafkajobs

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/events"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// testQueue is the priority queue the driver inserts consumed jobs into. It
// records the payloads of the inserted jobs.
type testQueue struct {
	mu       sync.Mutex
	payloads []string
}

func (*testQueue) Remove(string) []jobs.Job { return nil }

func (q *testQueue) Insert(job jobs.Job) {
	q.mu.Lock()
	q.payloads = append(q.payloads, string(job.Body()))
	q.mu.Unlock()
}

func (*testQueue) ExtractMin() jobs.Job { return nil }
func (*testQueue) Len() uint64          { return 0 }

// consumed reports whether a job with the payload was inserted.
func (q *testQueue) consumed(payload string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return slices.Contains(q.payloads, payload)
}

var _ jobs.Queue = (*testQueue)(nil)

// testConfigurer supplies the global kafka configuration to the driver
// constructors.
type testConfigurer struct {
	conf config
}

func (*testConfigurer) Has(name string) bool { return name == pluginName }

func (c *testConfigurer) UnmarshalKey(_ string, out any) error {
	*out.(*config) = c.conf

	return nil
}

// newTestDriver builds a driver with a kafka client that has no broker and no
// consume configuration. A poll blocks until the client is closed.
func newTestDriver(t *testing.T, pipeName string) (*Driver, *recordingHandler, *kgo.Client) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	handler := &recordingHandler{}
	eventBus, _ := events.NewEventBus()

	d := &Driver{
		log:           slog.New(handler),
		pq:            &testQueue{},
		cfg:           &config{},
		tracer:        sdktrace.NewTracerProvider(),
		prop:          propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}),
		eventBus:      eventBus,
		kafkaClient:   client,
		recordsCh:     make(chan *kgo.Record, 1),
		requeueCh:     make(chan *Item, 1),
		done:          make(chan struct{}),
		restartDelay:  time.Millisecond,
		restartResend: time.Millisecond,
	}

	var pipe jobs.Pipeline = &testPipeline{name: pipeName}
	d.pipeline.Store(&pipe)
	d.listeners.Store(1)

	return d, handler, client
}

// newFakeCluster starts an in-process kafka cluster with one broker and one
// topic with one partition.
func newFakeCluster(t *testing.T, topic string) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	return cluster
}

// newClusterDriver builds a driver through FromPipeline against the fake
// cluster. The driver consumes the topic. An empty group builds a direct
// consumer.
func newClusterDriver(t *testing.T, cluster *kfake.Cluster, pipeName string, topic string, group string) (*Driver, *recordingHandler, *testQueue) {
	t.Helper()

	conf := config{
		Brokers:      cluster.ListenAddrs(),
		Ping:         &Ping{Timeout: time.Second * 5},
		ConsumerOpts: &ConsumerOpts{Topics: []string{topic}},
	}
	if group != "" {
		conf.GroupOpts = &GroupOptions{GroupID: group, BlockRebalanceOnPoll: true}
	}

	handler := &recordingHandler{}
	pq := &testQueue{}
	var pipe jobs.Pipeline = &testPipeline{name: pipeName}

	d, err := FromPipeline(t.Context(), nil, pipe, slog.New(handler), &testConfigurer{conf: conf}, pq)
	require.NoError(t, err)

	d.restartDelay = time.Millisecond
	d.restartResend = time.Millisecond

	return d, handler, pq
}

// stopDriver returns a function that stops the driver once. The test and its
// cleanup share the function.
func stopDriver(t *testing.T, d *Driver) func() {
	t.Helper()

	stop := sync.OnceFunc(func() { _ = d.Stop(context.Background()) })
	t.Cleanup(stop)

	return stop
}

// produce writes one record to the topic of the fake cluster.
func produce(t *testing.T, cluster *kfake.Cluster, topic string, value string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(topic))
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.ProduceSync(t.Context(), kgo.StringRecord(value)).FirstErr())
}

// failFetches answers every fetch with a non-retriable authorization error.
func failFetches(cluster *kfake.Cluster) {
	cluster.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()

		freq := req.(*kmsg.FetchRequest)
		resp := freq.ResponseKind().(*kmsg.FetchResponse)

		for _, topic := range freq.Topics {
			rt := kmsg.NewFetchResponseTopic()
			rt.Topic = topic.Topic
			rt.TopicID = topic.TopicID

			for _, partition := range topic.Partitions {
				rp := kmsg.NewFetchResponseTopicPartition()
				rp.Partition = partition.Partition
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				rt.Partitions = append(rt.Partitions, rp)
			}

			resp.Topics = append(resp.Topics, rt)
		}

		return resp, nil, true
	})
}

// loseGroupSession answers the next heartbeat with UNKNOWN_MEMBER_ID.
func loseGroupSession(cluster *kfake.Cluster) {
	cluster.ControlKey(int16(kmsg.Heartbeat), func(req kmsg.Request) (kmsg.Response, error, bool) {
		resp := req.(*kmsg.HeartbeatRequest).ResponseKind().(*kmsg.HeartbeatResponse)
		resp.ErrorCode = kerr.UnknownMemberID.Code

		return resp, nil, true
	})
}

// holdRequests makes the cluster hold every request with the key until
// release is closed.
func holdRequests(cluster *kfake.Cluster, key kmsg.Key, release <-chan struct{}) {
	cluster.ControlKey(int16(key), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		cluster.SleepControl(func() { <-release })

		return nil, nil, false
	})
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

func runListen(d *Driver) chan error {
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

	var cancel context.CancelFunc
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()

		cancel = d.kafkaCancelCtx

		return cancel != nil
	}, listenerTestTimeout, time.Millisecond)

	return cancel
}

// waitLog waits until the message was logged at least n times.
func waitLog(t *testing.T, logs *recordingHandler, message string, n int) {
	t.Helper()

	require.Eventually(t, func() bool {
		return logs.count(message) >= n
	}, listenerTestTimeout, time.Millisecond*10, "the message %q was not logged %d times", message, n)
}

// TestListenerRequestsRestartOnNonRetriableError asserts that the listener
// stops and asks the JOBS plugin to recreate the pipeline when a fetch fails
// with a non-retriable error.
func TestListenerRequestsRestartOnNonRetriableError(t *testing.T) {
	const pipeName = "test-restart-on-fatal-error"
	const topic = "test-restart-on-fatal-error"

	_, commands := subscribeDriverCommands(t)
	cluster := newFakeCluster(t, topic)
	d, logs, _ := newClusterDriver(t, cluster, pipeName, topic, "")
	stop := stopDriver(t, d)

	failFetches(cluster)
	require.NoError(t, d.Run(t.Context(), *d.pipeline.Load()))

	ev := requireCommand(t, commands, pipeName)
	require.Equal(t, restartStr, ev.Message())
	require.Equal(t, uint32(0), d.listeners.Load())
	require.Equal(t, 1, logs.count("non-recoverable consumer error"))
	waitLog(t, logs, "kafka listener stopped", 1)
	require.False(t, d.listening.Load())

	stop()
}

// TestListenerDoesNotRestartStoppedPipeline asserts that a driver stopped while
// it waits to send the restart command sends no command. The JOBS plugin stops
// the driver before it destroys or restarts the pipeline. A Resume during the
// wait is refused, so the driver never reports a ready pipeline that nothing
// polls.
func TestListenerDoesNotRestartStoppedPipeline(t *testing.T) {
	const pipeName = "test-no-restart-when-stopped"
	const topic = "test-no-restart-when-stopped"
	const sentinel = "test-no-restart-when-stopped-sentinel"

	bus, commands := subscribeDriverCommands(t)
	cluster := newFakeCluster(t, topic)
	d, logs, _ := newClusterDriver(t, cluster, pipeName, topic, "")
	stop := stopDriver(t, d)
	// The delay is longer than the test timeout. A wait that Stop does not
	// interrupt fails the test.
	d.restartDelay = time.Minute

	failFetches(cluster)
	require.NoError(t, d.Run(t.Context(), *d.pipeline.Load()))
	waitLog(t, logs, "non-recoverable consumer error", 1)
	waitLog(t, logs, "kafka listener stopped", 1)

	require.Error(t, d.Resume(t.Context(), pipeName))
	state, err := d.State(t.Context())
	require.NoError(t, err)
	require.False(t, state.Ready)

	stop()

	bus.Send(events.NewEvent(events.EventJOBSDriverCommand, sentinel, restartStr))
	requireNoCommand(t, commands, pipeName, sentinel)

	waitLog(t, logs, "driver is stopped, the pipeline restart command was not sent", 1)
	require.Equal(t, uint32(0), d.listeners.Load())
}

// TestRequestRestartRepeatsUntilStopped asserts that the restart command is
// sent again until the JOBS plugin stops the driver. The JOBS plugin reads the
// commands from a channel with one slot, and the bus drops a command when the
// slot is full.
func TestRequestRestartRepeatsUntilStopped(t *testing.T) {
	const pipeName = "test-restart-repeats"

	_, commands := subscribeDriverCommands(t)
	d, logs, _ := newTestDriver(t, pipeName)

	go d.requestRestart()

	requireCommand(t, commands, pipeName)
	requireCommand(t, commands, pipeName)

	require.NoError(t, d.Stop(t.Context()))
	waitLog(t, logs, "driver is stopped, the pipeline restart command was not sent", 1)
}

// TestListenerContinuesAfterGroupSessionLoss asserts that a lost group session
// does not stop the listener. The kafka client rejoins the group on its own,
// and a pipeline restart would drop the queued jobs for nothing.
func TestListenerContinuesAfterGroupSessionLoss(t *testing.T) {
	const pipeName = "test-continue-after-session-loss"
	const topic = "test-continue-after-session-loss"
	const sentinel = "test-continue-after-session-loss-sentinel"

	bus, commands := subscribeDriverCommands(t)
	cluster := newFakeCluster(t, topic)
	d, logs, pq := newClusterDriver(t, cluster, pipeName, topic, "test-continue-after-session-loss-group")
	stop := stopDriver(t, d)

	require.NoError(t, d.Run(t.Context(), *d.pipeline.Load()))

	produce(t, cluster, topic, "first")
	require.Eventually(t, func() bool { return pq.consumed("first") }, listenerTestTimeout, time.Millisecond*10)

	loseGroupSession(cluster)
	waitLog(t, logs, "group session was lost, the consumer rejoins the group", 1)

	produce(t, cluster, topic, "second")
	require.Eventually(t, func() bool { return pq.consumed("second") }, listenerTestTimeout, time.Millisecond*10)
	require.Equal(t, uint32(1), d.listeners.Load())

	bus.Send(events.NewEvent(events.EventJOBSDriverCommand, sentinel, restartStr))
	requireNoCommand(t, commands, pipeName, sentinel)

	stop()
}

// TestGroupRejoins asserts which lost group sessions the kafka client restores
// on its own. The other group errors restart the pipeline.
func TestGroupRejoins(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		rejoins bool
	}{
		{name: "unknown member", err: &kgo.ErrGroupSession{Err: kerr.UnknownMemberID}, rejoins: true},
		{name: "illegal generation", err: &kgo.ErrGroupSession{Err: kerr.IllegalGeneration}, rejoins: true},
		{name: "group authorization", err: &kgo.ErrGroupSession{Err: kerr.GroupAuthorizationFailed}, rejoins: false},
		{name: "fenced instance", err: &kgo.ErrGroupSession{Err: kerr.FencedInstanceID}, rejoins: false},
		{name: "plain fetch error", err: kerr.UnknownMemberID, rejoins: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.rejoins, groupRejoins(c.err))
		})
	}
}

// TestListenerResetsListenersOnContextCancel asserts that the driver reports no
// active listener after the Stop call cancels the poll context.
func TestListenerResetsListenersOnContextCancel(t *testing.T) {
	const pipeName = "test-reset-listeners-on-cancel"

	d, logs, _ := newTestDriver(t, pipeName)

	errCh := runListen(d)
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

// TestResumeStartsListenerAfterExit asserts that Resume starts a new listener
// when the previous listener goroutine has exited. A driver that reports Ready
// must have a goroutine that polls.
func TestResumeStartsListenerAfterExit(t *testing.T) {
	const pipeName = "test-resume-after-exit"

	d, logs, _ := newTestDriver(t, pipeName)
	ctx := t.Context()

	require.NoError(t, d.Run(ctx, *d.pipeline.Load()))
	waitCancelFunc(t, d)()
	waitLog(t, logs, "kafka listener stopped", 1)
	require.Equal(t, uint32(0), d.listeners.Load())

	d.mu.Lock()
	d.kafkaCancelCtx = nil
	d.mu.Unlock()

	require.NoError(t, d.Resume(ctx, pipeName))
	waitCancelFunc(t, d)()
	waitLog(t, logs, "kafka listener stopped", 2)
}
