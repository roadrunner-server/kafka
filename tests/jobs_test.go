package tests

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"tests/helpers"

	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	kafkaPlugin "github.com/roadrunner-server/kafka/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	initAddr = "127.0.0.1:6001"
	pqAddr   = "127.0.0.1:6002"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&kafkaPlugin.Plugin{},
	}
}

// boot starts the container with the observed logger and waits for the rpc
// listener, which is the readiness signal the fixed sleeps used to stand in for.
func boot(t *testing.T, cfgPath string, addr string, opts ...helpers.Option) (*helpers.RR, func()) {
	t.Helper()

	return helpers.Start(t, cfgPath, jobsPlugins(),
		append([]helpers.Option{
			helpers.WithObservedLogger(),
			helpers.WithTCPProbe(addr),
		}, opts...)...)
}

// TestPushAndProcess follows a batch of jobs through the plain, groupless
// consumer, which polls partitions directly.
func TestPushAndProcess(t *testing.T) {
	const jobCount = 100

	helpers.CleanupTopics(t, "test-1")

	rr, _ := boot(t, "configs/.rr-kafka-init.yaml", initAddr)

	for range jobCount {
		helpers.PushToPipe("test-1", false, initAddr)(t)
	}

	rr.WaitLog(t, "job was processed successfully", jobCount)

	helpers.DestroyPipelines(initAddr, "test-1")(t)

	rr.RequireLogCount(t, "job was pushed successfully", jobCount)
	rr.RequireLogCount(t, "job was processed successfully", jobCount)
}

// TestPushAndProcessCG runs the same batch through a consumer group, whose
// poller commits offsets through the group coordinator instead.
func TestPushAndProcessCG(t *testing.T) {
	const jobCount = 100

	helpers.CleanupTopics(t, "foo")

	rr, _ := boot(t, "configs/.rr-kafka-init-cg.yaml", initAddr)

	for range jobCount {
		helpers.PushToTopic("test-1", "foo", false, initAddr)(t)
	}

	rr.WaitLog(t, "job was processed successfully", jobCount)

	helpers.DestroyPipelines(initAddr, "test-1")(t)

	rr.RequireLogCount(t, "job was pushed successfully", jobCount)
	rr.RequireLogCount(t, "job was processed successfully", jobCount)
}

// TestPriorityQueueBacklog pushes far more jobs than the slow workers can take,
// so most of them sit in the priority queue until the pipeline is destroyed
// under them. The group poller has to come down cleanly with them in flight.
func TestPriorityQueueBacklog(t *testing.T) {
	const jobCount = 100

	helpers.CleanupTopics(t, "foo-pq")

	rr, stop := boot(t, "configs/.rr-kafka-init-pq.yaml", pqAddr)

	for range jobCount {
		helpers.PushToTopic("test-1-pq", "foo-pq", false, pqAddr)(t)
	}

	rr.RequireLogCount(t, "job was pushed successfully", jobCount)

	// the workers have to be busy before the destroy, otherwise the backlog
	// would never form
	rr.WaitLog(t, "job processing was started", 2)

	helpers.DestroyPipelines(pqAddr, "test-1-pq")(t)

	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)

	// the poller writes its stop records while the container shuts down
	stop()
	rr.WaitLog(t, "------> job poller was stopped <------", 1)
	rr.WaitLog(t, "consumer context canceled, stopping the listener", 1)
}

// declareRoundTrip drives a declared pipeline through resume, a push, a pause
// under which pushes accumulate, and a final resume that drains them.
func declareRoundTrip(t *testing.T, rr *helpers.RR, pipeline string, withGroup bool) {
	t.Helper()

	helpers.CleanupTopics(t, pipeline)
	helpers.DeclarePipe(initAddr, pipeline, pipeline, withGroup)(t)
	helpers.ResumePipes(initAddr, pipeline)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PushToPipe(pipeline, false, initAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(initAddr, pipeline)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	// a paused pipeline accepts pushes and leaves them on the topic
	helpers.PushToPipe(pipeline, false, initAddr)(t)
	helpers.PushToPipe(pipeline, false, initAddr)(t)
	rr.RequireLogCount(t, "job was pushed successfully", 3)
	rr.NeverLog(t, "jobs protocol error")

	helpers.ResumePipes(initAddr, pipeline)(t)
	rr.WaitLog(t, "job was processed successfully", 3)

	helpers.DestroyPipelines(initAddr, pipeline)(t)

	rr.RequireLogCount(t, "job was processed successfully", 3)
	rr.RequireLogCount(t, "pipeline was resumed", 2)
	rr.RequireLogCount(t, "pipeline was paused", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestDeclareAndConsume covers a pipeline declared over rpc with the direct
// partition poller. The old test made the same calls and asserted counts only
// after 16 seconds of sleeps.
func TestDeclareAndConsume(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-kafka-declare.yaml", initAddr)

	declareRoundTrip(t, rr, "test-22", false)
}

// TestDeclareAndConsumeCG covers the same round trip through a consumer group.
func TestDeclareAndConsumeCG(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-kafka-declare.yaml", initAddr)

	declareRoundTrip(t, rr, "test-33", true)
}

// TestRequeueRetriesUntilComplete covers the worker that fails a job with a
// growing attempts header and only completes it on the fourth delivery. The
// old test slept out the retries and asserted nothing.
func TestRequeueRetriesUntilComplete(t *testing.T) {
	helpers.CleanupTopics(t, "test-3")

	rr, _ := boot(t, "configs/.rr-kafka-jobs-err.yaml", initAddr)

	helpers.DeclarePipe(initAddr, "test-3", "test-3", false)(t)
	helpers.ResumePipes(initAddr, "test-3")(t)
	helpers.PushToPipe("test-3", false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(initAddr, "test-3")(t)
	helpers.DestroyPipelines(initAddr, "test-3")(t)

	// one original delivery plus the three the worker requeued
	rr.RequireLogCount(t, "job processing was started", 4)
	rr.RequireLogCount(t, "job was processed successfully", 1)
}

// TestPingOk covers the boot-time broker ping with a reachable broker: Serve
// stays up and the pipeline comes up behind it.
func TestPingOk(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-kafka-ping-ok.yaml", initAddr)

	rr.WaitLog(t, "ping kafka: ok", 1)
	rr.WaitLog(t, "pipeline was started", 1)

	helpers.DestroyPipelines(initAddr, "test-1")(t)
}

// TestPingFailed covers the fail-fast path: an unreachable broker has to stop
// Serve rather than leave a dead pipeline running.
func TestPingFailed(t *testing.T) {
	err := helpers.StartExpectServeError(t, "configs/.rr-kafka-ping-failed.yaml", jobsPlugins())

	require.ErrorContains(t, err, "kafka_ping: unable to dial")
}

// TestOTELSpans checks the spans the driver emits around a push and a destroy.
func TestOTELSpans(t *testing.T) {
	tracer := newInMemoryTracer(t)

	helpers.CleanupTopics(t, "foo-bar")

	rr, _ := boot(t, "configs/.rr-kafka-otel.yaml", initAddr, helpers.WithPlugin(tracer))

	for range 3 {
		helpers.PushToTopic("test-1", "foo-bar", false, initAddr)(t)
	}

	rr.WaitLog(t, "job was processed successfully", 3)

	helpers.DestroyPipelines(initAddr, "test-1")(t)

	names := make(map[string]struct{})
	for _, s := range tracer.exp.GetSpans() {
		names[s.Name] = struct{}{}
	}

	got := make([]string, 0, len(names))
	for name := range names {
		got = append(got, name)
	}
	slices.Sort(got)

	for _, want := range []string{
		"destroy_pipeline",
		"jobs_listener",
		"kafka_listener",
		"kafka_push",
		"push",
	} {
		require.Contains(t, got, want, "collected spans: %v", got)
	}
}

// inMemoryTracer stands in for the otel plugin, keeping the spans in process.
type inMemoryTracer struct {
	tp  *sdktrace.TracerProvider
	exp *tracetest.InMemoryExporter
}

func newInMemoryTracer(t *testing.T) *inMemoryTracer {
	t.Helper()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	return &inMemoryTracer{tp: tp, exp: exp}
}

func (*inMemoryTracer) Init() error                        { return nil }
func (*inMemoryTracer) Name() string                       { return "inMemoryTracer" }
func (m *inMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.tp }

var _ = slog.LevelError
