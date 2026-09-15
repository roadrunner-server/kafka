package kafkajobs

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestStopTwiceDoesNotPanic asserts that Stop is idempotent. The JOBS plugin
// calls Stop from the restart handler and from Destroy or the shutdown, and
// the calls can overlap.
func TestStopTwiceDoesNotPanic(t *testing.T) {
	const pipeName = "test-stop-twice"

	d, _, _ := newTestDriver(t, pipeName)

	require.NoError(t, d.Stop(t.Context()))
	require.NotPanics(t, func() {
		require.NoError(t, d.Stop(t.Context()))
	})
}

// TestResumeAfterStopReturnsError asserts that a stopped driver refuses to
// resume. A listener started on a closed client exits with an error at once.
func TestResumeAfterStopReturnsError(t *testing.T) {
	const pipeName = "test-resume-after-stop"

	d, logs, _ := newTestDriver(t, pipeName)
	d.listeners.Store(0)

	require.NoError(t, d.Stop(t.Context()))
	require.Error(t, d.Resume(t.Context(), pipeName))
	require.Equal(t, uint32(0), d.listeners.Load())
	require.Equal(t, 0, logs.count("listener error"))
}

// TestStopEndsDriverGoroutines asserts that Stop ends the listener, the
// handler goroutines and the kafka client. The JOBS plugin builds a new driver
// on every pipeline restart.
func TestStopEndsDriverGoroutines(t *testing.T) {
	const pipeName = "test-stop-ends-goroutines"
	const topic = "test-stop-ends-goroutines"

	cluster := newFakeCluster(t, topic)
	goroutines := runtime.NumGoroutine()

	d, _, pq := newClusterDriver(t, cluster, pipeName, topic, "test-stop-ends-goroutines-group")
	require.NoError(t, d.Run(t.Context(), *d.pipeline.Load()))

	produce(t, cluster, topic, "first")
	require.Eventually(t, func() bool { return pq.consumed("first") }, listenerTestTimeout, time.Millisecond*10)

	require.NoError(t, d.Stop(t.Context()))

	// require.Eventually runs its condition in a goroutine, which adds to the
	// count, so the test polls on its own goroutine.
	deadline := time.Now().Add(listenerTestTimeout)
	for runtime.NumGoroutine() > goroutines {
		require.Less(t, time.Now(), deadline, "the driver goroutines are still running")
		time.Sleep(time.Millisecond * 10)
	}
}

// TestAckAfterStopDoesNotPanic asserts that a job that observed the running
// state before Stop can still call Ack and Requeue after Stop. A send on a
// closed channel panics even inside a select with a default case.
func TestAckAfterStopDoesNotPanic(t *testing.T) {
	const pipeName = "test-ack-after-stop"

	d, _, _ := newTestDriver(t, pipeName)
	// The job loaded stopped == 0 before Stop ran.
	item := fromConsumer(&kgo.Record{}, d.requeueCh, d.recordsCh, &atomic.Uint64{})

	require.NoError(t, d.Stop(t.Context()))

	require.NotPanics(t, func() { _ = item.Ack() })
	require.NotPanics(t, func() { _ = item.Requeue(nil, 0) })
}

// TestPauseIsAtomic asserts that two concurrent Pause calls pause the pipeline
// once. A second decrement wraps the listener counter and reports a paused
// pipeline as ready.
func TestPauseIsAtomic(t *testing.T) {
	const pipeName = "test-pause-atomic"

	d, _, _ := newTestDriver(t, pipeName)
	ctx := t.Context()

	for range 2000 {
		d.listeners.Store(1)

		start := make(chan struct{})
		var paused atomic.Int32
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				<-start
				if d.Pause(ctx, pipeName) == nil {
					paused.Add(1)
				}
			})
		}
		close(start)
		wg.Wait()

		require.Equal(t, int32(1), paused.Load())
		require.Equal(t, uint32(0), d.listeners.Load())
	}
}

// TestFromPipelineClosesClientOnPingFailure asserts that a constructor that
// fails the ping closes the kafka client it created. The JOBS plugin runs the
// constructor again on every pipeline restart.
func TestFromPipelineClosesClientOnPingFailure(t *testing.T) {
	const pipeName = "test-ping-failure"

	conf := config{
		Brokers: []string{"127.0.0.1:1"},
		Ping:    &Ping{Timeout: time.Millisecond * 100},
	}
	var pipe jobs.Pipeline = &testPipeline{name: pipeName}

	goroutines := runtime.NumGoroutine()

	_, err := FromPipeline(t.Context(), nil, pipe, slog.New(&recordingHandler{}), &testConfigurer{conf: conf}, &testQueue{})
	require.Error(t, err)

	// require.Eventually runs its condition in a goroutine, which adds to the
	// count, so the test polls on its own goroutine.
	deadline := time.Now().Add(listenerTestTimeout)
	for runtime.NumGoroutine() > goroutines {
		require.Less(t, time.Now(), deadline, "the kafka client goroutines are still running")
		time.Sleep(time.Millisecond * 10)
	}
}

// TestStopReturnsWithinContext asserts that Stop returns when its context
// expires while the broker does not answer the group leave. The JOBS plugin
// handles the driver commands one at a time, so a blocked Stop blocks the
// commands of every pipeline.
func TestStopReturnsWithinContext(t *testing.T) {
	const pipeName = "test-stop-bounded"
	const topic = "test-stop-bounded"

	cluster := newFakeCluster(t, topic)
	d, _, pq := newClusterDriver(t, cluster, pipeName, topic, "test-stop-bounded-group")

	require.NoError(t, d.Run(t.Context(), *d.pipeline.Load()))

	produce(t, cluster, topic, "first")
	require.Eventually(t, func() bool { return pq.consumed("first") }, listenerTestTimeout, time.Millisecond*10)

	release := make(chan struct{})
	holdRequests(cluster, kmsg.LeaveGroup, release)
	holdRequests(cluster, kmsg.OffsetCommit, release)
	// The hold ends after the assertion window. A Stop that ignores the
	// context then returns late and the test reports the failure.
	time.AfterFunc(time.Second*15, func() { close(release) })

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	start := time.Now()
	require.NoError(t, d.Stop(ctx))
	require.Less(t, time.Since(start), time.Second*5)
}

// TestFromPipelineRejectsEmptyName asserts that a pipeline declared without a
// name is rejected. The JOBS plugin validates only the driver of a declared
// pipeline, and events.NewEvent returns a typed nil event for an empty name.
func TestFromPipelineRejectsEmptyName(t *testing.T) {
	var pipe jobs.Pipeline = &testPipeline{}

	d, err := FromPipeline(t.Context(), nil, pipe, slog.New(&recordingHandler{}), &testConfigurer{}, &testQueue{})
	require.Nil(t, d)
	require.ErrorContains(t, err, "pipeline name is required")
}
