package kafkajobs

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// testMessage is the jobs.Message the jobs plugin hands to Push.
type testMessage struct {
	name      string
	id        string
	payload   []byte
	headers   map[string][]string
	priority  int64
	groupID   string
	topic     string
	metadata  string
	partition int32
	offset    int64
}

func (m *testMessage) ID() string                   { return m.id }
func (m *testMessage) GroupID() string              { return m.groupID }
func (m *testMessage) Priority() int64              { return m.priority }
func (m *testMessage) Name() string                 { return m.name }
func (m *testMessage) Payload() []byte              { return m.payload }
func (*testMessage) Delay() int64                   { return 0 }
func (*testMessage) AutoAck() bool                  { return false }
func (m *testMessage) Headers() map[string][]string { return m.headers }
func (m *testMessage) UpdatePriority(p int64)       { m.priority = p }
func (m *testMessage) Offset() int64                { return m.offset }
func (m *testMessage) Partition() int32             { return m.partition }
func (m *testMessage) Topic() string                { return m.topic }
func (m *testMessage) Metadata() string             { return m.metadata }

var _ jobs.Message = (*testMessage)(nil)

// TestFromJob covers the kafka specific message fields the driver carries
// through: topic, partition, offset and metadata.
func TestFromJob(t *testing.T) {
	item := fromJob(&testMessage{
		name:      "some/php/namespace",
		id:        "job-id",
		payload:   []byte(`{"hello":"world"}`),
		headers:   map[string][]string{"test": {"test2"}},
		priority:  3,
		groupID:   "test-1",
		topic:     "foo",
		metadata:  "meta",
		partition: 2,
		offset:    42,
	})

	require.Equal(t, "job-id", item.ID())
	require.Equal(t, "test-1", item.GroupID())
	require.Equal(t, int64(3), item.Priority())
	require.Equal(t, []byte(`{"hello":"world"}`), item.Body())
	require.Equal(t, "foo", item.Options.Queue)
	require.Equal(t, "meta", item.Options.Metadata)
	require.Equal(t, int32(2), item.Options.Partition)
	require.Equal(t, int64(42), item.Options.Offset)
}

func TestItemContext(t *testing.T) {
	item := &Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		headers: map[string][]string{"test": {"test2"}},
		Options: &Options{Pipeline: "test-1", Queue: "foo", Partition: 2, Offset: 42},
	}

	data, err := item.Context()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, "job-id", got["id"])
	require.Equal(t, "kafka", got["driver"])
	require.Equal(t, "test-1", got["pipeline"])
	require.Equal(t, float64(2), got["partition"])
	require.Equal(t, float64(42), got["offset"])
}

// queue stands in for the jobs priority queue. Insert blocks until the test
// receives the item, so an insert on the caller goroutine deadlocks the
// synctest bubble.
type queue chan jobs.Job

func (q queue) Insert(item jobs.Job)   { q <- item }
func (queue) Remove(string) []jobs.Job { return nil }
func (queue) ExtractMin() jobs.Job     { return nil }
func (queue) Len() uint64              { return 0 }

// newItem wires an item to a commit channel and a queue standing in for the
// listener.
func newItem(commits chan *kgo.Record, pq jobs.Queue) *Item {
	return &Item{
		Ident:     "job-id",
		headers:   map[string][]string{},
		Options:   &Options{Pipeline: "test-1"},
		stopped:   &atomic.Uint64{},
		record:    &kgo.Record{Topic: "foo"},
		commitsCh: commits,
		pq:        pq,
	}
}

// TestAckCommitsTheRecord checks an ack hands the consumed record back to the
// listener for the offset commit.
func TestAckCommitsTheRecord(t *testing.T) {
	commits := make(chan *kgo.Record, 1)
	item := newItem(commits, nil)

	require.NoError(t, item.Ack())
	require.Same(t, item.record, <-commits)
}

// TestStoppedPipelineRejectsReply covers the guard that keeps a late worker
// reply from touching a consumer the driver has already torn down.
func TestStoppedPipelineRejectsReply(t *testing.T) {
	stopped := func() *Item {
		i := newItem(make(chan *kgo.Record, 1), make(queue))
		i.stopped.Store(1)
		return i
	}

	require.ErrorContains(t, stopped().Ack(), "the pipeline is probably stopped")
	require.ErrorContains(t, stopped().NackWithOptions(true, 0), "the pipeline is probably stopped")
	require.ErrorContains(t, stopped().Requeue(nil, 0), "the pipeline is probably stopped")
}

// TestAckOnFullChannel covers the shutdown race: with nobody draining the
// commit channel, the reply fails instead of blocking a worker forever.
func TestAckOnFullChannel(t *testing.T) {
	commits := make(chan *kgo.Record)

	require.ErrorContains(t, newItem(commits, nil).Ack(), "the pipeline is probably stopped")
}

// TestRetryStaysInThePipeline covers roadrunner#2413: a retry goes back into
// the pipeline priority queue after the requested delay, with the merged
// headers. The same item keeps the consumed record, so its final ack commits
// the original offset.
func TestRetryStaysInThePipeline(t *testing.T) {
	tests := []struct {
		name  string
		retry func(*Item) error
		// stopAfter stops the pipeline this long after the retry, 0 keeps it running
		stopAfter   time.Duration
		wantInsert  bool
		wantDelay   time.Duration
		wantHeaders map[string][]string
	}{
		{
			name:        "requeue without delay",
			retry:       func(i *Item) error { return i.Requeue(map[string][]string{"attempts": {"2"}}, 0) },
			wantInsert:  true,
			wantHeaders: map[string][]string{"attempts": {"2"}, "keep": {"x"}},
		},
		{
			name:        "requeue with delay",
			retry:       func(i *Item) error { return i.Requeue(map[string][]string{"attempts": {"2"}}, 10) },
			wantInsert:  true,
			wantDelay:   10 * time.Second,
			wantHeaders: map[string][]string{"attempts": {"2"}, "keep": {"x"}},
		},
		{
			name:        "nack with requeue and delay",
			retry:       func(i *Item) error { return i.NackWithOptions(true, 10) },
			wantInsert:  true,
			wantDelay:   10 * time.Second,
			wantHeaders: map[string][]string{"attempts": {"1"}, "keep": {"x"}},
		},
		{
			name:  "nack without requeue",
			retry: func(i *Item) error { return i.NackWithOptions(false, 10) },
		},
		{
			name:      "pipeline stopped during the delay",
			retry:     func(i *Item) error { return i.Requeue(nil, 10) },
			stopAfter: 5 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := make(queue)
				item := newItem(nil, q)
				item.headers = map[string][]string{"attempts": {"1"}, "keep": {"x"}}
				start := time.Now()

				require.NoError(t, tc.retry(item))

				if tc.stopAfter > 0 {
					time.Sleep(tc.stopAfter)
					item.stopped.Store(1)
				}

				select {
				case got := <-q:
					require.True(t, tc.wantInsert, "unexpected retry")
					require.Same(t, item, got)
					require.Equal(t, tc.wantDelay, time.Since(start))
					require.Equal(t, tc.wantHeaders, item.headers)
				case <-time.After(time.Hour):
					require.False(t, tc.wantInsert, "no retry within an hour")
				}
			})
		})
	}
}

// TestNackIsANoop records the FanOut semantics: a plain nack has no gate to
// open and nothing to commit.
func TestNackIsANoop(t *testing.T) {
	require.NoError(t, newItem(nil, nil).Nack())
}

// newSerialItem is newItem with the gate the listener sets in Serial mode.
func newSerialItem(commits chan *kgo.Record, pq jobs.Queue) *Item {
	item := newItem(commits, pq)
	item.done = make(chan struct{})
	return item
}

// released reports whether the serial gate of item is open. A nil gate (FanOut
// mode) is never open.
func released(item *Item) bool {
	select {
	case <-item.done:
		return true
	default:
		return false
	}
}

// TestSerialGate covers which worker replies open the per-partition gate of a
// Serial pipeline. Only a reply that settles the record opens it; a requeue
// keeps the partition blocked until the retry is settled.
func TestSerialGate(t *testing.T) {
	tests := []struct {
		name string
		// reply is the call the jobs plugin makes for the worker reply
		reply func(*Item) error
		// drain gives the commit channel room for the ack; false leaves nobody reading it
		drain bool
		// stop marks the pipeline stopped before the reply
		stop         bool
		wantErr      string
		wantReleased bool
	}{
		{name: "ack", reply: (*Item).Ack, drain: true, wantReleased: true},
		{name: "ack on a full commit channel", reply: (*Item).Ack, wantErr: "the pipeline is probably stopped"},
		{name: "ack on a stopped pipeline", reply: (*Item).Ack, drain: true, stop: true, wantErr: "the pipeline is probably stopped"},
		{name: "nack", reply: (*Item).Nack, wantReleased: true},
		{name: "nack without requeue", reply: func(i *Item) error { return i.NackWithOptions(false, 0) }, wantReleased: true},
		{name: "nack with requeue", reply: func(i *Item) error { return i.NackWithOptions(true, 0) }},
		{name: "requeue", reply: func(i *Item) error { return i.Requeue(nil, 0) }},
		{name: "requeue on a stopped pipeline", reply: func(i *Item) error { return i.Requeue(nil, 0) }, stop: true, wantErr: "the pipeline is probably stopped"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			commits := make(chan *kgo.Record)
			if tc.drain {
				commits = make(chan *kgo.Record, 1)
			}

			// the buffer takes the re-insert of a requeue without a reader
			item := newSerialItem(commits, make(queue, 1))
			if tc.stop {
				item.stopped.Store(1)
			}

			err := tc.reply(item)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.wantReleased, released(item))
		})
	}
}

// TestSerialGateOpensAfterTheRetry covers the retry cycle of a Serial
// pipeline: the requeued item comes back from the queue after the delay and
// its ack opens the gate. A later duplicate reply is harmless.
func TestSerialGateOpensAfterTheRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := make(queue)
		commits := make(chan *kgo.Record, 1)
		item := newSerialItem(commits, q)
		start := time.Now()

		require.NoError(t, item.NackWithOptions(true, 10))
		require.False(t, released(item), "gate open before the retry")

		retry := <-q
		require.Same(t, item, retry)
		require.Equal(t, 10*time.Second, time.Since(start))
		require.False(t, released(item), "gate open before the retry reply")

		require.NoError(t, retry.Ack())
		require.True(t, released(item))

		require.NoError(t, item.Nack())
		require.True(t, released(item))
	})
}

// TestFanOutItemHasNoGate records that FanOut items carry a nil gate and every
// reply path tolerates it.
func TestFanOutItemHasNoGate(t *testing.T) {
	item := newItem(make(chan *kgo.Record, 1), make(queue, 1))

	require.NoError(t, item.Nack())
	require.NoError(t, item.NackWithOptions(false, 0))
	require.NoError(t, item.Ack())
}
