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

// TestNackIsANoop records the kafka semantics: an offset that is not committed
// is redelivered by the broker, so a plain nack has nothing to do.
func TestNackIsANoop(t *testing.T) {
	require.NoError(t, newItem(nil, nil).Nack())
}
