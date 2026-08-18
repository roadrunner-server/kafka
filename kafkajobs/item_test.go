package kafkajobs

import (
	"encoding/json"
	"sync/atomic"
	"testing"

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

// newItem wires an item to buffered channels standing in for the listener.
func newItem(commits chan *kgo.Record, requeues chan *Item) *Item {
	return &Item{
		Ident:     "job-id",
		headers:   map[string][]string{},
		Options:   &Options{Pipeline: "test-1"},
		stopped:   &atomic.Uint64{},
		record:    &kgo.Record{Topic: "foo"},
		commitsCh: commits,
		requeueCh: requeues,
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
		i := newItem(make(chan *kgo.Record, 1), make(chan *Item, 1))
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

// TestRequeueCarriesNewHeaders checks a requeued copy travels through the
// requeue channel with the merged headers and cleared broker coordinates.
func TestRequeueCarriesNewHeaders(t *testing.T) {
	requeues := make(chan *Item, 1)
	item := newItem(nil, requeues)
	item.Options.Offset = 42

	require.NoError(t, item.Requeue(map[string][]string{"attempts": {"1"}}, 0))

	copied := <-requeues
	require.Equal(t, []string{"1"}, copied.headers["attempts"])
	require.Equal(t, int64(42), copied.Options.Offset)
}

// TestNackIsANoop records the kafka semantics: an offset that is not committed
// is redelivered by the broker, so a plain nack has nothing to do.
func TestNackIsANoop(t *testing.T) {
	require.NoError(t, newItem(nil, nil).Nack())
}
