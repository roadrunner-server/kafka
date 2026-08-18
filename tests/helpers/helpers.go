package helpers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BrokerAddr is the plaintext listener the compose file publishes.
const BrokerAddr = "127.0.0.1:9092"

// CleanupTopics drops the topics a test produces to, both before it runs and
// after it finishes, so a consumer reading from the earliest offset cannot
// inherit records left behind by an earlier or an interrupted run.
func CleanupTopics(t *testing.T, topics ...string) {
	t.Helper()

	deleteTopics(t, topics...)
	t.Cleanup(func() { deleteTopics(t, topics...) })
}

func deleteTopics(t *testing.T, topics ...string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(BrokerAddr))
	require.NoError(t, err)
	defer client.Close()

	// a topic the run never created reports itself as unknown, which is fine
	resp, err := kadm.NewClient(client).DeleteTopics(context.Background(), topics...)
	require.NoError(t, err)

	for _, r := range resp {
		if r.Err != nil && !errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			t.Fatalf("delete topic %s: %v", r.Topic, r.Err)
		}
	}
}

const (
	// redialTimeout bounds PushEventually, which retries across a broker outage.
	redialTimeout = time.Second * 120
	redialTick    = time.Second
)

func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Resume",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.JobsHandlerResponse{}))
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Pause",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.JobsHandlerResponse{}))
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Destroy",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Pipelines{}))
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return PushToTopic(pipeline, pipeline, autoAck, address)
}

// PushToTopic pushes to a pipeline whose consumed topic differs from its name.
// The driver produces to the topic carried by the job, so a mismatch lands the
// job in an auto-created topic nobody reads.
func PushToTopic(pipeline string, topic string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, topic, autoAck)},
			&jobsProto.JobsHandlerResponse{}))
	}
}

// PushExpectError pushes to a pipeline whose broker is down and requires the
// call to fail, so an outage is not silently swallowed.
func PushExpectError(address string, pipeline string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.Error(t, client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, pipeline, false)},
			&jobsProto.JobsHandlerResponse{}))
	}
}

// PushEventually keeps retrying a push until it lands. Used after a broker
// outage, where the producer needs a while to reconnect.
func PushEventually(t *testing.T, address string, pipeline string) {
	t.Helper()

	require.Eventually(t, func() bool {
		client := NewJobsClient(t, address)

		return client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, pipeline, false)},
			&jobsProto.JobsHandlerResponse{}) == nil
	}, redialTimeout, redialTick, "the producer never recovered after the outage")
}

func dummyJob(pipeline string, topic string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    topic,
		},
	}
}

// DeclarePipe declares a pipeline over rpc, consuming topic from the earliest
// offset. withGroup adds a consumer group, which switches the driver to the
// group poller.
func DeclarePipe(address string, pipeline string, topic string, withGroup bool) func(t *testing.T) {
	return func(t *testing.T) {
		options := map[string]string{
			"driver":                    "kafka",
			"name":                      pipeline,
			"priority":                  "3",
			"auto_create_topics_enable": "true",
			"producer_options": `{
				"max_message_bytes": 1000,
				"required_acks": "LeaderAck",
				"compression_codec": "snappy",
				"disable_idempotent": true
			}`,
			"consumer_options": fmt.Sprintf(`{"topics": ["%s"], "consumer_offset": {"type": "AtStart"}}`, topic),
		}

		if withGroup {
			options["group_options"] = `{
				"group_id": "foo-bar",
				"block_rebalance_on_poll": true
			}`
		}

		require.NoError(t, NewJobsClient(t, address).Call("jobs.Declare",
			&jobsProto.DeclareRequest{Pipeline: options},
			&jobsProto.JobsHandlerResponse{}))
	}
}
