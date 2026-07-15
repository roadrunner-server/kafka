package helpers

import (
	"net"
	"net/rpc"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()
	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call("jobs.Resume", &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call("jobs.Push", &jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func createDummyJob(pipeline string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call("jobs.Pause", &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		assert.NoError(t, err)
	}
}

func PushToPipeErr(pipeline string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, "127.0.0.1:6001")
		req := &jobsProto.PushRequest{Job: &jobsProto.Job{
			Job:     "some/php/namespace",
			Id:      "1",
			Payload: []byte(`{"hello":"world"}`),
			Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
			Options: &jobsProto.Options{
				Priority:  1,
				Pipeline:  pipeline,
				AutoAck:   true,
				Topic:     pipeline,
				Offset:    0,
				Partition: 0,
			},
		}}
		err := client.Call("jobs.Push", req, &jobsProto.JobsHandlerResponse{})
		assert.Error(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}

		// Retry the destroy 10× with 1s gaps; if all attempts fail, return
		// without asserting. Some negative tests intentionally destroy
		// non-existent pipelines and rely on this silent-after-retry pattern.
		for range 10 {
			err := client.Call("jobs.Destroy", req, &jobsProto.Pipelines{})
			if err == nil {
				return
			}
			time.Sleep(time.Second)
		}
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		resp := &jobsProto.Stats{}
		err := client.Call("jobs.GetStats", &emptypb.Empty{}, resp)
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetStats())

		st := resp.GetStats()[0]
		state.Queue = st.GetQueue()
		state.Pipeline = st.GetPipeline()
		state.Driver = st.GetDriver()
		state.Active = st.GetActive()
		state.Delayed = st.GetDelayed()
		state.Reserved = st.GetReserved()
		state.Ready = st.GetReady()
		state.Priority = st.GetPriority()
	}
}
