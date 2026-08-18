package tests

import (
	"context"
	"io"
	"testing"
	"time"

	"tests/helpers"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

const durabilityAddr = "127.0.0.1:6001"

// broker drives a kafka container over the docker api, so the durability tests
// can kill it under a running pipeline and bring it back. It binds the same
// 9092 the compose broker uses, which is why these tests run in their own CI
// job without the compose stack.
type broker struct {
	cli *client.Client
	id  string
}

func startBroker(t *testing.T) *broker {
	t.Helper()

	ctx := t.Context()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	const networkName = "rr-e2e-tests"
	_ = cli.NetworkRemove(ctx, networkName)
	_, err = cli.NetworkCreate(ctx, networkName, network.CreateOptions{})
	require.NoError(t, err)

	pull, err := cli.ImagePull(ctx, "confluentinc/cp-kafka:8.1.1", image.PullOptions{})
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, pull)
	_ = pull.Close()

	k, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "confluentinc/cp-kafka:8.1.1",
		Env: []string{
			"KAFKA_NODE_ID=1",
			"KAFKA_PROCESS_ROLES=broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS=1@broker:29093",
			"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://broker:29093,PLAINTEXT_INTERNAL://broker:29092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092,PLAINTEXT_INTERNAL://broker:29092",
			"KAFKA_LOG_DIRS=/tmp/kraft-combined-logs",
			"CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
			"AUTO_CREATE_TOPICS_ENABLE=true",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		},
	}, &container.HostConfig{
		NetworkMode: container.NetworkMode(networkName),
		PortBindings: nat.PortMap{
			"9092/tcp": {nat.PortBinding{HostIP: "127.0.0.1", HostPort: "9092"}},
		},
	}, &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{networkName: {}},
	}, nil, "broker")
	require.NoError(t, err)

	b := &broker{cli: cli, id: k.ID}
	require.NoError(t, cli.ContainerStart(ctx, k.ID, container.StartOptions{}))

	t.Cleanup(func() {
		bg := context.Background()
		timeout := 10
		_ = cli.ContainerStop(bg, k.ID, container.StopOptions{Signal: "SIGKILL", Timeout: &timeout})
		_ = cli.ContainerRemove(bg, k.ID, container.RemoveOptions{RemoveVolumes: true, Force: true})
		_ = cli.NetworkRemove(bg, networkName)
	})

	// the driver pings at boot and fails Serve when the broker is not ready,
	// so wait for the listener, not just the container state
	waitBrokerUp(t)

	return b
}

func waitBrokerUp(t *testing.T) {
	t.Helper()

	// an open socket is not enough, kafka accepts and drops connections while
	// the quorum forms, so ask it to answer a real request
	require.Eventually(t, func() bool {
		cl, errC := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:9092"))
		if errC != nil {
			return false
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		return cl.Ping(ctx) == nil
	}, time.Minute*2, time.Millisecond*500, "broker never answered a ping")
}

func (b *broker) kill(t *testing.T) {
	t.Helper()

	timeout := 10
	require.NoError(t, b.cli.ContainerStop(context.Background(), b.id, container.StopOptions{
		Signal:  "SIGKILL",
		Timeout: &timeout,
	}))
}

func (b *broker) start(t *testing.T) {
	t.Helper()

	require.NoError(t, b.cli.ContainerStart(context.Background(), b.id, container.StartOptions{}))
	waitBrokerUp(t)
}

// redialRoundTrip kills the broker under a running pipeline, checks a push
// fails while it is down, and follows a push through once it is back. The old
// tests made the same calls behind 55 seconds of sleeps and asserted only
// counts of started pipelines.
func redialRoundTrip(t *testing.T, cfgPath string, pipes ...string) {
	t.Helper()

	b := startBroker(t)

	rr, _ := helpers.Start(t, cfgPath, jobsPlugins(),
		helpers.WithObservedLogger(),
		helpers.WithTCPProbe(durabilityAddr),
	)

	rr.WaitLog(t, "pipeline was started", len(pipes))

	b.kill(t)

	// with the broker gone, the push has to fail rather than pretend
	for _, p := range pipes {
		helpers.PushExpectError(durabilityAddr, p)(t)
	}
	rr.WaitLog(t, "job push error", len(pipes))

	b.start(t)

	// the producer has to reconnect before these land
	for _, p := range pipes {
		helpers.PushEventually(t, durabilityAddr, p)
	}

	rr.WaitLog(t, "job was processed successfully", len(pipes))

	helpers.DestroyPipelines(durabilityAddr, pipes...)(t)

	rr.RequireLogCount(t, "pipeline was stopped", len(pipes))
}

func TestDurabilityKafka(t *testing.T) {
	redialRoundTrip(t, "configs/.rr-kafka-durability-redial.yaml", "test-1", "test-2")
}

func TestDurabilityKafkaCG(t *testing.T) {
	redialRoundTrip(t, "configs/.rr-kafka-durability-redial-cg.yaml", "test-11", "test-22")
}
