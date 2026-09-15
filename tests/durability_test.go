package tests

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"tests/helpers"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
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

func startBroker(t *testing.T, extraEnv ...string) *broker {
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
	err = jsonmessage.DisplayJSONMessagesStream(pull, io.Discard, 0, false, nil)
	_ = pull.Close()
	require.NoError(t, err)

	k, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "confluentinc/cp-kafka:8.1.1",
		Env: append([]string{
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
		}, extraEnv...),
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

func TestDurabilityKafkaCGReadPermissionRecovery(t *testing.T) {
	startBroker(t,
		"KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		"KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true",
	)
	cl, err := kgo.NewClient(kgo.SeedBrokers(helpers.BrokerAddr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	admin := kadm.NewClient(cl)
	_, err = admin.CreateTopic(t.Context(), 3, 1, nil, "foo")
	require.NoError(t, err)

	rr, _ := boot(t, "configs/.rr-kafka-init-cg.yaml", durabilityAddr)

	producePartitionBatch(t, cl, 0)
	waitPartitionOffsets(t, admin, 10)
	rr.WaitLog(t, "job was processed successfully", 30)

	allow := kadm.NewACLs().Topics("foo").Allow("User:ANONYMOUS").Operations(kadm.OpAll).ResourcePatternType(kadm.ACLPatternLiteral)
	allowed, err := admin.CreateACLs(t.Context(), allow)
	require.NoError(t, err)
	require.Len(t, allowed, 1)
	require.NoError(t, allowed[0].Err)

	deny := kadm.NewACLs().Topics("foo").Deny("User:ANONYMOUS").DenyHosts("*").Operations(kadm.OpRead).ResourcePatternType(kadm.ACLPatternLiteral)
	denied, err := admin.CreateACLs(t.Context(), deny)
	require.NoError(t, err)
	require.Len(t, denied, 1)
	require.NoError(t, denied[0].Err)
	rr.WaitLog(t, "non-recoverable consumer error", 1)
	rr.WaitLog(t, "kafka listener stopped", 1)

	deleted, err := admin.DeleteACLs(t.Context(), deny)
	require.NoError(t, err)
	require.Len(t, deleted, 1)
	require.NoError(t, deleted[0].Err)
	require.Len(t, deleted[0].Deleted, 1)
	require.NoError(t, deleted[0].Deleted[0].Err)

	producePartitionBatch(t, cl, 10)
	waitPartitionOffsets(t, admin, 20)
	rr.RequireLogCount(t, "job was processed successfully", 60)
}

func producePartitionBatch(t *testing.T, cl *kgo.Client, first int) {
	t.Helper()

	var records []*kgo.Record
	for partition := range int32(3) {
		for n := range 10 {
			records = append(records, &kgo.Record{
				Topic:     "foo",
				Partition: partition,
				Key:       fmt.Appendf(nil, "p%d-%d", partition, first+n),
				Value:     []byte(`{"hello":"world"}`),
				Headers: []kgo.RecordHeader{
					{Key: "rr_job", Value: []byte("some/php/namespace")},
					{Key: "rr_pipeline", Value: []byte("test-1")},
				},
			})
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, cl.ProduceSync(ctx, records...).FirstErr())
}

func waitPartitionOffsets(t *testing.T, admin *kadm.Client, want int64) {
	t.Helper()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		offsets, err := admin.FetchOffsets(ctx, "bar")
		if err != nil || len(offsets["foo"]) != 3 {
			return false
		}
		for _, offset := range offsets["foo"] {
			if offset.Err != nil || offset.At != want {
				return false
			}
		}
		return true
	}, time.Minute, time.Second, "each partition must commit offset %d", want)
}
