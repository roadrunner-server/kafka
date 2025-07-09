package tests

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"
	mocklogger "tests/mock"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/roadrunner-server/config/v5"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/informer/v5"
	"github.com/roadrunner-server/jobs/v5"
	"github.com/roadrunner-server/kafka/v5"
	"github.com/roadrunner-server/logger/v5"
	"github.com/roadrunner-server/resetter/v5"
	"github.com/roadrunner-server/server/v5"
	"go.uber.org/zap"

	rpcPlugin "github.com/roadrunner-server/rpc/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kafkaDocker(pause, start, remove chan struct{}) (chan struct{}, error) {
	ctx := context.Background()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cli.Close()
	}()

	// Create a network
	networkName := "rr-e2e-tests"
	_, err = cli.NetworkCreate(ctx, networkName, network.CreateOptions{})
	if err != nil {
		return nil, err
	}

	netConf := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			networkName: {},
		},
	}

	zk, err := cli.ImagePull(ctx, "confluentinc/cp-zookeeper:latest", image.PullOptions{})
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = zk.Close()
	}()

	_, _ = io.Copy(os.Stdout, zk)

	zkc, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "confluentinc/cp-zookeeper",
		Tty:   false,
		Env:   []string{"ZOOKEEPER_CLIENT_PORT=2181", "ZOOKEEPER_TICK_TIME=2000"},
	}, &container.HostConfig{}, netConf, nil, "zookeeper")
	if err != nil {
		panic(err)
	}

	err = cli.ContainerStart(ctx, zkc.ID, container.StartOptions{})
	if err != nil {
		return nil, err
	}

	cpKafka, err := cli.ImagePull(ctx, "confluentinc/cp-kafka:7.8.2", image.PullOptions{})
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = cpKafka.Close()
	}()

	_, _ = io.Copy(os.Stdout, cpKafka)

	k, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "confluentinc/cp-kafka:7.8.2",
		Tty:   false,
		Env: []string{
			"KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_BROKER_ID=1",
			"AUTO_CREATE_TOPICS_ENABLE=true",
			"KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092,PLAINTEXT_INTERNAL://broker:29092",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		},
	}, &container.HostConfig{
		LogConfig: container.LogConfig{},
		PortBindings: map[nat.Port][]nat.PortBinding{
			"9092/tcp": {
				nat.PortBinding{HostIP: "127.0.0.1", HostPort: "9092"},
			},
		},
		//Links:         []string{fmt.Sprintf("%s:alias", zkc.ID)},
		RestartPolicy: container.RestartPolicy{Name: "always"},
	}, netConf, nil, "broker")
	if err != nil {
		return nil, err
	}

	err = cli.ContainerStart(ctx, k.ID, container.StartOptions{})
	if err != nil {
		return nil, err
	}

	done := make(chan struct{}, 1)

	go func() {
		for {
			select {
			case <-pause:
				timeout := 10
				err2 := cli.ContainerStop(context.Background(), k.ID, container.StopOptions{
					Signal:  "SIGKILL",
					Timeout: &timeout,
				})
				if err2 != nil {
					panic(err2)
				}
			case <-start:
				err2 := cli.ContainerStart(context.Background(), k.ID, container.StartOptions{})
				if err2 != nil {
					panic(err2)
				}
			case <-remove:
				bg := context.Background()

				timeout := 10
				err2 := cli.ContainerStop(bg, zkc.ID, container.StopOptions{
					Signal:  "SIGKILL",
					Timeout: &timeout,
				})
				if err2 != nil {
					panic(err2)
				}
				err2 = cli.ContainerStop(bg, k.ID, container.StopOptions{
					Signal:  "SIGKILL",
					Timeout: &timeout,
				})
				if err2 != nil {
					panic(err2)
				}

				err2 = cli.ContainerRemove(bg, k.ID, container.RemoveOptions{
					RemoveVolumes: true,
					Force:         true,
				})
				if err2 != nil {
					panic(err2)
				}

				err2 = cli.ContainerRemove(bg, zkc.ID, container.RemoveOptions{
					RemoveVolumes: true,
					Force:         true,
				})
				if err2 != nil {
					panic(err2)
				}

				_ = cli.NetworkRemove(ctx, networkName)

				done <- struct{}{}
				return
			}
		}
	}()

	return done, nil
}

func TestDurabilityKafka(t *testing.T) {
	pause, start, remove := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 1)
	doneCh, err := kafkaDocker(pause, start, remove)
	if err != nil {
		t.Fatal(err)
	}

	// we need to wait for the container, because of ping
	time.Sleep(time.Second * 40)

	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*30))

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-kafka-durability-redial.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err = cont.RegisterAll(
		l,
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&kafka.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second)
	pause <- struct{}{}
	time.Sleep(time.Second * 15)

	t.Run("PushPipelineWhileRedialing-1", helpers.PushToPipeErr("test-1"))
	t.Run("PushPipelineWhileRedialing-2", helpers.PushToPipeErr("test-2"))

	start <- struct{}{}
	time.Sleep(time.Second * 20)

	t.Run("PushPipelineWhileRedialing-1", helpers.PushToPipe("test-1", false, "127.0.0.1:6001"))
	t.Run("PushPipelineWhileRedialing-2", helpers.PushToPipe("test-2", false, "127.0.0.1:6001"))
	time.Sleep(time.Second * 5)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6001", "test-1", "test-2"))

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job processing was started").Len(), 1)
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("job push error").Len())

	t.Cleanup(func() {
		remove <- struct{}{}
		<-doneCh
	})
}

func TestDurabilityKafkaCG(t *testing.T) {
	pause, start, remove := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 1)
	doneCh, err := kafkaDocker(pause, start, remove)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 40)

	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*30))

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-kafka-durability-redial-cg.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err = cont.RegisterAll(
		l,
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&kafka.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()
	time.Sleep(time.Second)
	pause <- struct{}{}
	time.Sleep(time.Second * 15)

	t.Run("PushPipelineWhileRedialing-1", helpers.PushToPipeErr("test-11"))
	t.Run("PushPipelineWhileRedialing-2", helpers.PushToPipeErr("test-22"))

	time.Sleep(time.Second)
	start <- struct{}{}
	time.Sleep(time.Second * 20)

	t.Run("PushPipelineWhileRedialing-1", helpers.PushToPipe("test-11", false, "127.0.0.1:6001"))
	t.Run("PushPipelineWhileRedialing-2", helpers.PushToPipe("test-22", false, "127.0.0.1:6001"))

	time.Sleep(time.Second * 2)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6001", "test-11", "test-22"))

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job processing was started").Len(), 1)
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("job push error").Len())

	t.Cleanup(func() {
		remove <- struct{}{}
		<-doneCh
	})
}
