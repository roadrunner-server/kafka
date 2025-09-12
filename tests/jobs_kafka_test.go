package tests

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"
	mocklogger "tests/mock"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	"github.com/roadrunner-server/config/v5"
	"github.com/roadrunner-server/endure/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v3/pkg/rpc"
	"github.com/roadrunner-server/informer/v5"
	"github.com/roadrunner-server/jobs/v5"
	kp "github.com/roadrunner-server/kafka/v5"
	"github.com/roadrunner-server/otel/v5"
	"github.com/roadrunner-server/resetter/v5"
	rpcPlugin "github.com/roadrunner-server/rpc/v5"
	"github.com/roadrunner-server/server/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestKafkaInitCG(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-init-cg.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		l,
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority:  1,
			Pipeline:  "test-1",
			Topic:     "foo",
			Partition: 1,
		},
	}}

	er := &jobsProto.Empty{}
	errCall := client.Call("jobs.Push", req, er)
	require.NoError(t, errCall)

	wgg := &sync.WaitGroup{}
	wgg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wgg.Done()
			resp := &jobsProto.Empty{}
			errCall := client.Call("jobs.Push", req, resp)
			require.NoError(t, errCall)
		}()
	}
	wgg.Wait()

	time.Sleep(time.Second * 10)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6001", "test-1"))

	stopCh <- struct{}{}
	wg.Wait()

	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 100)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 100)
}

func TestKafkaPQCG(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "v2023.2.0",
		Path:    "configs/.rr-kafka-init-pq.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		l,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	conn, err := net.Dial("tcp", "127.0.0.1:6002")
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     uuid.NewString(),
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority:  1,
			Pipeline:  "test-1-pq",
			Topic:     "foo-pq",
			Partition: 1,
		},
	}}

	wgg := &sync.WaitGroup{}
	wgg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wgg.Done()
			resp := &jobsProto.Empty{}
			errCall := client.Call("jobs.Push", req, resp)
			require.NoError(t, errCall)
		}()
	}
	wgg.Wait()

	time.Sleep(time.Second * 5)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6002", "test-1-pq"))

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 0, oLogger.FilterMessageSnippet("job was processed successfully").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.Equal(t, 4, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 4, oLogger.FilterMessageSnippet("------> job poller was stopped <------").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("consumer context canceled, stopping the listener").Len())
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 100)
}

func TestKafkaPipeliningStrategy(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2025.1.0",
		Path:    "configs/.rr-kafka-serial-consumption.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		l,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	defer func() { _ = conn.Close() }()
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	defer func() { _ = client.Close() }()

	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:6001", "test-consume-1", "test-consume-2"))
	seed := func(topic int, group *sync.WaitGroup) {
		defer group.Done()
		for i := 0; i < 20; i++ {
			resp := &jobsProto.Empty{}
			req := &jobsProto.PushRequest{Job: &jobsProto.Job{
				Job:     uuid.NewString(),
				Id:      uuid.NewString(),
				Payload: []byte(fmt.Sprintf("%v:%v", topic, i)),
				Options: &jobsProto.Options{
					Pipeline: "test-produce",
					Topic:    fmt.Sprintf("serial-%v", topic),
				},
			}}
			errCall := client.Call("jobs.Push", req, resp)
			require.NoError(t, errCall)
		}
	}

	wgg := &sync.WaitGroup{}
	wgg.Add(3)
	for i := 1; i < 4; i++ {
		go seed(i, wgg)
	}
	wgg.Wait()

	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:6001", "test-consume-1", "test-consume-2"))

	time.Sleep(time.Second * 5)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6001", "test-consume-1", "test-consume-2", "test-produce"))

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 60, oLogger.FilterMessageSnippet("job was pushed successfully").Len())

	logs := oLogger.FilterMessageSnippet("php consumed:1:").All()
	assert.Len(t, logs, 20)
	for i, entry := range oLogger.FilterMessageSnippet("php consumed:1:").All() {
		assert.Equal(t, fmt.Sprintf("php consumed:1:%v\n", i), entry.Message)
	}

	logs = oLogger.FilterMessageSnippet("php consumed:2:").All()
	assert.Len(t, logs, 20)
	for i, entry := range logs {
		assert.Equal(t, fmt.Sprintf("php consumed:2:%v\n", i), entry.Message)
	}

	logs = oLogger.FilterMessageSnippet("php consumed:3:").All()
	assert.Len(t, logs, 20)
	for i, entry := range logs {
		assert.Equal(t, fmt.Sprintf("php consumed:3:%v\n", i), entry.Message)
	}
}

func TestKafkaInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-init.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		l,
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	require.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority: 1,
			Pipeline: "test-1",
			Topic:    "test-1",
		},
	}}

	wgg := &sync.WaitGroup{}
	wgg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wgg.Done()
			er := &jobsProto.Empty{}
			errCall := client.Call("jobs.Push", req, er)
			require.NoError(t, errCall)
		}()
	}
	wgg.Wait()

	time.Sleep(time.Second * 5)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6001", "test-1"))
	time.Sleep(time.Second * 5)

	stopCh <- struct{}{}
	wg.Wait()

	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 1000)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 1000)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 1000)
}

func TestKafkaDeclareCG(t *testing.T) {
	cont := endure.New(slog.LevelError, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-declare.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		l,
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	t.Run("DeclarePipeline", declarePipeCG("test-33"))
	time.Sleep(time.Second * 2)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:6001", "test-33"))
	time.Sleep(time.Second * 2)

	t.Run("PushPipeline", helpers.PushToPipe("test-33", false, "127.0.0.1:6001"))

	for i := 0; i < 2; i++ {
		t.Run("PushPipeline", helpers.PushToPipe("test-33", false, "127.0.0.1:6001"))
	}

	time.Sleep(time.Second * 5)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:6001", "test-33"))

	for i := 0; i < 2; i++ {
		t.Run("PushPipeline", helpers.PushToPipe("test-33", false, "127.0.0.1:6001"))
	}

	time.Sleep(time.Second * 2)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:6001", "test-33"))
	time.Sleep(time.Second * 5)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6001", "test-33"))
	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 5, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
	assert.Equal(t, 5, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was resumed").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was paused").Len())
}

func TestKafkaDeclare(t *testing.T) {
	cont := endure.New(slog.LevelError, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-declare.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		l,
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	t.Run("DeclarePipeline", declarePipe("test-22"))
	time.Sleep(time.Second * 2)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:6001", "test-22"))
	time.Sleep(time.Second * 2)

	t.Run("PushPipeline", helpers.PushToPipe("test-22", false, "127.0.0.1:6001"))

	for i := 0; i < 2; i++ {
		t.Run("PushPipeline", helpers.PushToPipe("test-22", false, "127.0.0.1:6001"))
	}

	time.Sleep(time.Second * 5)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:6001", "test-22"))

	for i := 0; i < 2; i++ {
		t.Run("PushPipeline", helpers.PushToPipe("test-22", false, "127.0.0.1:6001"))
	}

	time.Sleep(time.Second * 2)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:6001", "test-22"))
	time.Sleep(time.Second * 5)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6001", "test-22"))
	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 5, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
	assert.Equal(t, 5, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was resumed").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was paused").Len())
}

func TestKafkaJobsError(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-jobs-err.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&kp.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	t.Run("DeclarePipeline", declarePipe("test-3"))
	time.Sleep(time.Second)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:6001", "test-3"))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:6001"))
	time.Sleep(time.Second * 25)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:6001", "test-3"))
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6001", "test-3"))

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6001", "test-3"))

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
	assert.Equal(t, 4, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 4, oLogger.FilterMessageSnippet("job was processed successfully").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was paused").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was resumed").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.Equal(t, 3, oLogger.FilterMessageSnippet("jobs protocol error").Len())
}

func TestKafkaOTEL(t *testing.T) {
	cont := endure.New(slog.LevelError, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-otel.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		&otel.Plugin{},
		l,
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

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

	time.Sleep(time.Second * 3)
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	require.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority:  1,
			Pipeline:  "test-1",
			Topic:     "foo-bar",
			Partition: 1,
		},
	}}

	er := &jobsProto.Empty{}
	errCall := client.Call("jobs.Push", req, er)
	require.NoError(t, errCall)

	wgg := &sync.WaitGroup{}
	wgg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wgg.Done()
			er := &jobsProto.Empty{}
			errCall := client.Call("jobs.Push", req, er)
			require.NoError(t, errCall)
		}()
	}
	wgg.Wait()

	time.Sleep(time.Second * 10)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6001", "test-1"))
	time.Sleep(time.Second)

	stopCh <- struct{}{}
	wg.Wait()

	resp, err := http.Get("http://127.0.0.1:9411/api/v2/spans?serviceName=rr_test_kafka") //nolint:noctx
	assert.NoError(t, err)

	buf, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	var spans []string
	err = json.Unmarshal(buf, &spans)
	assert.NoError(t, err)

	sort.Slice(spans, func(i, j int) bool {
		return spans[i] < spans[j]
	})

	expected := []string{
		"destroy_pipeline",
		"jobs_listener",
		"kafka_listener",
		"kafka_push",
		"kafka_stop",
		"push",
	}
	assert.Equal(t, expected, spans)

	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 3)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 3)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 3)

	t.Cleanup(func() {
		_ = resp.Body.Close()
	})
}

func TestKafkaPingFailed(t *testing.T) {
	cont := endure.New(slog.LevelError, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.3.0",
		Path:    "configs/.rr-kafka-ping-failed.yaml",
	}

	l, _ := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		l,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	require.NoError(t, err)

	_, err = cont.Serve()

	assert.Error(t, err, "failed: server should not start because of kafka ping error")
	assert.Contains(t, err.Error(), "kafka_ping: unable to dial")
}

func TestKafkaPingOk(t *testing.T) {
	cont := endure.New(slog.LevelError, endure.GracefulShutdownTimeout(time.Second))

	cfg := &config.Plugin{
		Version: "v2023.3.0",
		Path:    "configs/.rr-kafka-ping-ok.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&kp.Plugin{},
		l,
	)
	assert.NoError(t, err)

	err = cont.Init()

	require.NoError(t, err)

	_, err = cont.Serve()
	require.NoError(t, err)
	time.Sleep(time.Second)

	err = cont.Stop()
	assert.NoError(t, err)

	assert.Equal(t, 1, oLogger.FilterMessage("ping kafka: ok").Len())
}

func declarePipe(topic string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", "127.0.0.1:6001")
		assert.NoError(t, err)
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		consumer := fmt.Sprintf(`{"topics": ["%s"], "consumer_offset": {"type": "AtStart"}}`, topic)

		pipe := &jobsProto.DeclareRequest{
			Pipeline: map[string]string{
				"driver":                    "kafka",
				"name":                      topic,
				"priority":                  "3",
				"auto_create_topics_enable": "true",

				"producer_options": `{
					"max_message_bytes": 1000,
					"required_acks": "LeaderAck",
					"compression_codec": "snappy",
					"disable_idempotent": true
				}`,

				"consumer_options": consumer,
			},
		}

		er := &jobsProto.Empty{}
		err = client.Call("jobs.Declare", pipe, er)
		assert.NoError(t, err)
	}
}

func declarePipeCG(topic string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", "127.0.0.1:6001")
		assert.NoError(t, err)
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		consumer := fmt.Sprintf(`{"topics": ["%s"], "consumer_offset": {"type": "AtStart"}}`, topic)

		pipe := &jobsProto.DeclareRequest{
			Pipeline: map[string]string{
				"driver":                    "kafka",
				"name":                      topic,
				"priority":                  "3",
				"auto_create_topics_enable": "true",

				"group_options": `{
					"group_id": "foo-bar",
					"block_rebalance_on_poll": true
				}`,

				"producer_options": `{
					"max_message_bytes": 1000,
					"required_acks": "LeaderAck",
					"compression_codec": "snappy",
					"disable_idempotent": true
				}`,

				"consumer_options": consumer,
			},
		}

		er := &jobsProto.Empty{}
		err = client.Call("jobs.Declare", pipe, er)
		assert.NoError(t, err)
	}
}
