package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"
	mocklogger "tests/mock"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	kp "github.com/roadrunner-server/kafka/v6"
	"github.com/roadrunner-server/otel/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaInitCG(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-init-cg.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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
	client := helpers.NewJobsClient(t, "127.0.0.1:6001")
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority:  1,
			Pipeline:  "test-1",
			Topic:     "foo",
			Partition: 1,
		},
	}}

	_, errCall := client.Push(t.Context(), connect.NewRequest(req))
	require.NoError(t, errCall)

	wgg := &sync.WaitGroup{}
	for range 100 {
		wgg.Go(func() {
			_, errCall := client.Push(t.Context(), connect.NewRequest(req))
			require.NoError(t, errCall)
		})
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

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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
	client := helpers.NewJobsClient(t, "127.0.0.1:6002")
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     uuid.NewString(),
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority:  1,
			Pipeline:  "test-1-pq",
			Topic:     "foo-pq",
			Partition: 1,
		},
	}}

	wgg := &sync.WaitGroup{}
	for range 100 {
		wgg.Go(func() {
			_, errCall := client.Push(t.Context(), connect.NewRequest(req))
			require.NoError(t, errCall)
		})
	}
	wgg.Wait()

	time.Sleep(time.Second * 5)
	t.Run("DestroyPipelines", helpers.DestroyPipelines("127.0.0.1:6002", "test-1-pq"))

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 0, oLogger.FilterMessageSnippet("job was processed successfully").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	// "job processing was started" fires once per job pulled by the listener (jobs/listener.go),
	// not once per worker. The pool has 4 workers, so 4 is the minimum; the actual count
	// fluctuates with Kafka consumer group poll timing (5-8 observed across runs).
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job processing was started").Len(), 4)
	assert.Equal(t, 4, oLogger.FilterMessageSnippet("------> job poller was stopped <------").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("consumer context canceled, stopping the listener").Len())
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 100)
}

func TestKafkaInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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
	client := helpers.NewJobsClient(t, "127.0.0.1:6001")
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority: 1,
			Pipeline: "test-1",
			Topic:    "test-1",
		},
	}}

	wgg := &sync.WaitGroup{}
	for range 1000 {
		wgg.Go(func() {
			_, errCall := client.Push(t.Context(), connect.NewRequest(req))
			require.NoError(t, errCall)
		})
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
	cont := endure.New(slog.LevelError)

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-declare.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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

	for range 2 {
		t.Run("PushPipeline", helpers.PushToPipe("test-33", false, "127.0.0.1:6001"))
	}

	time.Sleep(time.Second * 5)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:6001", "test-33"))

	for range 2 {
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
	cont := endure.New(slog.LevelError)

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-declare.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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

	for range 2 {
		t.Run("PushPipeline", helpers.PushToPipe("test-22", false, "127.0.0.1:6001"))
	}

	time.Sleep(time.Second * 5)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:6001", "test-22"))

	for range 2 {
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
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-jobs-err.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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
	cont := endure.New(slog.LevelError)

	cfg := &config.Plugin{
		Version: "v2023.1.0",
		Path:    "configs/.rr-kafka-otel.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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
	client := helpers.NewJobsClient(t, "127.0.0.1:6001")
	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority:  1,
			Pipeline:  "test-1",
			Topic:     "foo-bar",
			Partition: 1,
		},
	}}

	_, errCall := client.Push(t.Context(), connect.NewRequest(req))
	require.NoError(t, errCall)

	wgg := &sync.WaitGroup{}
	for range 3 {
		wgg.Go(func() {
			_, errCall := client.Push(t.Context(), connect.NewRequest(req))
			require.NoError(t, errCall)
		})
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

	slices.Sort(spans)

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
	cont := endure.New(slog.LevelError)

	cfg := &config.Plugin{
		Version: "v2023.3.0",
		Path:    "configs/.rr-kafka-ping-failed.yaml",
	}

	l, _ := mocklogger.SlogTestLogger(slog.LevelDebug)
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
	cont := endure.New(slog.LevelError)

	cfg := &config.Plugin{
		Version: "v2023.3.0",
		Path:    "configs/.rr-kafka-ping-ok.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
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
		client := helpers.NewJobsClient(t, "127.0.0.1:6001")

		consumer := fmt.Sprintf(`{"topics": ["%s"], "consumer_offset": {"type": "AtStart"}}`, topic)

		req := &jobsProto.DeclareRequest{
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

		_, err := client.Declare(t.Context(), connect.NewRequest(req))
		assert.NoError(t, err)
	}
}

func declarePipeCG(topic string) func(t *testing.T) {
	return func(t *testing.T) {
		client := helpers.NewJobsClient(t, "127.0.0.1:6001")

		consumer := fmt.Sprintf(`{"topics": ["%s"], "consumer_offset": {"type": "AtStart"}}`, topic)

		req := &jobsProto.DeclareRequest{
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

		_, err := client.Declare(t.Context(), connect.NewRequest(req))
		assert.NoError(t, err)
	}
}
