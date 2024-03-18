package tests

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"tests/helpers"
	mocklogger "tests/mock"
	"time"

	"github.com/roadrunner-server/config/v4"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/informer/v4"
	"github.com/roadrunner-server/jobs/v4"
	kp "github.com/roadrunner-server/kafka/v4"
	"github.com/roadrunner-server/resetter/v4"
	rpcPlugin "github.com/roadrunner-server/rpc/v4"
	"github.com/roadrunner-server/server/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestKafkaTLS(t *testing.T) {
	t.Run("TestKafkaTLS", pushJobTest("configs/.rr-kafka-tls.yaml", "127.0.0.1:6002", "test-1-tls"))
}

func TestKafkaTLSRootCA(t *testing.T) {
	t.Run("TestKafkaTLSRootCA", pushJobTest("configs/.rr-kafka-tls-root-ca.yaml", "127.0.0.1:6003", "test-1-tls-root-ca"))
}

func pushJobTest(env string, rpcAddress string, pipeline string) func(t *testing.T) {
	return func(t *testing.T) {
		cont := endure.New(slog.LevelDebug)

		cfg := &config.Plugin{
			Version: "v2023.1.0",
			Path:    env,
			Prefix:  "rr",
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
		require.NoError(t, err)

		ch, err := cont.Serve()
		require.NoError(t, err)

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
		t.Run("PushPipeline", helpers.PushToPipe(pipeline, false, rpcAddress))
		time.Sleep(time.Second * 5)
		t.Run("DestroyPipelines", helpers.DestroyPipelines(rpcAddress, pipeline))
		time.Sleep(time.Second * 5)

		stopCh <- struct{}{}
		wg.Wait()

		assert.Equal(t, 1, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
		assert.Equal(t, 1, oLogger.FilterMessageSnippet("job processing was started").Len())
		assert.Equal(t, 1, oLogger.FilterMessageSnippet("job was processed successfully").Len())
	}
}
