package tests

import (
	"testing"

	"tests/helpers"

	"github.com/stretchr/testify/require"
)

const (
	restartAddr               = "127.0.0.1:6006"
	restartBrokerPort         = 9094
	declaredRestartAddr       = "127.0.0.1:6007"
	declaredRestartBrokerPort = 9095
)

// TestRestartAfterNonRetriableError covers a pipeline whose fetches fail with
// a non-retriable error. The listener stops and asks the JOBS plugin for a new
// pipeline. The in-process broker fails the fetches until the JOBS plugin
// receives the restart command.
func TestRestartAfterNonRetriableError(t *testing.T) {
	const (
		pipeline = "test-restart"
		topic    = "test-restart"
	)

	cluster := helpers.NewFakeCluster(t, restartBrokerPort, topic)
	rr, _ := boot(t, "configs/.rr-kafka-restart.yaml", restartAddr)
	rr.WaitLog(t, "pipeline was started", 1)

	helpers.PushToPipe(pipeline, false, restartAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	failing := helpers.FailFetches(cluster)
	rr.WaitLog(t, "non-recoverable consumer error", 1)
	rr.WaitLog(t, "kafka listener stopped", 1)
	// The listener exited. The new pipeline must fetch without an error.
	failing.Store(false)

	rr.WaitLog(t, "received JOBS event", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.WaitLog(t, "pipeline was started", 2)

	helpers.PushEventually(t, restartAddr, pipeline)
	rr.WaitLog(t, "job was processed successfully", 2)
	require.True(t, helpers.StatsFor(t, restartAddr, pipeline).GetReady())
	rr.RequireLogCount(t, "pipeline restart command was sent", 1)

	helpers.DestroyPipelines(restartAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

// TestRestartDeclaredPipelineAfterNonRetriableError covers the same failure
// for a pipeline that was declared and resumed over RPC. The JOBS plugin runs
// the new pipeline because the pipeline was resumed at runtime.
func TestRestartDeclaredPipelineAfterNonRetriableError(t *testing.T) {
	const pipeline = "test-restart-declared"

	cluster := helpers.NewFakeCluster(t, declaredRestartBrokerPort, pipeline)
	rr, _ := boot(t, "configs/.rr-kafka-restart-declare.yaml", declaredRestartAddr)
	// The YAML fixture supplies the group options.
	helpers.DeclarePipe(declaredRestartAddr, pipeline, pipeline, false)(t)
	helpers.ResumePipes(declaredRestartAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was resumed", 1)

	helpers.PushToPipe(pipeline, false, declaredRestartAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	failing := helpers.FailFetches(cluster)
	rr.WaitLog(t, "non-recoverable consumer error", 1)
	rr.WaitLog(t, "kafka listener stopped", 1)
	// The listener exited. The new pipeline must fetch without an error.
	failing.Store(false)

	rr.WaitLog(t, "received JOBS event", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.WaitLog(t, "pipeline was started", 1)

	helpers.PushEventually(t, declaredRestartAddr, pipeline)
	rr.WaitLog(t, "job was processed successfully", 2)
	require.True(t, helpers.StatsFor(t, declaredRestartAddr, pipeline).GetReady())
	rr.RequireLogCount(t, "pipeline was resumed", 1)

	helpers.DestroyPipelines(declaredRestartAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}
