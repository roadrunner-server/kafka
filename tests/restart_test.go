package tests

import (
	"testing"
	"time"

	"tests/helpers"

	"github.com/stretchr/testify/require"
)

const (
	staticMemberAddr         = "127.0.0.1:6004"
	declaredStaticMemberAddr = "127.0.0.1:6005"
)

// TestRestartAfterGroupMemberRemoved covers a pipeline that loses its group
// membership while the broker stays available. The listener stops on the
// non-retriable error. The driver must ask the JOBS plugin for a new pipeline
// to resume consumption.
func TestRestartAfterGroupMemberRemoved(t *testing.T) {
	const (
		pipeline   = "test-fence"
		topic      = "test-fence"
		group      = "bar-fence"
		instanceID = "rr-fence-1"
	)

	helpers.CleanupTopics(t, topic)

	rr, _ := boot(t, "configs/.rr-kafka-static-member.yaml", staticMemberAddr)
	rr.WaitLog(t, "pipeline was started", 1)

	helpers.PushToPipe(pipeline, false, staticMemberAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	// The commit path can restore the session without an error from the
	// listener. Wait until the member has no pending commits.
	helpers.WaitCommitted(t, group, topic, 0, 1)

	helpers.RemoveGroupMember(t, group, instanceID)

	rr.WaitLog(t, "non-recoverable consumer error", 1)
	rr.WaitLog(t, "listener error", 1)

	rr.WaitLog(t, "received JOBS event", 1)
	rr.NeverLog(t, "failed to restart the pipeline")
	rr.RequireLogCount(t, "pipeline was started", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 1)

	helpers.PushEventually(t, staticMemberAddr, pipeline)
	rr.WaitLog(t, "job was processed successfully", 2)

	require.True(t, helpers.StatsFor(t, staticMemberAddr, pipeline).GetReady())
	require.Len(t, helpers.GroupMembers(t, group), 1)
	require.True(t, helpers.GroupAssignedPartitions(t, group).Lookup(topic, 0))

	helpers.DestroyPipelines(staticMemberAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

func TestRestartDeclaredPipelineAfterGroupMemberRemoved(t *testing.T) {
	const (
		pipeline   = "test-fence-declared"
		group      = "bar-fence-declared"
		instanceID = "rr-fence-declared-1"
	)

	helpers.CleanupTopics(t, pipeline)
	rr, _ := boot(t, "configs/.rr-kafka-static-member-declare.yaml", declaredStaticMemberAddr)
	// The YAML fixture supplies the group options.
	helpers.DeclarePipe(declaredStaticMemberAddr, pipeline, pipeline, false)(t)
	helpers.ResumePipes(declaredStaticMemberAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was resumed", 1)

	helpers.PushToPipe(pipeline, false, declaredStaticMemberAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)
	helpers.WaitCommitted(t, group, pipeline, 0, 1)
	helpers.RemoveGroupMember(t, group, instanceID)

	rr.WaitLog(t, "non-recoverable consumer error", 1)
	rr.WaitLog(t, "received JOBS event", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.WaitLogWithin(t, "pipeline was started", 1, time.Second*10)

	helpers.PushEventually(t, declaredStaticMemberAddr, pipeline)
	rr.WaitLog(t, "job was processed successfully", 2)
	require.True(t, helpers.StatsFor(t, declaredStaticMemberAddr, pipeline).GetReady())
	require.Len(t, helpers.GroupMembers(t, group), 1)
	require.True(t, helpers.GroupAssignedPartitions(t, group).Lookup(pipeline, 0))
	rr.RequireLogCount(t, "pipeline was resumed", 1)
	rr.RequireLogCount(t, "pipeline was started", 1)

	helpers.DestroyPipelines(declaredStaticMemberAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}
