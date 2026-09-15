package tests

import (
	"testing"

	"tests/helpers"

	"github.com/stretchr/testify/require"
)

const (
	staticMemberAddr         = "127.0.0.1:6004"
	declaredStaticMemberAddr = "127.0.0.1:6005"
)

// TestRejoinAfterGroupMemberRemoved covers a pipeline that loses its group
// membership while the broker stays available. The kafka client rejoins the
// group on its own. The driver must keep its listener and resume consumption
// without a pipeline restart.
func TestRejoinAfterGroupMemberRemoved(t *testing.T) {
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
	rr.WaitLog(t, "group session was lost, the consumer rejoins the group", 1)

	helpers.PushEventually(t, staticMemberAddr, pipeline)
	rr.WaitLog(t, "job was processed successfully", 2)

	require.True(t, helpers.StatsFor(t, staticMemberAddr, pipeline).GetReady())
	require.Len(t, helpers.GroupMembers(t, group), 1)
	require.True(t, helpers.GroupAssignedPartitions(t, group).Lookup(topic, 0))
	require.Zero(t, rr.CountLog("non-recoverable consumer error"))
	require.Zero(t, rr.CountLog("kafka listener stopped"))
	require.Zero(t, rr.CountLog("received JOBS event"))
	rr.RequireLogCount(t, "pipeline was started", 1)

	helpers.DestroyPipelines(staticMemberAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestRejoinDeclaredPipelineAfterGroupMemberRemoved covers the same loss for a
// pipeline that was declared and resumed over RPC.
func TestRejoinDeclaredPipelineAfterGroupMemberRemoved(t *testing.T) {
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
	rr.WaitLog(t, "group session was lost, the consumer rejoins the group", 1)

	helpers.PushEventually(t, declaredStaticMemberAddr, pipeline)
	rr.WaitLog(t, "job was processed successfully", 2)
	require.True(t, helpers.StatsFor(t, declaredStaticMemberAddr, pipeline).GetReady())
	require.Len(t, helpers.GroupMembers(t, group), 1)
	require.True(t, helpers.GroupAssignedPartitions(t, group).Lookup(pipeline, 0))
	require.Zero(t, rr.CountLog("non-recoverable consumer error"))
	require.Zero(t, rr.CountLog("kafka listener stopped"))
	require.Zero(t, rr.CountLog("received JOBS event"))
	require.Zero(t, rr.CountLog("pipeline was started"))
	rr.RequireLogCount(t, "pipeline was resumed", 1)

	helpers.DestroyPipelines(declaredStaticMemberAddr, pipeline)(t)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}
