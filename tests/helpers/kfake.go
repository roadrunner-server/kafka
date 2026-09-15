package helpers

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// NewFakeCluster starts an in-process kafka cluster with one broker on the port
// and one topic with one partition. The cluster answers requests the same way a
// broker does, and a test can replace the answer to one request kind.
func NewFakeCluster(t *testing.T, port int, topic string) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.Ports(port), kfake.SeedTopics(1, topic), kfake.AllowAutoTopicCreation())
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	return cluster
}

// FailFetches answers every fetch with a non-retriable authorization error
// while the returned flag is set. The cluster processes the fetches normally
// after the flag is cleared.
func FailFetches(cluster *kfake.Cluster) *atomic.Bool {
	failing := &atomic.Bool{}
	failing.Store(true)

	cluster.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()

		if !failing.Load() {
			return nil, nil, false
		}

		freq := req.(*kmsg.FetchRequest)
		resp := freq.ResponseKind().(*kmsg.FetchResponse)

		for _, topic := range freq.Topics {
			rt := kmsg.NewFetchResponseTopic()
			rt.Topic = topic.Topic
			rt.TopicID = topic.TopicID

			for _, partition := range topic.Partitions {
				rp := kmsg.NewFetchResponseTopicPartition()
				rp.Partition = partition.Partition
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				rt.Partitions = append(rt.Partitions, rp)
			}

			resp.Topics = append(resp.Topics, rt)
		}

		return resp, nil, true
	})

	return failing
}
