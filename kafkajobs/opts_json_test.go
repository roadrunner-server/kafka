package kafkajobs

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumerOffsetsJSON(t *testing.T) {
	tests := []struct {
		name    string
		options string
		want    ConsumerOpts
	}{
		{
			name:    "consumer offset",
			options: `{"consumer_offset":{"type":"At","value":42}}`,
			want:    ConsumerOpts{ConsumerOffset: &Offset{Type: At, Value: 42}},
		},
		{
			name:    "partition offset",
			options: `{"consume_partitions":{"events":{"3":{"type":"Relative","value":-2}}}}`,
			want:    ConsumerOpts{ConsumePartitions: map[string]map[int32]*Offset{"events": {3: {Type: Relative, Value: -2}}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var options ConsumerOpts
			require.NoError(t, json.Unmarshal([]byte(tc.options), &options))
			require.Equal(t, tc.want, options)
		})
	}
}
