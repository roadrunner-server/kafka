package kafkajobs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItemContextJSON(t *testing.T) {
	tests := []struct {
		name        string
		job         string
		headers     map[string][]string
		wantHeaders string
		wantErr     bool
	}{
		{name: "nil headers", job: "Job", wantHeaders: `{}`},
		{name: "nil header values", job: "Job", headers: map[string][]string{"values": nil}, wantHeaders: `{"values":[]}`},
		{name: "invalid job text", job: "\xff", wantErr: true},
		{name: "invalid header text", job: "Job", headers: map[string][]string{"values": {"\xff"}}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			item := &Item{Job: tc.job, headers: tc.headers, Options: &Options{Pipeline: "test"}}
			data, err := item.Context()
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Contains(t, string(data), `"headers":`+tc.wantHeaders)
		})
	}
}
