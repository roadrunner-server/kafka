package tests

import (
	"log/slog"
	"testing"

	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/kafka/v6/kafkajobs"
	"github.com/stretchr/testify/require"
)

func TestPipelineProducerTimeouts(t *testing.T) {
	tests := []struct {
		name    string
		options string
	}{
		{name: "request timeout", options: `{"request_timeout":1000000000}`},
		{name: "delivery timeout", options: `{"delivery_timeout":2000000000}`},
		{name: "transaction timeout", options: `{"transaction_timeout":3000000000}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Plugin{Type: "yaml", ReadInCfg: []byte(`{"version":"3","kafka":{"brokers":["127.0.0.1:9092"]}}`)}
			require.NoError(t, cfg.Init())
			pipeline := jobs.Pipeline{
				"name":             "timeouts",
				"driver":           "kafka",
				"producer_options": tc.options,
				"consumer_options": `{"topics":["test"],"pipelining_strategy":"invalid"}`,
			}

			_, err := kafkajobs.FromPipeline(t.Context(), nil, pipeline, slog.Default(), cfg, nil)
			require.ErrorContains(t, err, "unknown pipelining strategy: invalid")
		})
	}
}
