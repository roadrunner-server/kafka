package kafkajobs

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPipeliningStrategy covers the consumer_options.pipelining_strategy
// default and validation.
func TestPipeliningStrategy(t *testing.T) {
	tests := []struct {
		name string
		// consumer is the consumer_options block; nil means a producer-only pipeline
		consumer *ConsumerOpts
		want     PipeliningStrategy
		wantErr  string
	}{
		{name: "unset defaults to FanOut", consumer: &ConsumerOpts{Topics: []string{"foo"}}, want: FanOutPipelining},
		{name: "FanOut", consumer: &ConsumerOpts{Topics: []string{"foo"}, PipeliningStrategy: "FanOut"}, want: FanOutPipelining},
		{name: "Serial", consumer: &ConsumerOpts{Topics: []string{"foo"}, PipeliningStrategy: "Serial"}, want: SerialPipelining},
		{name: "values are case sensitive", consumer: &ConsumerOpts{Topics: []string{"foo"}, PipeliningStrategy: "serial"}, wantErr: "unknown pipelining strategy: serial"},
		{name: "unknown value", consumer: &ConsumerOpts{Topics: []string{"foo"}, PipeliningStrategy: "Ordered"}, wantErr: "unknown pipelining strategy: Ordered"},
		{name: "no consumer options", consumer: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config{
				Brokers:      []string{"127.0.0.1:9092"},
				ConsumerOpts: tc.consumer,
			}

			_, err := cfg.InitDefault(slog.New(slog.DiscardHandler))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			if tc.consumer != nil {
				require.Equal(t, tc.want, cfg.ConsumerOpts.PipeliningStrategy)
			}
		})
	}
}
