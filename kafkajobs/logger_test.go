package kafkajobs

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

type captureHandler struct {
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func TestLogger_Log(t *testing.T) {
	cases := []struct {
		name    string
		keyvals []any
	}{
		{
			name:    "keyvals is nil",
			keyvals: nil,
		},
		{
			name:    "beginning sasl authentication",
			keyvals: []any{"broker", "b-name", "addr", net.ParseIP("127.0.0.1"), "mechanism", "some-thing", "authenticate", true},
		},
		{
			name:    "no strings",
			keyvals: []any{0, "val1", 2, "val3"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := &captureHandler{}
			newLogger(slog.New(h)).Log(kgo.LogLevelDebug, c.name, c.keyvals...)
			assert.Len(t, h.records, 1)
			assert.Equal(t, c.name, h.records[0].Message)
		})
	}
}
