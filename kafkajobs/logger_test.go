package kafkajobs

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogger_Log(t *testing.T) {
	cases := []struct {
		name     string
		keyvals  []any
		expected []zapcore.Field
	}{
		{
			name:     "keyvals is nil",
			keyvals:  nil,
			expected: []zapcore.Field{},
		},
		{
			name:    "beginning sasl authentication",
			keyvals: []any{"broker", "b-name", "addr", net.ParseIP("127.0.0.1"), "mechanism", "some-thing", "authenticate", true},
			expected: []zapcore.Field{
				zap.String("broker", "b-name"),
				zap.Stringer("addr", net.ParseIP("127.0.0.1")),
				zap.String("mechanism", "some-thing"),
				zap.Bool("authenticate", true),
			},
		},
		{
			name:    "not even",
			keyvals: []any{"one message"},
			expected: []zapcore.Field{
				zap.String("one message", "<missing>"),
			},
		},
		{
			name:    "no strings",
			keyvals: []any{0, "val1", 2, "val3"},
			expected: []zapcore.Field{
				zap.String("0", "val1"),
				zap.String("2", "val3"),
			},
		},
	}

	core, o := observer.New(zapcore.DebugLevel)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			newLogger(zap.New(core)).
				Log(kgo.LogLevelDebug, c.name, c.keyvals...)
			assert.Equal(t, 1, o.Len())
			logs := o.TakeAll()
			assert.Equal(t, c.expected, logs[0].Context)
		})
	}
}
