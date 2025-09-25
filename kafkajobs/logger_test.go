package kafkajobs

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name:     "keycals is nil",
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
			name:    "len=1",
			keyvals: []any{"one message"},
			expected: []zapcore.Field{
				zap.String("kgo_driver", "one message"),
			},
		},
		{
			name:    "no strings",
			keyvals: []any{1, "val1", 2, "val2"},
			expected: []zapcore.Field{
				zap.Int("kgo_driver", 1),
				zap.String("kgo_driver", "val1"),
				zap.Int("kgo_driver", 2),
				zap.String("kgo_driver", "val2"),
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

func TestTryFetchKeyValPairs(t *testing.T) {
	cases := []struct {
		name     string
		keyvals  []any
		expected []zapcore.Field
		ok       bool
	}{
		{
			name:     "keyvals is nil",
			keyvals:  nil,
			expected: nil,
			ok:       false,
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
			ok: true,
		},
		{
			name:     "len=1",
			keyvals:  []any{"one message"},
			expected: nil,
			ok:       false,
		},
		{
			name:     "no strings",
			keyvals:  []any{1, "val1", 2, "val2"},
			expected: nil,
			ok:       false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pairs, ok := tryFetchKeyValPairs(c.keyvals)
			require.Equal(t, c.ok, ok)
			require.Equal(t, c.expected, pairs)
		})
	}
}
