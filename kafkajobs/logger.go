package kafkajobs

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logger struct {
	l *zap.Logger
}

func newLogger(l *zap.Logger) *logger {
	return &logger{
		l,
	}
}

func (l *logger) Level() kgo.LogLevel {
	switch l.l.Level() {
	case zap.DebugLevel:
		return kgo.LogLevelDebug
	case zap.InfoLevel:
		return kgo.LogLevelInfo
	case zap.WarnLevel:
		return kgo.LogLevelWarn
	case zap.ErrorLevel, zap.PanicLevel, zap.DPanicLevel, zap.FatalLevel:
		return kgo.LogLevelError
	case zapcore.InvalidLevel:
		return kgo.LogLevelNone
	default:
		return kgo.LogLevelDebug
	}
}

func (l *logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	zf := toKeyValuePair(keyvals)

	switch level {
	case kgo.LogLevelDebug, kgo.LogLevelNone:
		l.l.Debug(msg, zf...)
	case kgo.LogLevelInfo:
		l.l.Info(msg, zf...)
	case kgo.LogLevelWarn:
		l.l.Warn(msg, zf...)
	case kgo.LogLevelError:
		l.l.Error(msg, zf...)
	default:
		l.l.Debug(msg, zf...)
	}
}

func toKeyValuePair(keyvals []any) []zapcore.Field {
	inLen := len(keyvals)
	if inLen == 0 {
		return nil
	}

	if inLen%2 == 0 {
		// Make sure we have even number of elements and every odd element is a string.
		isKVP := true
		keys := make([]string, 0, inLen/2)
		for i := 0; i < inLen; i += 2 {
			if key, ok := keyvals[i].(string); ok {
				keys = append(keys, key)
			} else {
				isKVP = false
				break
			}
		}

		// positive scenario
		if isKVP {
			result := make([]zap.Field, inLen/2)
			for i, key := range keys {
				result[i] = zap.Any(key, keyvals[i*2+1])
			}
			return result
		}
	}

	// This should never happen.
	result := make([]zap.Field, inLen)
	for i, val := range keyvals {
		result[i] = zap.Any(fmt.Sprintf("val%d", i), val)
	}
	return result
}
