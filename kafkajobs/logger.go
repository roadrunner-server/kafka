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

	// This should never happen.
	if inLen%2 != 0 {
		keyvals = append(keyvals, "<missing>")
	}

	result := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i < len(keyvals); i += 2 {
		keyAny := keyvals[i]
		val := keyvals[i+1]

		// By convention, keys are expected to be strings,
		// but as a fallback we convert them using fmt.Sprint.
		key, ok := keyAny.(string)
		if !ok {
			key = fmt.Sprint(keyAny)
		}

		result = append(result, zap.Any(key, val))
	}

	return result
}
