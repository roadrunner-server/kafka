package kafkajobs

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
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
	default:
		return kgo.LogLevelDebug
	}
}

func (l *logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	zf := make([]zap.Field, 0, len(keyvals))

	for i := 0; i < len(keyvals); i++ {
		zf = append(zf, zap.Any("kgo_driver", keyvals[i]))
	}

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
