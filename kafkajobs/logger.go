package kafkajobs

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type logger struct {
	l *slog.Logger
}

func newLogger(l *slog.Logger) *logger {
	return &logger{
		l,
	}
}

func (l *logger) Level() kgo.LogLevel {
	ctx := context.Background()
	switch {
	case l.l.Enabled(ctx, slog.LevelDebug):
		return kgo.LogLevelDebug
	case l.l.Enabled(ctx, slog.LevelInfo):
		return kgo.LogLevelInfo
	case l.l.Enabled(ctx, slog.LevelWarn):
		return kgo.LogLevelWarn
	case l.l.Enabled(ctx, slog.LevelError):
		return kgo.LogLevelError
	default:
		return kgo.LogLevelNone
	}
}

func (l *logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	switch level {
	case kgo.LogLevelDebug, kgo.LogLevelNone:
		l.l.Debug(msg, keyvals...)
	case kgo.LogLevelInfo:
		l.l.Info(msg, keyvals...)
	case kgo.LogLevelWarn:
		l.l.Warn(msg, keyvals...)
	case kgo.LogLevelError:
		l.l.Error(msg, keyvals...)
	default:
		l.l.Debug(msg, keyvals...)
	}
}
