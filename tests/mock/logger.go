package mocklogger

import (
	"encoding/json"
	"github.com/roadrunner-server/endure/v2/dep"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strings"
)

type ZapLoggerMock struct {
	l *zap.Logger
}

type Logger interface {
	NamedLogger(string) *zap.Logger
}

func ZapTestLogger(enab zapcore.LevelEnabler) (*ZapLoggerMock, *ObservedLogs) {
	logs := &ObservedLogs{}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), // or zapcore.NewConsoleEncoder for plain text
		zapcore.AddSync(os.Stdout),
		enab,
	)

	logger := zap.New(
		core,
		zap.Development(),
		zap.Hooks(func(entry zapcore.Entry) error {
			line := strings.TrimSpace(entry.Message)
			var rawFields map[string]interface{}
			var c []zapcore.Field
			jsonStartIndex := strings.Index(line, "{")
			if jsonStartIndex > 0 {
				jsonStr := line[jsonStartIndex:]
				_ = json.Unmarshal([]byte(jsonStr), &rawFields)
			}

			for field, value := range rawFields {
				c = append(c, zap.Any(field, value))
			}

			logs.add(LoggedEntry{entry, c})
			return nil
		}),
	)

	return &ZapLoggerMock{logger}, logs
}

func (z *ZapLoggerMock) Init() error {
	return nil
}

func (z *ZapLoggerMock) Serve() chan error {
	return make(chan error, 1)
}

func (z *ZapLoggerMock) Stop() error {
	return z.l.Sync()
}

func (z *ZapLoggerMock) Provides() []*dep.Out {
	return []*dep.Out{
		dep.Bind((*Logger)(nil), z.ProvideLogger),
	}
}

func (z *ZapLoggerMock) Weight() uint {
	return 100
}

func (z *ZapLoggerMock) ProvideLogger() *Log {
	return NewLogger(z.l)
}

type Log struct {
	base *zap.Logger
}

func NewLogger(log *zap.Logger) *Log {
	return &Log{
		base: log,
	}
}

func (l *Log) NamedLogger(string) *zap.Logger {
	return l.base
}
