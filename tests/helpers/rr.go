package helpers

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	mocklogger "tests/mock"

	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/logger/v6"
	"github.com/stretchr/testify/require"
)

const (
	// defaultConfigVersion is the config schema version used by the test configs.
	defaultConfigVersion = "v2024.2.0"
	// probeTimeout caps how long Start waits for the rpc listener to answer.
	probeTimeout = time.Second * 30
	probeTick    = time.Millisecond * 20
	probeDial    = time.Second
	// logTimeout bounds WaitLog. Jobs move through the pipeline asynchronously,
	// so the record a test is after can lag the rpc call that caused it.
	logTimeout = time.Second * 60
	logTick    = time.Millisecond * 50
	// negativeWindow is how long NeverLog watches for a record that must not
	// appear. Delivery from kafka can lag a poll cycle.
	negativeWindow = time.Second * 5
	// shutdownTimeout is the endure graceful shutdown budget.
	shutdownTimeout = time.Second * 60
)

// bootCfg holds the options applied to a container before it is started.
type bootCfg struct {
	version  string
	logLevel slog.Level
	logger   loggerKind
	probe    func(ctx context.Context) bool
	extra    []any
}

// loggerKind selects which logger plugin Start registers.
type loggerKind int

const (
	realLogger loggerKind = iota
	observedLogger
)

// Option customizes the container built by Start.
type Option func(*bootCfg)

// WithConfigVersion overrides the config schema version.
func WithConfigVersion(v string) Option {
	return func(b *bootCfg) { b.version = v }
}

// WithLogLevel sets the endure container log level (debug by default).
func WithLogLevel(l slog.Level) Option {
	return func(b *bootCfg) { b.logLevel = l }
}

// WithObservedLogger registers an in-memory logger instead of the real logger
// plugin and exposes the captured records as RR.Logs.
func WithObservedLogger() Option {
	return func(b *bootCfg) { b.logger = observedLogger }
}

// WithPlugin registers an extra plugin, used to swap in a test tracer.
func WithPlugin(p any) Option {
	return func(b *bootCfg) { b.extra = append(b.extra, p) }
}

// WithTCPProbe makes Start return only once addr accepts a connection. The rpc
// listener binds after the driver has pinged the broker, so dialing it proves the
// pipeline is ready to take calls.
func WithTCPProbe(addr string) Option {
	return func(b *bootCfg) {
		b.probe = func(ctx context.Context) bool {
			d := net.Dialer{Timeout: probeDial}
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return false
			}

			_ = conn.Close()
			return true
		}
	}
}

// RR is a running container.
type RR struct {
	// Logs holds the captured log records, non-nil only with WithObservedLogger.
	Logs *mocklogger.ObservedLogs
}

// CountLog returns how many captured records match snippet.
func (rr *RR) CountLog(snippet string) int {
	return rr.Logs.FilterMessageSnippet(snippet).Len()
}

// WaitLog blocks until the observed log holds at least want records matching
// snippet. Polling replaces the fixed sleeps the suite used between an rpc call
// and the assertion on its effect.
func (rr *RR) WaitLog(t *testing.T, snippet string, want int) {
	t.Helper()

	rr.WaitLogWithin(t, snippet, want, logTimeout)
}

// WaitLogWithin is WaitLog with an explicit bound, for the one wait that has to
// outlast a redial.
func (rr *RR) WaitLogWithin(t *testing.T, snippet string, want int, timeout time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		return rr.CountLog(snippet) >= want
	}, timeout, logTick, "expected at least %d records matching %q, saw %d",
		want, snippet, rr.CountLog(snippet))
}

// RequireLogCount asserts the exact number of records matching snippet, after
// waiting for them to arrive. An exact count catches a driver that redelivers.
func (rr *RR) RequireLogCount(t *testing.T, snippet string, want int) {
	t.Helper()

	rr.WaitLog(t, snippet, want)
	require.Equal(t, want, rr.CountLog(snippet), "records matching %q", snippet)
}

// NeverLog asserts no record matching snippet shows up within the negative
// window. Used where the point of the test is that nothing happens.
func (rr *RR) NeverLog(t *testing.T, snippet string) {
	t.Helper()

	require.Never(t, func() bool {
		return rr.CountLog(snippet) > 0
	}, negativeWindow, logTick, "unexpected record matching %q", snippet)
}

// Start registers the plugins, boots the container and waits for the probe, if
// any, to answer. Errors arriving on the container channel are reported through
// t.Errorf and stop the container, but they do not abort the test.
//
// The returned stop is idempotent and also registered with t.Cleanup, so tests
// asserting on records written during shutdown can stop the container mid-test.
func Start(t *testing.T, cfgPath string, plugins []any, opts ...Option) (*RR, func()) {
	t.Helper()

	cont, rr, bc := newContainer(t, cfgPath, plugins, opts)
	require.NoError(t, cont.Init())

	ch, err := cont.Serve()
	require.NoError(t, err)

	stopCont := sync.OnceValue(cont.Stop)
	done := make(chan struct{})
	wg := &sync.WaitGroup{}

	wg.Go(func() {
		for {
			select {
			case res := <-ch:
				if res == nil {
					return
				}
				t.Errorf("plugin %s reported an error: %v", res.VertexID, res.Error)
				if errS := stopCont(); errS != nil {
					t.Errorf("container stop: %v", errS)
				}
			case <-done:
				if errS := stopCont(); errS != nil {
					t.Errorf("container stop: %v", errS)
				}
				return
			}
		}
	})

	// The drain goroutine calls t.Errorf, so it has to be joined while the test
	// is still running.
	stop := sync.OnceFunc(func() {
		close(done)
		wg.Wait()
	})
	t.Cleanup(stop)

	if bc.probe != nil {
		require.Eventually(t, func() bool { return bc.probe(t.Context()) }, probeTimeout, probeTick, "rpc listener did not become ready")
	}

	return rr, stop
}

// StartExpectServeError registers the plugins, requires Init to pass and Serve
// to fail, and returns the Serve error.
func StartExpectServeError(t *testing.T, cfgPath string, plugins []any, opts ...Option) error {
	t.Helper()

	cont, _, _ := newContainer(t, cfgPath, plugins, opts)
	require.NoError(t, cont.Init())

	_, err := cont.Serve()
	require.Error(t, err)
	// the container is deliberately not stopped: the jobs plugin's Stop panics
	// on a double close when Serve failed before the processor came up, see
	// roadrunner-server/roadrunner#2378

	return err
}

// newContainer builds the container and registers the config, a logger and the
// caller's plugins. The container is not initialized yet.
func newContainer(t *testing.T, cfgPath string, plugins []any, opts []Option) (*endure.Endure, *RR, *bootCfg) {
	t.Helper()

	bc := &bootCfg{version: defaultConfigVersion, logLevel: slog.LevelDebug}
	for _, o := range opts {
		o(bc)
	}

	rr := &RR{}
	all := make([]any, 0, 2+len(plugins)+len(bc.extra))
	all = append(all, &config.Plugin{Version: bc.version, Path: cfgPath})

	switch bc.logger {
	case realLogger:
		all = append(all, &logger.Plugin{})
	case observedLogger:
		l, obs := mocklogger.SlogTestLogger(slog.LevelDebug)
		rr.Logs = obs
		all = append(all, l)
	}

	all = append(all, bc.extra...)

	cont := endure.New(bc.logLevel, endure.GracefulShutdownTimeout(shutdownTimeout))
	require.NoError(t, cont.RegisterAll(append(all, plugins...)...))

	return cont, rr, bc
}
