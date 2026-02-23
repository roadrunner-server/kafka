// Package mocklogger provides an observable zap logger for use in integration tests.
//
// [ZapLoggerMock] implements the RoadRunner endure plugin interface (Init, Serve, Stop,
// Provides, Weight) and supplies named *zap.Logger instances. It is constructed via
// [ZapTestLogger], which returns both the mock plugin and an [ObservedLogs] collector.
//
// [ObservedLogs] captures log entries in a concurrency-safe manner and exposes filtering
// methods — FilterLevelExact, FilterMessage, FilterMessageSnippet, FilterField, and
// FilterFieldKey — enabling test assertions against specific log output.
package mocklogger
