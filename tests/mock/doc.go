// Package mocklogger provides an observable slog.Logger for use in integration tests.
//
// [SlogLoggerMock] implements the RoadRunner endure plugin interface (Init, Serve, Stop,
// Provides, Weight) and supplies named *slog.Logger instances. It is constructed via
// [SlogTestLogger], which returns both the mock plugin and an [ObservedLogs] collector.
//
// [ObservedLogs] captures log entries in a concurrency-safe manner and exposes filtering
// methods — FilterLevelExact, FilterMessage, FilterMessageSnippet, FilterAttr, and
// FilterAttrKey — enabling test assertions against specific log output.
package mocklogger
