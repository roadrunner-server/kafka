// Package kafka provides a RoadRunner jobs plugin that registers the Apache Kafka driver.
//
// The [Plugin] type implements the RoadRunner endure plugin lifecycle (Init, Name, Weight, Collects)
// and exposes two factory methods — DriverFromConfig and DriverFromPipeline — that delegate
// to the [kafkajobs] package for the actual driver construction. Configuration is read from
// the .rr.yaml file under the "kafka" key.
//
// The plugin declares three dependency-injection interfaces:
//   - [Logger] — provides a named *zap.Logger instance.
//   - [Configurer] — unmarshals configuration sections and checks their existence.
//   - [Tracer] — supplies an OpenTelemetry TracerProvider for distributed tracing.
package kafka
