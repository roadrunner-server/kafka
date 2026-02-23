// Package kafkajobs implements the core Apache Kafka jobs driver for RoadRunner.
//
// The [Driver] type satisfies the jobs.Driver interface, providing Run, Push, Pause,
// Resume, Stop, and State operations for producing and consuming Kafka messages as
// RoadRunner jobs. It is built on top of the franz-go (kgo) Kafka client library.
//
// Two factory constructors are provided:
//   - [FromConfig] — builds a Driver from the .rr.yaml configuration file.
//   - [FromPipeline] — builds a Driver from an on-the-fly pipeline declaration.
//
// The [Item] type represents a single Kafka job message and implements the jobs.Job
// interface with Ack, Nack, NackWithOptions, Requeue, and Respond methods.
//
// Configuration is expressed through a rich set of option types:
//   - [ProducerOpts] — idempotency, acks, max message bytes, timeouts, compression
//     codecs (gzip, snappy, lz4, zstd), and partitioning strategies (manual, uniform,
//     round-robin, least-backup, sticky).
//   - [ConsumerOpts] — topics, topic regexp, fetch sizes, partition assignments, and
//     offset control (at, after-milli, at-end, at-start, relative, with-epoch).
//   - [GroupOptions] — consumer group ID, instance ID, and rebalance blocking.
//   - [SASL] — authentication via plain, SCRAM-SHA-256, SCRAM-SHA-512, or AWS MSK IAM.
//   - [TLS] — mutual TLS with configurable client auth types.
//
// OpenTelemetry tracing is integrated throughout the driver using Jaeger propagation,
// emitting spans for push, run, pause, resume, stop, and state operations.
package kafkajobs
