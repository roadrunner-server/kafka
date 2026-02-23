// Package tests contains integration and end-to-end tests for the RoadRunner Kafka
// jobs plugin.
//
// The test suite exercises the full plugin lifecycle — initialization, pipeline
// declaration, job push, pause, resume, and destroy — using the RoadRunner endure
// dependency-injection container with real Kafka brokers. Coverage includes:
//
//   - Consumer group and non-consumer-group modes.
//   - Static configuration and on-the-fly pipeline declaration.
//   - OpenTelemetry span verification.
//   - Ping success and failure scenarios.
//   - TLS connections with client certificates and root CAs.
//   - Durability and redial scenarios using Docker-based Kafka container restarts.
//
// Tests rely on the [mocklogger] and [helpers] sub-packages for observable logging
// and RPC-based pipeline control respectively.
package tests
