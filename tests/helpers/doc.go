// Package helpers provides reusable RPC-based test utilities for the Kafka plugin
// integration tests.
//
// Each exported function returns a func(t *testing.T) closure suitable for use with
// t.Run, communicating with a running RoadRunner instance over TCP using the goridge
// RPC codec. The available helpers are:
//
//   - [ResumePipes] — resumes one or more pipelines.
//   - [PushToPipe] — pushes a dummy job to the specified pipeline.
//   - [PushToPipeErr] — pushes a job and asserts that an error is returned.
//   - [PausePipelines] — pauses one or more pipelines.
//   - [DestroyPipelines] — destroys one or more pipelines with retry logic.
//   - [Stats] — retrieves pipeline statistics and populates a jobs.State struct.
package helpers
