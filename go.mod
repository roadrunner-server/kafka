module github.com/roadrunner-server/kafka/v4

go 1.20

require (
	github.com/aws/aws-sdk-go v1.44.254
	github.com/goccy/go-json v0.10.2
	github.com/roadrunner-server/api/v4 v4.3.2
	github.com/roadrunner-server/endure/v2 v2.2.0
	github.com/roadrunner-server/errors v1.2.0
	github.com/roadrunner-server/sdk/v4 v4.2.1
	github.com/twmb/franz-go v1.13.3
	go.opentelemetry.io/contrib/propagators/jaeger v1.16.0
	go.opentelemetry.io/otel v1.15.0
	go.opentelemetry.io/otel/sdk v1.15.0
	go.opentelemetry.io/otel/trace v1.15.0
	go.uber.org/zap v1.24.0
)

require (
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.16.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/roadrunner-server/tcplisten v1.3.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.5.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
)
