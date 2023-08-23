module github.com/roadrunner-server/kafka/v4

go 1.21

toolchain go1.21.0

require (
	github.com/aws/aws-sdk-go v1.44.329
	github.com/goccy/go-json v0.10.2
	github.com/roadrunner-server/api/v4 v4.6.2
	github.com/roadrunner-server/endure/v2 v2.4.2
	github.com/roadrunner-server/errors v1.3.0
	github.com/roadrunner-server/sdk/v4 v4.3.2
	github.com/twmb/franz-go v1.14.4
	go.opentelemetry.io/contrib/propagators/jaeger v1.17.0
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/trace v1.16.0
	go.uber.org/zap v1.25.0
)

require (
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/roadrunner-server/tcplisten v1.4.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.6.1 // indirect
	go.opentelemetry.io/otel/metric v1.16.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.12.0 // indirect
	golang.org/x/sys v0.11.0 // indirect
)
