module github.com/roadrunner-server/kafka/v2

go 1.18

require (
	github.com/confluentinc/confluent-kafka-go v1.9.1
	github.com/goccy/go-json v0.9.10
	github.com/roadrunner-server/api/v2 v2.19.0
	github.com/roadrunner-server/errors v1.1.2
	github.com/roadrunner-server/sdk/v2 v2.18.0
	go.uber.org/zap v1.21.0
)

replace (
	github.com/roadrunner-server/api/v2 => ../../api
)

require (
	github.com/pkg/errors v0.9.1 // indirect
	github.com/roadrunner-server/tcplisten v1.1.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
