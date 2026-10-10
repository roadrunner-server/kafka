# Changelog

## [6.1.0](https://github.com/roadrunner-server/kafka/compare/v6.0.0...v6.1.0) (2026-10-10)


### Features

* **kafka:** add consumer_options.pipelining_strategy ([6455267](https://github.com/roadrunner-server/kafka/commit/64552676afe6f327abdaf9a4788025ee536de3b2))
* **kafka:** add pipelining_strategy: Serial for partition ordering ([4a656b0](https://github.com/roadrunner-server/kafka/commit/4a656b09b1c7fd088d3a7d76431ee99def482d56))
* **kafka:** keep one record per partition in flight with pipelining_strategy: Serial ([f13a57d](https://github.com/roadrunner-server/kafka/commit/f13a57d60778fe82f15f12fb688244565d49f4f8))
* **kafka:** open the serial gate on a settling worker reply ([f6accaa](https://github.com/roadrunner-server/kafka/commit/f6accaa7e5b511c19967d19f0bdec476ba3498f7))


### Bug Fixes

* **kafka:** start one listener per pipeline across pause and resume ([07936f9](https://github.com/roadrunner-server/kafka/commit/07936f95c5833818a2c8498c65b27c8f793bd89e))
