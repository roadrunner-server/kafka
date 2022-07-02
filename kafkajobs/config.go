package amqpjobs

// pipeline rabbitmq info
const ()

// config is used to parse pipeline configuration
type config struct {
	// global
	Addr string `mapstructure:"addr"`
}

func (c *config) InitDefault() {

	if c.Addr == "" {
		c.Addr = "amqp://guest:guest@127.0.0.1:5672/"
	}

}
