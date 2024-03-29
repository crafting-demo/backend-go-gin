package kafka

import (
	"github.com/Shopify/sarama"
)

type Consumer struct {
	Brokers []string
}

// New returns a new kafka connection as a Consumer.
func (c *Consumer) New() (sarama.Consumer, error) {
	if len(c.Brokers) == 0 {
		// host, port := os.Getenv("KAFKA_SERVICE_HOST"), os.Getenv("KAFKA_SERVICE_PORT")
		host, port := "127.0.0.1", "9092"
		c.Brokers = append(c.Brokers, host+":"+port)
	}

	config := sarama.NewConfig()

	conn, err := sarama.NewConsumer(c.Brokers, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
