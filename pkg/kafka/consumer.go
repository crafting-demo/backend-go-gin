package kafka

import (
	"os"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	Topic   string
	Brokers []string
}

// New returns a new kafka connection as a Consumer.
func (c *Consumer) New() (sarama.Consumer, error) {
	if len(c.Brokers) == 0 {
		host, port := os.Getenv("KAFKA_SERVICE_HOST"), os.Getenv("KAFKA_SERVICE_PORT")
		c.Brokers = append(c.Brokers, host+":"+port)
	}

	config := sarama.NewConfig()

	conn, err := sarama.NewConsumer(c.Brokers, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Messages returns the read channel for the messages that are returned by the broker.
func (c *Consumer) Messages() (<-chan *sarama.ConsumerMessage, error) {
	conn, err := c.New()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	consumer, err := conn.ConsumePartition(c.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}

	return consumer.Messages(), nil
}
