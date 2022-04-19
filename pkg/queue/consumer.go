package queue

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

type Consumer struct {
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

// Run listens to a queue topic for processing.
func (c *Consumer) Run(topic string, msgCh chan<- []byte) error {
	conn, err := c.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	consumer, err := conn.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}

	doneCh := make(chan struct{}, 1)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			select {
			case msg := <-consumer.Messages():
				msgCh <- msg.Value
			case <-signalCh:
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	return nil
}
