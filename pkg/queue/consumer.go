package queue

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	brokers    []string
	messagesCh chan []byte
	exitCh     chan struct{}
}

// New returns a new kafka connection as a Consumer.
func (c *Consumer) New() (sarama.Consumer, error) {
	config := sarama.NewConfig()

	conn, err := sarama.NewConsumer(c.brokers, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *Consumer) Run(topic string) error {
	conn, err := c.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	consumer, err := conn.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			select {
			case msg := <-consumer.Messages():
				c.messagesCh <- msg.Value
			case <-signalCh:
				c.exitCh <- struct{}{}
			}
		}
	}()

	<-c.exitCh
	return nil
}
