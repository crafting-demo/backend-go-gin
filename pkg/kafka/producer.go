package kafka

import (
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

type Producer struct {
	Brokers []string
}

// New returns a new kafka connection as a Producer.
func (p *Producer) New() (sarama.AsyncProducer, error) {
	if len(p.Brokers) == 0 {
		host, port := os.Getenv("KAFKA_SERVICE_HOST"), os.Getenv("KAFKA_SERVICE_PORT")
		p.Brokers = append(p.Brokers, host+":"+port)
	}

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewAsyncProducer(p.Brokers, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Enqueue adds a new message to a queue by topic.
func (p *Producer) Enqueue(topic string, message []byte) error {
	conn, err := p.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(string(message)),
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case conn.Input() <- msg:
			return nil
		case err := <-conn.Errors():
			return err
		case <-signals:
			return nil
		}
	}
}
