package queue

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

type Producer struct {
	brokers []string
}

// New returns a new kafka connection as a Producer.
func (p *Producer) New() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewAsyncProducer(p.brokers, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Enqueue adds a message to a queue.
func (p *Producer) Enqueue(topic string, message Message) error {
	conn, err := p.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	val, err := json.Marshal(message)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(val),
	}
	conn.Input() <- msg
	return nil
}
