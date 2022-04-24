package kafka

import (
	"encoding/json"
	"os"

	"github.com/Shopify/sarama"
)

type Producer struct {
	Topic   string
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
func (p *Producer) Enqueue(message Message) error {
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
		Topic: p.Topic,
		Value: sarama.ByteEncoder(val),
	}

	conn.Input() <- msg

	return nil
}
