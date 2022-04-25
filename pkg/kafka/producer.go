package kafka

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

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
		log.Println("Failed to create new producer", err)
		return err
	}
	defer conn.Close()

	value, err := json.Marshal(message)
	if err != nil {
		log.Println("Failed to encode json message", err)
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.Topic,
		Value: sarama.StringEncoder(string(value)),
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case conn.Input() <- msg:
			return nil
		case err := <-conn.Errors():
			log.Println("Failed to produce message", err)
		case <-signals:
			return nil
		}
	}
}
