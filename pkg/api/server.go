package api

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/crafting-demo/backend-go/pkg/kafka"
)

func Run() {
	consumer := kafka.Consumer{Topic: Go}

	conn, err := consumer.New()
	if err != nil {
		log.Println("Run: failed to create new consumer", err)
		return
	}
	defer conn.Close()

	partitionConsumer, err := conn.ConsumePartition(consumer.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Println("Run: failed to create partition consumer", err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var message kafka.Message
			if err := json.Unmarshal(msg.Value, &message); err != nil {
				log.Println("Run: failed to parse json encoded message", err)
				continue
			}
			go Process(message)
		case <-signals:
			return
		}
	}
}
