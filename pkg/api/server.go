package api

import (
	"encoding/json"
	"os"
	"os/signal"

	"github.com/crafting-demo/backend-go/pkg/kafka"
)

func Run() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	consumer := kafka.Consumer{Topic: Go}

	messages, err := consumer.Messages()
	if err != nil {
		return
	}

	for {
		select {
		case <-signalCh:
			return
		case msg := <-messages:
			var message kafka.Message
			if err := json.Unmarshal(msg.Value, &message); err == nil {
				Process(message)
			}
		}
	}
}
