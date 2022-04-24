package api

import (
	"encoding/json"

	"github.com/crafting-demo/backend-go/pkg/kafka"
)

func Run() {
	msgCh := make(chan []byte)
	doneCh := make(chan struct{}, 1)

	consumer := kafka.Consumer{Topic: Go}
	go consumer.Run(msgCh, doneCh)

	for {
		select {
		case <-doneCh:
			return
		case m := <-msgCh:
			var msg kafka.Message
			if err := json.Unmarshal(m, &msg); err == nil {
				Process(msg)
			}
		}
	}
}
