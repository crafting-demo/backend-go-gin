package api

import (
	"encoding/json"

	"github.com/crafting-demo/backend-go-gin/pkg/queue"
	"github.com/gin-gonic/gin"
)

// EndpointHandler handles messages from API endpoint.
func EndpointHandler(c *gin.Context) {
	var msg queue.Message
	c.BindJSON(&msg)
	ProcessMessage(msg)
}

// QueueHandler handles messages from broker queues.
func QueueHandler() {
	var consumer queue.Consumer
	msgCh := make(chan []byte)
	go consumer.Run(queue.GoGin, msgCh)
	for {
		m := <-msgCh
		if len(m) > 0 {
			var msg queue.Message
			json.Unmarshal(m, &msg)
			ProcessMessage(msg)
		}
	}
}
