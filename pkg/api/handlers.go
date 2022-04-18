package api

import (
	"encoding/json"

	"github.com/crafting-demo/backend-go-gin/pkg/queue"
	"github.com/gin-gonic/gin"
)

func DemoHandler(c *gin.Context) {
	var msg queue.Message
	c.BindJSON(&msg)
	HandleMessage(msg)
}

func QueueHandler() {
	var listener queue.Consumer
	msgCh := make(chan []byte)
	go listener.Run(queue.GoGin, msgCh)
	for {
		m := <-msgCh
		if len(m) > 0 {
			var msg queue.Message
			json.Unmarshal(m, &msg)
			HandleMessage(msg)
		}
	}
}
