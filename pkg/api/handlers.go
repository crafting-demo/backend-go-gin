package api

import (
	"github.com/crafting-demo/backend-go-gin/pkg/queue"
	"github.com/gin-gonic/gin"
)

func Demo(c *gin.Context) {
	var msg queue.Message
	c.BindJSON(&msg)
	HandleMessage(msg)
}

func HandleMessage(msg queue.Message) {
	for _, action := range msg.Actions {
		switch action.Action {
		case queue.Echo:
			go Echo(action, msg.Meta)
		case queue.Read:
			go Read(action, msg.Meta)
		case queue.Write:
			go Write(action, msg.Meta)
		case queue.Call:
			go Call(action, msg.Meta)
		}
	}
}
