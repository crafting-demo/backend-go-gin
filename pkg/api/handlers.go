package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/crafting-demo/backend-go-gin/pkg/db"
	"github.com/crafting-demo/backend-go-gin/pkg/logger"
	"github.com/gin-gonic/gin"
)

func HttpHandler(c *gin.Context) {
	// log.Println("Test Test Crafting")

	receivedAt := currentTime()

	logger.Write("\n\nReceived message from API at " + receivedAt)

	var message RequestMessage
	if err := c.ShouldBind(&message); err != nil {
		logger.Writef("API", "Failed to parse message", err)
		InternalServerError(c)
		return
	}

	var response ResponseMessage
	response.ReceivedTime = receivedAt
	if err := ApiHandler(message, &response); err != nil {
		logger.Writef("API", "Failed to handle message", err)
	}

	response.ReturnTime = currentTime()
	logger.Write("Finished processing message from API at " + response.ReturnTime)
	resBytes, _ := json.Marshal(response)
	logger.Write("Sending response " + string(resBytes))
	c.JSON(http.StatusOK, response)
}

func KafkaHandler(message RequestMessage) {
	// log.Println("Test Test Crafting")

	receivedAt := currentTime()
	logger.Write("\n\nReceived message from Kafka at " + receivedAt)

	var response ResponseMessage
	response.ReceivedTime = receivedAt
	if err := ApiHandler(message, &response); err != nil {
		logger.Writef("Kafka", "Failed to handle message", err)
	}
	returnTime := currentTime()
	logger.Write("Finished processing message from Kafka at " + returnTime)
}

// ApiHandler handles a "nested call" API.
func ApiHandler(request RequestMessage, response *ResponseMessage) error {
	logger.Write("Handle message " + request.Message)
	switch request.Message {
	case Hello:
		response.Message = "Hello! This is Go Gin service."
	case Echo:
		response.Message = "Echo from Go Gin service: " + request.Value
	case Write:
		if err := WriteEntity(db.MySQL, request.Key, request.Value); err != nil {
			response.Message = "Go Gin service: failed to write to database"
			return err
		}
		response.Message = "Go Gin service: successfully write to database"
	case Read:
		value, err := ReadEntity(db.MySQL, request.Key)
		if err != nil {
			response.Message = "Go Gin service: failed to read from database"
			return err
		}
		response.Message = "Go Gin service: successfully read from database, value: " + value
	}
	logger.Write("Handle message done, response: " + response.Message)

	return nil
}

func currentTime() string {
	return time.Now().UTC().String()
}
