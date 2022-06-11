package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/crafting-demo/backend-go-gin/pkg/kafka"
	"github.com/crafting-demo/backend-go-gin/pkg/logger"
	"github.com/gin-gonic/gin"
)

func HttpHandler(c *gin.Context) {
	// log.Println("Test Test Crafting")

	receivedAt := currentTime()

	var message Message
	if err := c.ShouldBind(&message); err != nil {
		logger.LogContext(nil, nil, []error{err}, receivedAt, "API")
		InternalServerError(c)
		return
	}

	request, _ := json.Marshal(message)
	msg, errors := NestedCallHandler(message)

	c.JSON(http.StatusOK, msg)

	response, _ := json.Marshal(msg)
	logger.LogContext(request, response, errors, receivedAt, "API")
}

func KafkaHandler(message Message) {
	// log.Println("Test Test Crafting")

	receivedAt := currentTime()

	request, _ := json.Marshal(message)
	msg, errors := NestedCallHandler(message)

	if err := enqueueMessage(msg.Meta.Caller, msg); err != nil {
		errors = append(errors, err)
	}

	response, _ := json.Marshal(msg)
	logger.LogContext(request, response, errors, receivedAt, "KAFKA")
}

// NestedCallHandler handles a "nested call" API.
func NestedCallHandler(message Message) (Message, []error) {
	var errors []error

	for i, action := range message.Actions {
		switch action.Action {
		case Echo:
			message.Actions[i].Status = Passed
		case Read:
			value, err := ReadEntity(action.Payload.ServiceName, action.Payload.Key)
			if err != nil {
				errors = append(errors, err)
				message.Actions[i].Status = Failed
				break
			}
			message.Actions[i].Status = Passed
			message.Actions[i].Payload.Value = value
		case Write:
			if err := WriteEntity(action.Payload.ServiceName, action.Payload.Key, action.Payload.Value); err != nil {
				errors = append(errors, err)
				message.Actions[i].Status = Failed
				break
			}
			message.Actions[i].Status = Passed
		case Call:
			respBody, err := serviceCall(action.Payload)
			if err != nil {
				errors = append(errors, err)
				message.Actions[i].Status = Failed
				break
			}
			var msg Message
			if err := json.Unmarshal(respBody, &msg); err != nil {
				errors = append(errors, err)
				message.Actions[i].Status = Failed
				break
			}
			message.Actions[i].Status = Passed
			message.Actions[i].Payload.Actions = msg.Actions
		}

		message.Actions[i].ServiceName = Gin
		message.Actions[i].ReturnTime = currentTime()
	}

	message.Meta.ReturnTime = currentTime()

	return message, errors
}

func serviceCall(payload Payload) ([]byte, error) {
	msg := Message{
		Meta: Meta{
			Caller:   Gin,
			Callee:   payload.ServiceName,
			CallTime: currentTime(),
		},
		Actions: payload.Actions,
	}

	reqBody, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(serviceEndpoint(payload.ServiceName), "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respBody, nil
}

func serviceEndpoint(serviceName string) string {
	var host string
	var port string
	switch serviceName {
	case Gin:
		host = os.Getenv("GIN_SERVICE_HOST")
		port = os.Getenv("GIN_SERVICE_PORT")
	case Express:
		host = os.Getenv("EXPRESS_SERVICE_HOST")
		port = os.Getenv("EXPRESS_SERVICE_PORT")
	case Rails:
		host = os.Getenv("RAILS_SERVICE_HOST")
		port = os.Getenv("RAILS_SERVICE_PORT")
	case Spring:
		host = os.Getenv("SPRING_SERVICE_HOST")
		port = os.Getenv("SPRING_SERVICE_PORT")
	case Django:
		host = os.Getenv("DJANGO_SERVICE_HOST")
		port = os.Getenv("DJANGO_SERVICE_PORT")
	}
	return "http://" + host + ":" + port + "/api"
}

func enqueueMessage(topic string, message Message) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	var producer kafka.Producer
	if err := producer.Enqueue(topic, msg); err != nil {
		return err
	}

	return nil
}

func currentTime() string {
	return time.Now().UTC().String()
}
