package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/crafting-demo/backend-go-gin/pkg/kafka"
	"github.com/crafting-demo/backend-go-gin/pkg/logger"
	"github.com/gin-gonic/gin"
)

// NestedCallHandler handles a "nested call" API.
func NestedCallHandler(c *gin.Context) {
	// logger.Write("Test Test Crafting")

	receivedAt := currentTime()
	var errors []error

	var message Message
	if err := c.ShouldBind(&message); err != nil {
		logger.LogContext(nil, nil, append(errors, err), receivedAt)
		InternalServerError(c)
		return
	}

	request, _ := json.Marshal(message)

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

	c.JSON(http.StatusOK, message)

	response, _ := json.Marshal(message)
	logger.LogContext(request, response, errors, receivedAt)

	if err := enqueueMessage(message.Meta.Caller, message); err != nil {
		logger.Writef("enqueueMessage", "failed to queue response message", err)
	}
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
	host := ""
	port := ""
	switch serviceName {
	case Gin:
		host = os.Getenv("GIN_SERVICE_HOST")
		port = os.Getenv("GIN_SERVICE_PORT_API")
	case Express:
		host = os.Getenv("EXPRESS_SERVICE_HOST")
		port = os.Getenv("EXPRESS_SERVICE_PORT_API")
	case Rails:
		host = os.Getenv("RAILS_SERVICE_HOST")
		port = os.Getenv("RAILS_SERVICE_PORT_API")
	case Spring:
		host = os.Getenv("SPRING_SERVICE_HOST")
		port = os.Getenv("SPRING_SERVICE_PORT_API")
	case Django:
		host = os.Getenv("DJANGO_SERVICE_HOST")
		port = os.Getenv("DJANGO_SERVICE_PORT_API")
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
