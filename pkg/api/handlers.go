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
// Accepts POST requests with a JSON body specifying
// the nested call, and returns the response JSON.
func NestedCallHandler(c *gin.Context) {
	receivedAt := currentTime()
	var errors []error

	var message Message
	if err := c.ShouldBind(&message); err != nil {
		logger.WriteContext(nil, nil, append(errors, err), receivedAt)
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
	logger.WriteContext(request, response, errors, receivedAt)

	if message.Meta.Caller == React {
		enqueueMessage(React, message)
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
	suffix := os.Getenv("SANDBOX_ENDPOINT_DNS_SUFFIX") + "/api"
	switch serviceName {
	case Gin:
		return "https://gin" + suffix
	case Express:
		return "https://express" + suffix
	case Rails:
		return "https://rails" + suffix
	case Spring:
		return "https://spring" + suffix
	case Django:
		return "https://django" + suffix
	}
	return "unknown"
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
