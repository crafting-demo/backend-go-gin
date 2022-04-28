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
// Accepts POST requests with a JSON body specifying the nested call.
// It processes the nested call, and returns the result JSON message.
func NestedCallHandler(c *gin.Context) {
	var message Message
	if err := c.ShouldBind(&message); err != nil {
		logger.Write("NestedCallHandler", "failed to bind json", err)
		InternalServerError(c)
		return
	}

	for _, action := range message.Actions {
		switch action.Action {
		case Echo:
			action.Status = Passed
		case Read:
			value, err := ReadEntity(action.Payload.ServiceName, action.Payload.Key)
			if err != nil {
				logger.Write("NestedCallHandler", "failed to read key", err)
				action.Status = Failed
				break
			}
			action.Status = Passed
			action.Payload.Value = value
		case Write:
			if err := WriteEntity(action.Payload.ServiceName, action.Payload.Key, action.Payload.Value); err != nil {
				logger.Write("NestedCallHandler", "failed to write key/value pair", err)
				action.Status = Failed
				break
			}
			action.Status = Passed
		case Call:
			respBody, err := serviceCall(action.Payload)
			if err != nil {
				logger.Write("NestedCallHandler", "failed to call "+action.Payload.ServiceName, err)
				action.Status = Failed
				break
			}
			var msg Message
			if err := json.Unmarshal(respBody, &msg); err != nil {
				logger.Write("NestedCallHandler", "failed to unmarshal response message from call", err)
				action.Status = Failed
				break
			}
			action.Status = Passed
			action.Payload.Actions = msg.Actions
		case Enqueue:
			msg := Message{
				Meta: Meta{
					Caller:   Gin,
					Callee:   action.Payload.ServiceName,
					CallTime: currentTime(),
				},
				Actions: action.Payload.Actions,
			}
			if err := enqueueMessage(msg); err != nil {
				logger.Write("NestedCallHandler", "failed to call enqueue message", err)
				action.Status = Failed
				break
			}
			action.Status = Passed
		}

		action.ServiceName = Gin
		action.ReturnTime = currentTime()
	}

	c.JSON(http.StatusOK, message)
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
	suffix := os.Getenv("SANDBOX_ENDPOINT_DNS_SUFFIX")
	switch serviceName {
	case React:
		return "https://react" + suffix
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

func enqueueMessage(message Message) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	producer := kafka.Producer{Topic: message.Meta.Callee}
	if err := producer.Enqueue(msg); err != nil {
		return err
	}

	return nil
}

func currentTime() string {
	return time.Now().UTC().String()
}
