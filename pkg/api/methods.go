package api

import (
	"fmt"
	"time"

	"github.com/crafting-demo/backend-go-gin/pkg/db"
	"github.com/crafting-demo/backend-go-gin/pkg/queue"
)

// HandleMessage processes a message action.
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

// Echo processes Echo action on message.
func Echo(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}
	msg.Actions = append(msg.Actions, action)

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

// Read processes Read action on message.
func Read(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}

	var value string
	var err error
	switch action.Payload.ServiceName {
	case db.MySQL:
		value, err = mysqlRead()
	case db.Postgres:
		value, err = postgresRead()
	case db.MongoDB:
		value, err = mongoRead()
	case db.DynamoDB:
		value, err = dynamoRead()
	case db.Redis:
		value, err = redisRead()
	}

	if err != nil {
		action.Payload.Value = fmt.Sprint(err)
	} else {
		action.Payload.Value = value
	}

	msg.Actions = append(msg.Actions, action)

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

// Write processes Write action on message.
func Write(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}

	var err error
	switch action.Payload.ServiceName {
	case db.MySQL:
		err = mysqlWrite()
	case db.Postgres:
		err = postgresWrite()
	case db.MongoDB:
		err = mongoWrite()
	case db.DynamoDB:
		err = dynamoWrite()
	case db.Redis:
		err = redisWrite()
	}

	if err != nil {
		action.Payload.Value = fmt.Sprint(err)
	}

	msg.Actions = append(msg.Actions, action)

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

// Call processes Call action on message.
func Call(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   action.Payload.ServiceName,
			CallTime: queue.Timestamp(time.Now()),
		},
		Actions: action.Payload.Actions,
	}

	var broker queue.Producer
	broker.Enqueue(action.Payload.ServiceName, msg)
}

func mysqlRead() (string, error) {
	return "", nil
}

func mysqlWrite() error {
	return nil
}

func postgresRead() (string, error) {
	return "", nil
}

func postgresWrite() error {
	return nil
}

func redisRead() (string, error) {
	return "", nil
}

func redisWrite() error {
	return nil
}

func mongoRead() (string, error) {
	return "", nil
}

func mongoWrite() error {
	return nil
}

func dynamoRead() (string, error) {
	return "", nil
}

func dynamoWrite() error {
	return nil
}
