package api

import (
	"fmt"
	"time"

	"github.com/crafting-demo/backend-go-gin/pkg/db"
	"github.com/crafting-demo/backend-go-gin/pkg/queue"
)

// ProcessMessage processes message actions.
func ProcessMessage(msg queue.Message) {
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

	key := action.Payload.Key
	var value string
	var err error
	switch action.Payload.ServiceName {
	case db.MySQL:
		value, err = mysqlRead(key)
	case db.Postgres:
		value, err = postgresRead(key)
	case db.MongoDB:
		value, err = mongoRead(key)
	case db.DynamoDB:
		value, err = dynamoRead(key)
	case db.Redis:
		value, err = redisRead(key)
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

	key, value := action.Payload.Key, action.Payload.Value
	var err error
	switch action.Payload.ServiceName {
	case db.MySQL:
		err = mysqlWrite(key, value)
	case db.Postgres:
		err = postgresWrite(key, value)
	case db.MongoDB:
		err = mongoWrite(key, value)
	case db.DynamoDB:
		err = dynamoWrite(key, value)
	case db.Redis:
		err = redisWrite(key, value)
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
