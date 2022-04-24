package api

import (
	"fmt"
	"time"

	"github.com/crafting-demo/backend-go/pkg/db"
	"github.com/crafting-demo/backend-go/pkg/kafka"
)

// Process processes a message.
func Process(message kafka.Message) {
	meta := message.Meta
	for _, action := range message.Actions {
		switch action.Action {
		case ECHO:
			go Echo(action, meta)
		case READ:
			go Read(action, meta)
		case WRITE:
			go Write(action, meta)
		case CALL:
			go Call(action, meta)
		}
	}
}

// Echo processes "Echo" message actions.
func Echo(action kafka.Action, meta kafka.Meta) {
	msg := kafka.Message{
		Meta: kafka.Meta{
			Caller:   meta.Callee,
			Callee:   React,
			CallTime: kafka.Timestamp(time.Now()),
		},
	}
	msg.Actions = append(msg.Actions, action)

	producer := kafka.Producer{Topic: React}
	producer.Enqueue(msg)
}

// Read processes "Read" message actions.
func Read(action kafka.Action, meta kafka.Meta) {
	msg := kafka.Message{
		Meta: kafka.Meta{
			Caller:   meta.Callee,
			Callee:   React,
			CallTime: kafka.Timestamp(time.Now()),
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
		value, err = mongodbRead(key)
	case db.DynamoDB:
		value, err = dynamodbRead(key)
	case db.Redis:
		value, err = redisRead(key)
	}

	if err != nil {
		action.Payload.Value = fmt.Sprint(err)
	} else {
		action.Payload.Value = value
	}

	msg.Actions = append(msg.Actions, action)

	producer := kafka.Producer{Topic: React}
	producer.Enqueue(msg)
}

// Write processes "Write" message actions.
func Write(action kafka.Action, meta kafka.Meta) {
	msg := kafka.Message{
		Meta: kafka.Meta{
			Caller:   meta.Callee,
			Callee:   React,
			CallTime: kafka.Timestamp(time.Now()),
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
		err = mongodbWrite(key, value)
	case db.DynamoDB:
		err = dynamodbWrite(key, value)
	case db.Redis:
		err = redisWrite(key, value)
	}

	if err != nil {
		action.Payload.Value = fmt.Sprint(err)
	}

	msg.Actions = append(msg.Actions, action)

	producer := kafka.Producer{Topic: React}
	producer.Enqueue(msg)
}

// Call processes "Call" message actions.
func Call(action kafka.Action, meta kafka.Meta) {
	msg := kafka.Message{
		Meta: kafka.Meta{
			Caller:   meta.Callee,
			Callee:   action.Payload.ServiceName,
			CallTime: kafka.Timestamp(time.Now()),
		},
		Actions: action.Payload.Actions,
	}

	producer := kafka.Producer{Topic: action.Payload.ServiceName}
	producer.Enqueue(msg)
}
