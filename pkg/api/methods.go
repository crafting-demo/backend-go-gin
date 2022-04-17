package api

import (
	"time"

	"github.com/crafting-demo/backend-go-gin/pkg/queue"
)

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

func Read(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}
	msg.Actions = append(msg.Actions, action)

	// Perform read based on db here

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

func Write(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}
	msg.Actions = append(msg.Actions, action)

	// Perform write depending on db here

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

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
