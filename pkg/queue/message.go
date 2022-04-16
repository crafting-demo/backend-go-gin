package queue

import "time"

const (
	Echo    = "Echo"
	Read    = "Read"
	Write   = "Write"
	Call    = "Call"
	Enqueue = "Enqueue"
)

type Message struct {
	Meta    Meta     `json:"meta"`
	Actions []string `json:"actions"`
	Payload Payload  `json:"payload"`
}

type Meta struct {
	Caller   string    `json:"caller"`
	Callee   string    `json:"callee"`
	CallTime Timestamp `json:"callTime"`
}

type Payload struct {
	ServiceName string   `json:"serviceName,omitempty"`
	Actions     []string `json:"actions,omitempty"`
	Key         string   `json:"key,omitempty"`
	Value       string   `json:"value,omitempty"`
}

type Timestamp time.Time
