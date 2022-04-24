package kafka

import "time"

type Message struct {
	Meta    Meta     `json:"meta"`
	Actions []Action `json:"actions"`
}

type Meta struct {
	Caller   string    `json:"caller"`
	Callee   string    `json:"callee"`
	CallTime Timestamp `json:"callTime"`
}

type Action struct {
	Action  string  `json:"action"`
	Payload Payload `json:"payload"`
}

type Payload struct {
	ServiceName string   `json:"serviceName,omitempty"`
	Actions     []Action `json:"actions,omitempty"`
	Key         string   `json:"key,omitempty"`
	Value       string   `json:"value,omitempty"`
}

type Timestamp time.Time
