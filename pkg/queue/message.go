package queue

import "time"

// Actions
const (
	Echo  = "Echo"
	Read  = "Read"
	Write = "Write"
	Call  = "Call"
)

// Topics
const (
	TsReact      = "frontend-typescript-react"
	GoGin        = "backend-go-gin"
	TsExpress    = "backend-typescript-express"
	RubyRails    = "backend-ruby-rails"
	KotlinSpring = "backend-kotlin-spring"
	PyDjango     = "backend-python-django"
)

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
