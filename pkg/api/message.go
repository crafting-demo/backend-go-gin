package api

type Message struct {
	Meta    Meta     `json:"meta"`
	Actions []Action `json:"actions"`
}

type Meta struct {
	Caller   string `json:"caller"`
	Callee   string `json:"callee"`
	CallTime string `json:"callTime"`
}

type Action struct {
	ServiceName string  `json:"serviceName,omitempty"`
	ReturnTime  string  `json:"returnTime,omitempty"`
	Status      string  `json:"status,omitempty"`
	Action      string  `json:"action"`
	Payload     Payload `json:"payload"`
}

type Payload struct {
	ServiceName string   `json:"serviceName,omitempty"`
	Actions     []Action `json:"actions,omitempty"`
	Key         string   `json:"key,omitempty"`
	Value       string   `json:"value,omitempty"`
}
