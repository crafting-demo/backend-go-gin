package api

type Message struct {
	Meta    Meta     `json:"meta"`
	Actions []Action `json:"actions"`
}

type Meta struct {
	Caller     string `json:"caller"`
	Callee     string `json:"callee"`
	CallTime   string `json:"callTime"`
	ReturnTime string `json:"returnTime,omitempty"`
}

type Action struct {
	ServiceName string  `json:"serviceName,omitempty"`
	Action      string  `json:"action"`
	Payload     Payload `json:"payload"`
	Status      string  `json:"status,omitempty"`
	ReturnTime  string  `json:"returnTime,omitempty"`
}

type Payload struct {
	ServiceName string   `json:"serviceName,omitempty"`
	Actions     []Action `json:"actions,omitempty"`
	Key         string   `json:"key,omitempty"`
	Value       string   `json:"value,omitempty"`
}
