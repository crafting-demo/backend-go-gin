package api

type RequestMessage struct {
	CallTime string `json:"callTime"`
	Target   string `json:"target"`
	Message  string `json:"message"`
	Key      string `json:"key,omitempty"`
	Value    string `json:"value,omitempty"`
}

type ResponseMessage struct {
	ReceivedTime string `json:"receivedTime"`
	ReturnTime   string `json:"returnTime"`
	Message      string `json:"message"`
}
