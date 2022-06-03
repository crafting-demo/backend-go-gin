package logger

import (
	"log"
)

func LogContext(request []byte, response []byte, errors []error, receivedAt string, requestMode string) {
	log.SetFlags(0)
	if requestMode == "API" {
		log.Println("Started API (HTTP) request for POST \"/api\" at " + receivedAt)
	}
	if requestMode == "KAFKA" {
		log.Println("Started KAFKA (TCP) request at " + receivedAt)
	}
	log.Println("  Request: " + string(request))
	log.Println("  Response: " + string(response))
	if errors != nil {
		var errMsg string
		for _, err := range errors {
			errMsg = errMsg + " " + err.Error()
		}
		log.Println("  Errors: " + errMsg)
	}
	log.Print("\n\n")
}

func Writef(source string, desc string, err error) {
	log.Println(source+": "+desc+":", err)
}

func Write(message string) {
	log.Println(message)
}
