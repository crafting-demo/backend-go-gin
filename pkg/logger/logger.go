package logger

import (
	"log"
)

func WriteContext(request []byte, response []byte, errors []error, receivedAt string) {
	log.SetFlags(0)
	log.Println("Started POST \"/api\" at " + receivedAt)
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

func Write(source string, desc string, err error) {
	log.Println(source+": "+desc+":", err)
}
