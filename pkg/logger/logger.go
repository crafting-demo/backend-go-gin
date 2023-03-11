package logger

import (
	"log"
)

func Writef(source string, desc string, err error) {
	log.Println(source+": "+desc+":", err)
}

func Write(message string) {
	log.Println(message)
}
