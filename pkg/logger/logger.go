package logger

import "log"

func Write(source string, desc string, err error) {
	log.Println(source+": "+desc+":", err)
}
