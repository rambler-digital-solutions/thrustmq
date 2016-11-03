package common

import (
	"fmt"
)

type OplogRecord struct {
	Message   string
	Subsystem string
	Action    string
}

func (record OplogRecord) Send() {
	OplogChannel <- record
}

func Log(subsystem string, message string, args ...interface{}) {
	finalMessage := fmt.Sprintf(message, args...)
	OplogChannel <- OplogRecord{Message: finalMessage, Subsystem: subsystem}
}
