package common

type OplogRecord struct {
	Message   string
	Subsystem string
	Action    string
}

func (record OplogRecord) Send() {
	OplogChannel <- record
}

func Log(subsystem string, message string) {
	OplogChannel <- OplogRecord{Message: message, Subsystem: subsystem}
}
