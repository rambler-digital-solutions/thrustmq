package common

type OplogRecord struct {
	Message   string
	Subsystem string
	Action    string
}

func (record OplogRecord) Send() {
	OplogChannel <- record
}
