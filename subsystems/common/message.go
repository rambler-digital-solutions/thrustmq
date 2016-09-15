package common

type MessageStruct struct {
	AckChannel chan bool
	Payload    []byte
}
