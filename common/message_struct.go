package common

type MessageStruct struct {
	AckChannel chan bool
	Payload    []byte
	Topic      int64
	Position   int64
}

type MessageChannel chan MessageStruct

type MessageChannels []MessageChannel
