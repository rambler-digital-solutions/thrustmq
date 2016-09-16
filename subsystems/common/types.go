package common

type MessageStruct struct {
	AckChannel chan bool
	Payload    []byte
}

type MessageChannel chan MessageStruct

type MessageChannels []MessageChannel
