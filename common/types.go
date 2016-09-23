package common

type MessageStruct struct {
	AckChannel chan bool
	Payload    []byte
	Topic      uint64
}

type MessageChannel chan MessageStruct

type MessageChannels []MessageChannel

type Shaft map[uint64]MessageChannel
