package common

import (
	"net"
)

type MessageStruct struct {
	AckChannel chan bool
	Payload    []byte
	Topic      int64
}

type MessageChannel chan MessageStruct

type MessageChannels []MessageChannel

type ConnectionStruct struct {
	Connection net.Conn
	Topic      int64
	Id         int64
	Channel    MessageChannel
}

type ConnectionsMap map[int64]ConnectionStruct
type TopicsMap map[int64]([]int64)

type IndexRecord struct {
	Offset     int64
	Length     int
	Topic      int64
	Connection int64
	Ack        byte
}
