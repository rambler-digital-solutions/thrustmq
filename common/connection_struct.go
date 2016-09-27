package common

import (
	"net"
)

type ConnectionStruct struct {
	Connection net.Conn
	Topic      uint64
	Id         uint64
	Channel    MessageChannel
}

type ConnectionsMap map[uint64]ConnectionStruct
type TopicsMap map[uint64]([]uint64)
