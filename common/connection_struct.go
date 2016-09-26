package common

import (
  "net"
)

type ConnectionStruct struct {
	Connection net.Conn
	Topic      int64
	Id         int64
	Channel    MessageChannel
}

type ConnectionsMap map[int64]ConnectionStruct
type TopicsMap map[int64]([]int64)
